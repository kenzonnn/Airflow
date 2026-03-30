import json
import os
from datetime import datetime, timezone
from io import BytesIO
from typing import Any
from urllib.parse import quote, urlparse

import boto3
import requests
from pypdf import PdfReader

try:
    from airflow.sdk import DAG, get_current_context, task
except ImportError:
    from airflow import DAG
    from airflow.decorators import get_current_context, task


def log_event(task_name: str, payload: dict[str, Any]) -> None:
    print(f"[{task_name}]")
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def preview_text(text: str, limit: int = 200) -> str:
    normalized = " ".join(text.split())
    return normalized[:limit] + ("..." if len(normalized) > limit else "")


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def ensure_dict(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"`{field_name}` must be a JSON object.")
    return value


def parse_s3_url(s3_url: str) -> tuple[str, str]:
    parsed = urlparse(s3_url)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URL: {s3_url}")
    return parsed.netloc, parsed.path.lstrip("/")


def resolve_s3_location(payload: dict[str, Any]) -> dict[str, str] | None:
    raw_s3_url = payload.get("s3_url") or payload.get("cv_s3")
    bucket = payload.get("s3_bucket") or os.getenv("CV_DEFAULT_S3_BUCKET")
    key = payload.get("s3_key") or payload.get("storage_path")

    file_name = payload.get("file_name") or payload.get("fileName")
    path_file = payload.get("path_file") or payload.get("pathFile")
    level = payload.get("level")
    file_id = payload.get("file_id") or payload.get("fileId")

    if raw_s3_url and str(raw_s3_url).startswith("s3://"):
        parsed_bucket, parsed_key = parse_s3_url(raw_s3_url)
        bucket = bucket or parsed_bucket
        key = key or parsed_key

    if not key and level and file_id and file_name:
        key = f"{str(level).strip('/')}/{file_id}/{file_name}"

    if not key and path_file and file_name:
        key = f"{str(path_file).rstrip('/')}/{file_name}".lstrip("/")

    if bucket and key:
        return {
            "s3_bucket": bucket,
            "s3_key": key,
            "s3_url": f"s3://{bucket}/{key}",
        }

    if raw_s3_url:
        return {"s3_url": str(raw_s3_url)}

    return None


def build_s3_client():
    client_kwargs: dict[str, Any] = {}

    region_name = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")
    endpoint_url = os.getenv("AWS_ENDPOINT_URL")

    if region_name:
        client_kwargs["region_name"] = region_name
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    return boto3.client("s3", **client_kwargs)


def extract_pdf_text_from_bytes(file_bytes: bytes) -> tuple[str, int]:
    reader = PdfReader(BytesIO(file_bytes))
    pages: list[str] = []

    for page in reader.pages:
        page_text = (page.extract_text() or "").strip()
        pages.append(page_text or "[empty page]")

    merged_text = "\n".join(pages).strip()
    if not merged_text:
        raise ValueError("No text could be extracted from the PDF.")

    return merged_text, len(reader.pages)


def build_headers(token_env_name: str) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    token = os.getenv(token_env_name)
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def json_or_text(response: requests.Response) -> dict[str, Any]:
    content_type = response.headers.get("Content-Type", "")
    if "application/json" in content_type:
        return response.json()

    text = response.text.strip()
    return {"raw_response": text} if text else {}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_report_s3_url(bucket: str, key: str) -> str:
    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
    encoded_key = quote(key, safe="/")

    if endpoint_url:
        return f"{endpoint_url.rstrip('/')}/{bucket}/{encoded_key}"

    region_name = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")
    if region_name and region_name != "us-east-1":
        return f"https://{bucket}.s3.{region_name}.amazonaws.com/{encoded_key}"

    return f"https://{bucket}.s3.amazonaws.com/{encoded_key}"


def build_report_key(prefix: str, cv_id: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S-%f")
    clean_prefix = prefix.strip("/") if prefix else "public/Carrier01/ReportCv"
    return f"{clean_prefix}/{cv_id}/{timestamp}.json"


with DAG(
    dag_id="cv_processing_backend_trigger",
    description="Backend-triggered CV processing: draft Mongo -> S3 -> analysis -> report S3 -> backend update",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    default_args={"retries": 2},
    tags=["cv", "backend", "orchestration"],
) as dag:

    @task
    def prepare_request() -> dict[str, Any]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        incoming = dict(dag_run.conf or {})

        cv_id = incoming.get("cv_id")
        user_id = incoming.get("user_id")

        if not cv_id:
            raise ValueError("`cv_id` is required in dag_run.conf")
        if not user_id:
            raise ValueError("`user_id` is required in dag_run.conf")

        s3_location = resolve_s3_location(incoming)
        cv_txt = incoming.get("cv_txt")
        has_extractable_s3_pdf = bool(
            s3_location and s3_location.get("s3_bucket") and s3_location.get("s3_key")
        )

        if not cv_txt and not has_extractable_s3_pdf:
            raise ValueError(
                "Provide either `cv_txt` or a valid S3 location via `s3_bucket` + `s3_key` or `s3_url`."
            )

        file_name = incoming.get("file_name")
        if not file_name and s3_location and s3_location.get("s3_key"):
            file_name = s3_location["s3_key"].split("/")[-1]

        normalized = {
            "cv_id": cv_id,
            "user_id": user_id,
            "user": ensure_dict(incoming.get("user"), "user") or {"_id": user_id},
            "title": incoming.get("title") or "",
            "file_name": file_name or incoming.get("fileName"),
            "cv_s3": incoming.get("cv_s3") or incoming.get("s3_url"),
            "candidate_type": incoming.get("candidate_type") or incoming.get("candidateType"),
            "visibility": incoming.get("visibility"),
            "linkedin": incoming.get("linkedin"),
            "star": bool(incoming.get("star", False)),
            "cv_txt": cv_txt,
            "analysis_payload": ensure_dict(incoming.get("analysis_payload"), "analysis_payload"),
            "update_payload": ensure_dict(incoming.get("update_payload"), "update_payload"),
            "report_s3_bucket": incoming.get("report_s3_bucket"),
            "report_s3_prefix": incoming.get("report_s3_prefix"),
            "storage_path": incoming.get("storage_path"),
            "path_file": incoming.get("path_file") or incoming.get("pathFile"),
            "level": incoming.get("level"),
            "file_id": incoming.get("file_id") or incoming.get("fileId"),
        }

        if s3_location:
            normalized.update(s3_location)

        if normalized["user"].get("_id") != user_id:
            normalized["user"]["_id"] = user_id

        log_event(
            "prepare_request",
            {
                "cv_id": normalized["cv_id"],
                "user_id": normalized["user_id"],
                "file_name": normalized.get("file_name"),
                "s3_url": normalized.get("s3_url"),
                "cv_s3": normalized.get("cv_s3"),
                "has_cv_txt": bool(normalized.get("cv_txt")),
            },
        )
        return normalized

    @task
    def extract_text(request_payload: dict[str, Any]) -> dict[str, Any]:
        existing_text = request_payload.get("cv_txt")
        if existing_text:
            result = {
                "cv_txt": existing_text,
                "nb_pages": None,
                "text_source": "dag_run_conf",
                "preview": preview_text(existing_text),
            }
            log_event(
                "extract_text",
                {
                    "cv_id": request_payload["cv_id"],
                    "text_source": result["text_source"],
                    "text_length": len(existing_text),
                    "preview": result["preview"],
                },
            )
            return result

        s3_client = build_s3_client()
        response = s3_client.get_object(
            Bucket=request_payload["s3_bucket"],
            Key=request_payload["s3_key"],
        )
        file_bytes = response["Body"].read()

        cv_txt, nb_pages = extract_pdf_text_from_bytes(file_bytes)
        result = {
            "cv_txt": cv_txt,
            "nb_pages": nb_pages,
            "text_source": "s3_pdf",
            "preview": preview_text(cv_txt),
        }
        log_event(
            "extract_text",
            {
                "cv_id": request_payload["cv_id"],
                "s3_url": request_payload.get("s3_url"),
                "nb_pages": nb_pages,
                "text_length": len(cv_txt),
            },
        )
        return result

    @task(retries=3)
    def analyse_cv(request_payload: dict[str, Any], extracted: dict[str, Any]) -> dict[str, Any]:
        url = require_env("CV_ANALYSIS_API_URL")
        timeout = env_int("CV_ANALYSIS_TIMEOUT_SECONDS", 120)

        body = {
            "cv_id": request_payload["cv_id"],
            "user_id": request_payload["user_id"],
            "file_name": request_payload.get("file_name"),
            "s3_url": request_payload.get("s3_url"),
            "cv_txt": extracted["cv_txt"],
            "candidate_type": request_payload.get("candidate_type"),
            "linkedin": request_payload.get("linkedin"),
        }
        body.update(request_payload.get("analysis_payload") or {})

        response = requests.post(
            url,
            json=body,
            headers=build_headers("CV_ANALYSIS_API_TOKEN"),
            timeout=timeout,
        )
        response.raise_for_status()

        analysis_response = json_or_text(response)
        if not isinstance(analysis_response, dict):
            raise ValueError("Analysis service response must be a JSON object.")

        log_event(
            "analyse_cv",
            {
                "cv_id": request_payload["cv_id"],
                "analysis_url": url,
                "status_code": response.status_code,
                "response_keys": sorted(list(analysis_response.keys())),
            },
        )
        return analysis_response

    @task
    def save_report_to_s3(request_payload: dict[str, Any], analysis_response: dict[str, Any]) -> dict[str, Any]:
        bucket = (
            request_payload.get("report_s3_bucket")
            or os.getenv("s3_bucket")
            or request_payload.get("s3_bucket")
        )
        if not bucket:
            raise ValueError("No report S3 bucket available. Set `report_s3_bucket` or `CV_REPORTS_S3_BUCKET`.")

        prefix = (
            request_payload.get("report_s3_prefix")
            or os.getenv("CV_REPORTS_S3_PREFIX")
            or "public/Carrier01/ReportCv"
        )
        key = build_report_key(prefix, request_payload["cv_id"])

        s3_client = build_s3_client()
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(analysis_response, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

        result = {
            "report_s3_bucket": bucket,
            "report_s3_key": key,
            "report_s3_url": build_report_s3_url(bucket, key),
            "report_generated_at": utc_now_iso(),
        }
        log_event(
            "save_report_to_s3",
            {
                "cv_id": request_payload["cv_id"],
                "report_s3_url": result["report_s3_url"],
            },
        )
        return result

    @task
    def build_backend_update_payload(
        request_payload: dict[str, Any],
        extracted: dict[str, Any],
        analysis_response: dict[str, Any],
        report_info: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_analysis = dict(analysis_response)
        normalized_analysis.setdefault("cv_id", request_payload["cv_id"])
        normalized_analysis.setdefault("s3_url", report_info["report_s3_url"])
        normalized_analysis.setdefault("timestamp", report_info["report_generated_at"])

        payload = {
            "cv_id": request_payload["cv_id"],
            "update": {
                "cv_txt": extracted["cv_txt"],
                "expertise": normalized_analysis,
                "analysis_result": normalized_analysis,
                "report_s3_url": report_info["report_s3_url"],
                "report_generated_at": report_info["report_generated_at"],
                "status": "done",
            },
        }
        payload["update"].update(request_payload.get("update_payload") or {})

        log_event(
            "build_backend_update_payload",
            {
                "cv_id": payload["cv_id"],
                "update_keys": sorted(list(payload["update"].keys())),
            },
        )
        return payload

    @task
    def update_cv_backend(update_payload: dict[str, Any]) -> dict[str, Any]:
        url_template = require_env("CV_PERSISTENCE_API_URL")
        method = os.getenv("CV_UPDATE_API_METHOD", "PATCH").upper()
        timeout = env_int("CV_PERSISTENCE_TIMEOUT_SECONDS", 120)

        if "{cv_id}" in url_template:
            url = url_template.format(cv_id=update_payload["cv_id"])
            body = update_payload["update"]
        else:
            url = url_template
            body = update_payload

        response = requests.request(
            method=method,
            url=url,
            json=body,
            headers=build_headers("CV_PERSISTENCE_API_TOKEN"),
            timeout=timeout,
        )
        response.raise_for_status()

        backend_response = json_or_text(response)
        result = {
            "status_code": response.status_code,
            "update_url": url,
            "response": backend_response,
        }
        log_event(
            "update_cv_backend",
            {
                "status_code": result["status_code"],
                "update_url": result["update_url"],
            },
        )
        return result

    request_payload = prepare_request()
    extracted = extract_text(request_payload)
    analysis_response = analyse_cv(request_payload, extracted)
    report_info = save_report_to_s3(request_payload, analysis_response)
    update_payload = build_backend_update_payload(
        request_payload,
        extracted,
        analysis_response,
        report_info,
    )
    update_cv_backend(update_payload)
