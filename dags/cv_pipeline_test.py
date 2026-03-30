import json
from airflow.sdk import DAG, task
from datetime import datetime


def log_task_response(task_name: str, payload: dict) -> None:
    print(f"[{task_name}] response:")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

with DAG(
    dag_id="cv_pipeline",
    description="Upload CV → Extraction texte → Analyse K8s → Sauvegarde BD",
    start_date=datetime(2024, 1, 1),
    schedule=None,       # déclenché par le front via API Airflow
    catchup=False,
    tags=["cv", "pipeline"],
    default_args={
        "retries": 2,    # chaque tâche réessaie 2 fois si elle échoue
    }
) as dag:

    @task
    def upload_cv_s3(user_id: str, file_name: str) -> dict:
        """
        Dans la vraie version : le front upload déjà sur S3 et
        nous envoie juste l'URL. Ici on simule cette réception.
        """
        print(f"CV reçu de l'utilisateur {user_id}")
        print(f"Fichier : {file_name}")

        # Ce que le front nous enverra vraiment :
        s3_info = {
            "user_id": user_id,
            "file_name": file_name,
            "s3_url": f"s3://mon-bucket/cvs/{user_id}/{file_name}",
            "s3_key": f"cvs/{user_id}/{file_name}"
        }
        print(f"CV disponible sur S3 : {s3_info['s3_url']}")
        log_task_response("upload_cv_s3", s3_info)
        return s3_info

    @task
    def extraire_texte_pdf(s3_info: dict) -> dict:
        """
        Dans la vraie version : télécharge le PDF depuis S3
        et extrait le texte (comme ton extractTextFromPDF Angular,
        mais côté serveur avec PyPDF2 ou pdfplumber).
        Ici on simule le texte extrait.
        """
        print(f"Téléchargement depuis S3 : {s3_info['s3_url']}")
        print("Extraction du texte PDF en cours...")

        # Simulation du texte extrait (comme ton pdfjsLib)
        texte_extrait = """
        Jean Dupont - Développeur Full Stack
        5 ans d'expérience en Angular, Node.js, Python
        Compétences : TypeScript, Docker, Kubernetes, AWS
        Expérience : TechCorp (2020-2024), StartupXYZ (2019-2020)
        Formation : Master Informatique - Université Paris
        """

        extraction = {
            "user_id": s3_info["user_id"],
            "s3_url": s3_info["s3_url"],
            "cv_txt": texte_extrait.strip(),
            "nb_pages": 2
        }
        log_task_response("extraire_texte_pdf", extraction)
        return extraction

    @task(retries=3)     # on insiste plus sur K8s car c'est le plus fragile
    def analyser_cv(extraction: dict) -> dict:
        """
        Dans la vraie version : appelle ton service K8s (analyseCv)
        qui retourne le JSON Expertise.
        Ici on simule la réponse du service.
        """
        print(f"Envoi du texte au service K8s pour analyse...")
        print(f"Taille du texte : {len(extraction['cv_txt'])} caractères")

        # Simulation de ce que retourne ton service K8s
        expertise = {
            "user_id": extraction["user_id"],
            "s3_url": extraction["s3_url"],
            "cv_txt": extraction["cv_txt"],
            "expertise": {
                "competences": ["Angular", "Node.js", "Python", "Docker", "Kubernetes"],
                "annees_experience": 5,
                "niveau": "Senior",
                "langages": ["TypeScript", "Python", "JavaScript"],
                "formations": ["Master Informatique"],
                "score": 87
            }
        }

        print(f"Analyse terminée - Score CV : {expertise['expertise']['score']}/100")
        log_task_response("analyser_cv", expertise)
        return expertise

    @task
    def sauvegarder_cv(cv_complet: dict) -> dict:
        """
        Dans la vraie version : appelle ta Lambda AWS (cvService.insert)
        qui sauvegarde dans la BD.
        """
        print(f"Sauvegarde en BD via Lambda AWS...")
        print(f"User : {cv_complet['user_id']}")
        print(f"Compétences : {cv_complet['expertise']['competences']}")
        print(f"Score : {cv_complet['expertise']['score']}/100")
        print(f"CV sauvegardé avec succès !")

        # Dans la vraie version tu feras :
        # import requests
        # requests.post("https://ta-lambda.amazonaws.com/cv", json=cv_complet)

        resultat_sauvegarde = {
            "status": "saved",
            "user_id": cv_complet["user_id"],
            "score": cv_complet["expertise"]["score"],
            "s3_url": cv_complet["s3_url"],
        }
        log_task_response("sauvegarder_cv", resultat_sauvegarde)
        return resultat_sauvegarde

        # Dans la vraie version tu feras :
        # import requests
        # requests.post("https://ta-lambda.amazonaws.com/cv", json=cv_complet)

    # --- Définition du pipeline ---
    # Chaque tâche reçoit le résultat de la précédente via XCom
    s3_info      = upload_cv_s3(
                        user_id="user_42",
                        file_name="cv_jean_dupont.pdf"
                   )
    extraction   = extraire_texte_pdf(s3_info)
    cv_analyse   = analyser_cv(extraction)
    sauvegarder_cv(cv_analyse)
