# Backend Trigger Flow

Ce fichier documente le nouveau DAG `cv_processing_backend_trigger`.

## Objectif

Ce DAG commence **apres** la creation initiale du CV en base par le backend.

Flow vise :

1. Front -> upload PDF vers S3
2. Backend -> creation du CV brouillon en MongoDB
3. Backend -> trigger Airflow avec `dag_run.conf`
4. Airflow -> lecture PDF depuis S3
5. Airflow -> extraction de `cv_txt`
6. Airflow -> appel du service d'analyse
7. Airflow -> sauvegarde du rapport JSON dans S3
8. Airflow -> appel backend pour mettre a jour le CV existant

## `dag_run.conf` attendu

```json
{
  "cv_id": "69c6997f497c1805fae8b48a",
  "user_id": "65324a0273071700089218cf",
  "user": {
    "_id": "65324a0273071700089218cf",
    "userName": "yathreb"
  },
  "title": "",
  "cv_s3": "69c6996cba6996ef4f6786d0",
  "s3_bucket": "facturation-uat-uploads",
  "storage_path": "public/Carrier01/Cv/example.pdf",
  "visibility": "public",
  "candidateType": "ingenieur",
  "star": true,
  "linkedin": "https://linkedin.com/in/example"
}
```

Important :

- `cv_s3` peut rester l'identifiant metier / storage deja stocke en base.
- mais pour que Airflow lise vraiment le PDF, il lui faut aussi une vraie localisation de fichier.
- le plus simple est d'envoyer `storage_path`, qui correspond au `fullPath` du front.
- le DAG accepte aussi `s3_key`, ou la combinaison `pathFile + fileName`, ou `level + fileId + fileName`.

## Contrat d'update backend

Le dernier task appelle `CV_UPDATE_API_URL`.

Si l'URL contient `{cv_id}` :

- exemple : `https://backend.example.com/cv/{cv_id}/workflow-update`
- Airflow enverra seulement l'objet `update`

Sinon :

- Airflow enverra :

```json
{
  "cv_id": "69c6997f497c1805fae8b48a",
  "update": {
    "cv_txt": "...",
    "expertise": { "...": "..." },
    "analysis_result": { "...": "..." },
    "report_s3_url": "https://bucket.s3.region.amazonaws.com/public/Carrier01/ReportCv/...",
    "report_generated_at": "2026-03-29T20:00:00+00:00",
    "status": "done"
  }
}
```

## Test local

1. Mettre les bonnes valeurs dans `.env`
2. Lancer `docker compose up -d --build`
3. Attendre que le DAG apparaisse dans Airflow
4. Declencher `cv_processing_backend_trigger` depuis l'UI
5. Coller le JSON de test dans `dag_run.conf`
6. Lire les logs task par task
