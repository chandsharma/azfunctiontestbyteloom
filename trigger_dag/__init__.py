import logging
import os
import requests
import azure.functions as func

# read these from your Function App Settings
BASE = os.getenv("AIRFLOW_BASE_URL")
USER = os.getenv("AIRFLOW_USER")
PASS = os.getenv("AIRFLOW_PASS")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("trigger_dag called")
    logging.info(f"BASE: {BASE}")
    logging.info(f"USER: {USER}")
    logging.info(f"PASS: {PASS}")
    dag_id = req.params.get("dag_id")
    if not dag_id:
        try:
            dag_id = req.get_json().get("dag_id")
        except:
            pass
    if not dag_id:
        return func.HttpResponse("missing dag_id", status_code=400)

    url = f"{BASE}/dags/{dag_id}/dagRuns"
    try:
        r = requests.post(url, auth=(USER, PASS), json={})
        r.raise_for_status()
        return func.HttpResponse(r.text, status_code=r.status_code)
    except Exception as e:
        logging.error("error triggering DAG", exc_info=True)
        return func.HttpResponse(str(e), status_code=500)