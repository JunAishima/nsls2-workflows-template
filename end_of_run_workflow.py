import os
from prefect import task, flow, get_run_logger
from data_validation import data_validation
from dotenv import load_dotenv


def get_api_key_from_env(api_key=None):
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! dry_run: {dry_run}")


@flow
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    uid = stop_doc["run_start"]
    if not api_key:
        api_key = get_api_key_from_env(api_key=None)
    data_validation(uid, api_key=api_key, dry_run=dry_run)
    log_completion(dry_run=dry_run)
    return True
