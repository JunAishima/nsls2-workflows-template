from prefect import task, flow, get_run_logger
from data_validation import data_validation


@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! dry_run: {dry_run}")


@flow
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    uid = stop_doc["run_start"]
    data_validation(uid, api_key=api_key, dry_run=dry_run)
    log_completion(dry_run=dry_run)
    return True
