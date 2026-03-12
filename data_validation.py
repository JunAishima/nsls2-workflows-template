from prefect import task, flow, get_run_logger
import time as ttime
from tiled.client import from_uri
from bluesky_tiled_plugin.writing.validator import validate


@task(retries=2, retry_delay_seconds=10)
def get_run(uid, api_key=None):
    logger = get_run_logger()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl["tla"]["raw"][uid]  # ***** replace tla with endstation/beamline tla
    logger.info(f"Validating uid {run.start['uid']}")
    return run


@task(retries=2, retry_delay_seconds=10)
def read_stream(run, stream):
    return run[stream].read()


@flow
def data_validation(uid, api_key=None, dry_run=False):
    logger = get_run_logger()
    run_client = get_run(uid, api_key=api_key)
    start_time = ttime.monotonic()
    try:
        # the following calls to validate() only work for SQL database-backed catalogs - remove if not available
        if dry_run:
            validate(run_client, fix_errors=False, try_reading=True, raise_on_error=True)
        else:
            validate(run_client, fix_errors=True, try_reading=True, raise_on_error=True)
    except AttributeError:
        # check by reading data if not SQL database-backed
        for stream in run_client:
            logger.info(f"{stream}:")
            stream_start_time = ttime.monotonic()
            stream_data = read_stream(run_client, stream)  # noqa: F841
            stream_elapsed_time = ttime.monotonic() - stream_start_time
            logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
            logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")
