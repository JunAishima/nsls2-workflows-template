from prefect.logging import disable_run_logger
from end_of_run_workflow import end_of_run_workflow
import os
import pytest


@pytest.fixture(autouse=True, scope="session")
def prefect_disable_logging():
    with disable_run_logger():
        yield


def test_end_of_run_workflow(prefect_disable_logging):
    print("starting test!")
    assert end_of_run_workflow(
        stop_doc={"run_start": "abcdef-abcdef-abcdef"},
        api_key=os.environ["TILED_API_KEY"],
    )
    print("finished test!")
