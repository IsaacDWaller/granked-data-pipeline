import pytest
from pytest_mock import MockerFixture

from granked_data_pipeline.ingestion_utilities import extract_data


@pytest.mark.parametrize(
    "status_code,expected_log_method,expected_outcome",
    [
        (200, "info", "succeeded"),
        (500, "error", "failed"),
    ],
)
def test_extract_data(
    mocker: MockerFixture, status_code, expected_log_method, expected_outcome
):
    mock_response = mocker.Mock()
    mock_response.status_code = status_code

    mocker.patch(
        "granked_data_pipeline.ingestion_utilities.requests.get",
        return_value=mock_response,
    )

    mock_logger = mocker.Mock()
    test_request_type = "test request type"

    response = extract_data(mock_response.url, mock_logger, test_request_type)
    assert response == mock_response

    getattr(mock_logger, expected_log_method).assert_called_with(
        f"{test_request_type.capitalize()} request {expected_outcome} url={mock_response.url} status_code={mock_response.status_code}"
    )
