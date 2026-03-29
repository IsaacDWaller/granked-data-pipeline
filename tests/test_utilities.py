from pytest_mock import MockerFixture

from granked_data_pipeline.utilities import extract_data


def test_extract_data_success(mocker: MockerFixture):
    mock_response = mocker.Mock()
    mock_response.status_code = 200

    mocker.patch(
        "granked_data_pipeline.utilities.requests.get", return_value=mock_response
    )

    mock_logger = mocker.Mock()
    test_request_type = "test request type"

    response = extract_data(mock_response.url, mock_logger, test_request_type)
    assert response == mock_response

    mock_logger.info.assert_called_with(
        f"{test_request_type.capitalize()} request succeeded url={mock_response.url} status_code={mock_response.status_code}"
    )


def test_extract_data_failure(mocker: MockerFixture):
    mock_response = mocker.Mock()
    mock_response.status_code = 500

    mocker.patch(
        "granked_data_pipeline.utilities.requests.get", return_value=mock_response
    )

    mock_logger = mocker.Mock()
    test_request_type = "test request type"

    response = extract_data(mock_response.url, mock_logger, test_request_type)
    assert response == mock_response

    mock_logger.error.assert_called_with(
        f"{test_request_type.capitalize()} request failed url={mock_response.url} status_code={mock_response.status_code}"
    )
