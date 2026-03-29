from pytest_mock import MockerFixture

from granked_data_pipeline.ingest_links import extract_links


def test_extract_links_success(mocker: MockerFixture):
    mock_response = mocker.Mock()
    mock_response.url = "https://www.testdomain.com"
    mock_response.status_code = 200

    mocker.patch(
        "granked_data_pipeline.ingest_links.requests.get", return_value=mock_response
    )
    mock_logger = mocker.Mock()

    response = extract_links("Test query", mock_logger)
    assert response == mock_response

    mock_logger.info.assert_called_with(
        f"Search request succeeded url={mock_response.url} status_code={mock_response.status_code}"
    )


def test_extract_links_failure(mocker: MockerFixture):
    mock_response = mocker.Mock()
    mock_response.url = "https://www.testdomain.com"
    mock_response.status_code = 500

    mocker.patch(
        "granked_data_pipeline.ingest_links.requests.get", return_value=mock_response
    )
    mock_logger = mocker.Mock()

    response = extract_links("Test query", mock_logger)
    assert response == mock_response

    mock_logger.error.assert_called_with(
        f"Search request failed url={mock_response.url} status_code={mock_response.status_code}"
    )
