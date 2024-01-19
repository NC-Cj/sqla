import pytest

from src.sqla.manager.manager import create_engine_from_url

# Constants for tests
VALID_URLS = ["sqlite:///database.db"]
INVALID_URL = "invalid://localhost/db1"


@pytest.mark.parametrize(
    "test_id, db_url",
    [
        ("HP-1", VALID_URLS[0])
    ]
)
def test_create_engine_from_url_happy_path(test_id, db_url):
    # Act
    result = create_engine_from_url(db_url)
    print(result)

    # Assert
    assert result is not None, f"{test_id}: Engine should be created successfully for valid URL"


@pytest.mark.parametrize(
    "test_id, db_url",
    [
        ('EC-1', '')
    ]
)
def test_create_engine_from_url_edge_cases(test_id, db_url):
    # Act
    result = create_engine_from_url(db_url)

    # Assert
    assert result is None, f"{test_id}: Engine should not be created for edge case URL"


@pytest.mark.parametrize(
    "test_id, db_url",
    [
        ("ERR-1", "invalidscheme://user:password@localhost/dbname"),
        ("ERR-1", "postgresql+psycopg2://wronguser:wrongpassword@localhost/dbname")
    ]
)
def test_create_engine_from_url_error_cases(test_id, db_url):
    # Act
    result = create_engine_from_url(db_url)

    # Assert
    assert result is None, f"{test_id}: Engine should not be created for invalid URL"
