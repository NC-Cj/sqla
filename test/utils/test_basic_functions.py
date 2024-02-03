import pytest

from src.sqlax.utils.helper import models_to_dict_list


# Mock model class to simulate SQLAlchemy model instances
class MockModel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def to_dict(self):
        return self.__dict__


# Test cases for parametrized tests
happy_path_test_cases = [
    # ID: Description
    (1, [MockModel(id=1, name='Test1'), MockModel(id=2, name='Test2')],
     [{'id': 1, 'name': 'Test1'}, {'id': 2, 'name': 'Test2'}]),
    (2, [MockModel()], [{}]),
    (3, [], []),
]
edge_case_test_cases = [
    # ID: Description
    (4, [None], AttributeError),
    (5, None, TypeError),
]
error_case_test_cases = [
    # ID: Description
    (6, 'not a list', AttributeError),
    (7, [123], AttributeError),
]


@pytest.mark.parametrize("test_id, input_models, expected_output", happy_path_test_cases)
def test_models_to_dict_list_happy_path(test_id, input_models, expected_output):
    # Act
    result = models_to_dict_list(input_models)

    # Assert
    assert result == expected_output, f"Test case {test_id} failed."


@pytest.mark.parametrize("test_id, input_models, expected_exception", edge_case_test_cases)
def test_models_to_dict_list_edge_cases(test_id, input_models, expected_exception):
    # Act & Assert
    with pytest.raises(expected_exception):
        models_to_dict_list(input_models)


@pytest.mark.parametrize("test_id, input_models, expected_exception", error_case_test_cases)
def test_models_to_dict_list_error_cases(test_id, input_models, expected_exception):
    # Act & Assert
    with pytest.raises(expected_exception):
        models_to_dict_list(input_models)
