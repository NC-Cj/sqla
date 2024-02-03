from typing import List, Any


def models_to_dict_list(models: List[Any]) -> List[dict]:
    """
    Convert a list of models object to a list of dictionaries.

    Example:
        ```python
        data = [Model(...), Model(...), ...]
        result = models_to_dict_list(data)
        ```
    """
    return [obj.to_dict() for obj in models]
