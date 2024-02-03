from typing import Union

from sqlalchemy.orm import Session, Query


class ModelMixin:

    @classmethod
    def quick_find_by(cls, session: Session, key: str, value: Union[str, list, tuple]) -> Query:
        """
        Quickly find records based on a key-value pair.

        This class method provides a convenient way to quickly find records in the database table represented by the class.
        It takes a SQLAlchemy session, a key, and a value as input.
        If the value is a list or tuple, the method performs an "in" query using the key and the list of values.
        Otherwise, it performs a "filter_by" query using the key and the value.
        The resulting query object is returned.

        Example:
            ```python
            session = get_session()
            query = Model.quick_find_by(session, "name", "John")
            ```
        """
        if isinstance(value, (list, tuple)):
            query = session.query(cls).filter(getattr(cls, key).in_(value))
        else:
            query = session.query(cls).filter_by(**{key: value})

        return query

    def to_dict(self) -> dict:
        """
        Convert the model object to a dictionary.

        Example:
            ```python
            model = Model()
            data = model.to_dict()
            ```
        """
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
