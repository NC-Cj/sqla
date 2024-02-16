from __future__ import annotations

from contextlib import contextmanager
from typing import List, Optional
from typing import Type, Callable, Any

from sqlalchemy import Connection
from sqlalchemy.orm import Session, Query

from src.sqlax.errors import exc
from src.sqlax.manager.base import ManagerInterface


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


def query_builder(
        session: Session,
        model_class: Type[Any],
        *conditions,
        **equality_conditions
) -> Query:
    """
    Build a SQLAlchemy query.

    This function builds a SQLAlchemy query based on the provided session, model class, conditions, and equality conditions.
    It starts with a query object created from the session and the model class.
    If conditions are provided, they are applied as filters to the query using the `filter` method.
    If equality conditions are provided, they are applied as filters using the `filter_by` method.
    The resulting query object is returned.

    Args:
        session (sqlalchemy.orm.Session): The SQLAlchemy session.
        model_class (Type[Any]): The SQLAlchemy model class representing the table.
        *conditions: Variable length arguments representing conditions to filter the records.
        **equality_conditions: Keyword arguments representing equality conditions to filter the records.

    Returns:
        sqlalchemy.orm.Query: The SQLAlchemy query object.

    Example:
        ```python
        session = get_session()
        query = query_builder(session, User, User.age > 30, name="John")
        ```
    """
    query = session.query(model_class)
    if conditions:
        query = query.filter(*conditions)

    if equality_conditions:
        query = query.filter_by(**equality_conditions)

    return query


class Controller:
    def __init__(
            self,
            *,
            obj: ManagerInterface
    ) -> None:
        self._dmi = obj

    @staticmethod
    def validate(
            model_class: Type[Any],
            validate_name: str
    ) -> bool:
        """Validate the specified model"""
        if not hasattr(model_class, validate_name):
            raise NotImplementedError(f"{model_class.__name__}.{validate_name} is not implemented")

        if not (validation_errors := getattr(model_class, validate_name)()):
            return True

        print("Validation errors:")
        for error in validation_errors:
            print("- ", error)

        return False

    @contextmanager
    def _get_managed_session(self) -> Session:
        """
        Get a managed SQLAlchemy session.

        This context manager function returns a managed SQLAlchemy session obtained from the `ManagerInterface`.
        Within the context, the session is yielded to the caller.
        If an exception occurs during the execution of the context, the session is rolled back and a `DatabaseException` is raised.
        Finally, the session is closed.

        Returns:
            sqlalchemy.orm.Session: The managed SQLAlchemy session.

        Raises:
            exc.DatabaseException: If an exception occurs during the execution of the context.

        Example:
            ```python
            with self._get_managed_session() as session:
                # Perform database operations using the session
            ```
        """
        session = self._dmi.get_new_session()
        try:
            yield session
        except Exception as e:
            session.rollback()
            raise exc.DatabaseException(e) from e
        finally:
            session.close()

    def execute_within_connect(
            self,
            do: Callable[[Connection, ...], Any],
            *args,
            **kwargs
    ) -> Any:
        """
        Execute a function within a database connection.

        This method creates a database connection using the `_dmi.get_engine().connect()` method.
        Within the context of the connection, the provided function `do` is executed with the connection as the first argument,
        along with any additional arguments and keyword arguments.
        The result of the function execution is returned.

        Args:
            do (Callable[[Connection, ...], ...]): A function that takes a database connection as the first argument and performs database operations.
            *args: Variable length arguments to be passed to the `do` function.
            **kwargs: Keyword arguments to be passed to the `do` function.

        Returns:
            Any: The result of the function execution.

        Raises:
            None
        """
        with self._dmi.get_engine().connect() as conn:
            return do(conn, *args, **kwargs)

    def execute_within_session(
            self,
            do: Callable[[Session, ...], Any],
            *args,
            **kwargs
    ) -> Any:
        """
        Execute a function within a managed session.

        This method creates a managed SQLAlchemy session using the `_get_managed_session` method.
        Within the context of the session, the provided function `do` is executed with the session as the first argument,
        along with any additional arguments and keyword arguments.
        The result of the function execution is returned.

        Args: do (Callable[[Session, ...], Any]): A function that takes a SQLAlchemy session as the first argument
        and performs database operations. *args: Variable length arguments to be passed to the `do` function.
        **kwargs: Keyword arguments to be passed to the `do` function.

        Returns:
            Any: The result of the function execution.

        Raises:
            None

        Example:
            ```python
            def my_function(session, arg1, arg2):
                # Perform database operations using the session
                return result

            db_controller = Controller(db_manager)
            result = db_controller.execute_within_session(my_function, arg1, arg2)
            ```
        """
        with self._get_managed_session() as session:
            return do(session, *args, **kwargs)

    def insert(
            self,
            model_class: Type[Any],
            data: dict
    ):
        """Insert data into the database"""
        with self._get_managed_session() as session:
            instance = model_class(**data)
            session.add(instance)
            session.commit()

            return instance

    def bulk_insert(
            self,
            model_class,
            data: list[dict]
    ) -> list[dict]:
        """Batch Insert Multiple Data"""
        record_dicts = []

        with self._get_managed_session() as session:
            instances = [model_class(**row) for row in data]
            session.add_all(instances)
            session.commit()

            record_dicts.extend(instance.to_dict() for instance in instances)

        return record_dicts

    def upsert(
            self,
            model_class: Type[Any],
            data: list[dict],
            conflict_target: list[str]
    ) -> None:
        """
        Insert data, update if data already exists

        Example:
            ```python
            # Upsert multiple records into the "User" table based on the "email" column as the conflict target
            values = [
                {"email": "user1@example.com", "name": "User 1"},
                {"email": "user2@example.com", "name": "User 2"},
                {"email": "user3@example.com", "name": "User 3"},
            ]
            conflict_target = ["email"]
            upsert_records(User, values, conflict_target)
            ```
        """
        with self._get_managed_session() as session:
            for row in data:
                c = [getattr(model_class, column) == row[column] for column in conflict_target]
                query = session.query(model_class).filter(*c)

                if existing_record := query.first():
                    for column, value in row.items():
                        setattr(existing_record, column, value)
                    session.add(existing_record)
                else:
                    instance = model_class(**row)
                    session.add(instance)

            session.commit()

    def update(
            self,
            model_class: Type[Any],
            data: dict,
            *conditions,
            **equality_conditions
    ) -> None:
        """Update specified records"""
        with self._get_managed_session() as session:
            query = query_builder(session, model_class, *conditions, **equality_conditions)
            query.update(data)
            session.commit()

    def delete(
            self,
            model_class: Type[Any],
            *conditions,
            **equality_conditions
    ) -> None:
        """Delete records from the database"""
        with self._get_managed_session() as session:
            query = query_builder(session, model_class, *conditions, **equality_conditions)
            query.delete()
            session.commit()

    def find_by_property(
            self,
            model_class: Type[Any],
            property_name: str,
            value: Any,
            all_=False
    ) -> Optional[Any]:
        """Find a record by a specific property value"""
        with self._get_managed_session() as session:
            query = session.query(model_class).filter(getattr(model_class, property_name) == value)
            return query.all() if all_ else query.first()

    def exists(
            self,
            model_class: Type[Any],
            *conditions,
            **equality_conditions
    ) -> bool:
        """Check if specified record exists"""
        with self._get_managed_session() as session:
            query = query_builder(session, model_class, *conditions, **equality_conditions)
            result = session.query(query.exists()).scalar()
            return bool(result)

    def count(
            self,
            model_class: Type[Any],
            *conditions,
            **equality_conditions
    ) -> int:
        """Count the number of records matching the specified conditions"""
        with self._get_managed_session() as session:
            query = query_builder(session, model_class, *conditions, **equality_conditions)
            return query.count()
