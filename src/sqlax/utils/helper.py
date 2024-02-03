from __future__ import annotations

from contextlib import contextmanager
from typing import List
from typing import Type, Callable, Any

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

    def execute(
            self,
            do: Callable[[Session, ...], ...],
            *args,
            **kwargs
    ):
        """
        Execute a function within a managed session.

        This method creates a managed SQLAlchemy session using the `_get_managed_session` method.
        Within the context of the session, the provided function `do` is executed with the session as the first argument,
        along with any additional arguments and keyword arguments.
        The result of the function execution is returned.

        Args:
            do (Callable[[Session, ...], ...]): A function that takes a SQLAlchemy session as the first argument and performs database operations.
            *args: Variable length arguments to be passed to the `do` function.
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

            service = Controller(db_manager)
            result = service.execute(my_function, arg1, arg2)
            ```
        """
        with self._get_managed_session() as session:
            return do(session, *args, **kwargs)

    def add(
            self,
            model_class: Type[Any],
            data: dict
    ):
        """
        Add a new record to the database.

        This method adds a new record to the database by creating an instance of the provided `model_class`
        using the data provided in the `data` dictionary.
        The instance is added to the session, and the changes are committed to the database.
        The added instance is returned.

        Args:
            model_class (Type[Any]): The SQLAlchemy model class representing the table.
            data (dict): A dictionary containing the column-value pairs for the new record.

        Returns:
            Base: The instance of the added record.

        Raises:
            None

        Example:
            ```python
            service = Controller(db_manager)
            data = {"name": "John Doe", "age": 30}
            instance = service.add(User, data)
            ```
        """
        with self._get_managed_session() as session:
            instance = model_class(**data)
            session.add(instance)
            session.commit()

            return instance

    def add_all(
            self,
            model_class,
            data: list[dict]
    ) -> list[dict]:
        record_dicts = []

        with self._get_managed_session() as session:
            instances = [model_class(**row) for row in data]
            session.add_all(instances)
            session.commit()

            record_dicts.extend(instance.to_dict() for instance in instances)

        return record_dicts

    def update_record_by_conditions(
            self,
            model_class: Type[Any],
            data: dict,
            *conditions,
            **equality_conditions
    ) -> None:
        """
        Update records in the specified model_class table based on the given conditions with the provided values.

        Args:
            model_class: The SQLAlchemy model class representing the table.
            data: A dictionary containing the column-value pairs to update.
            *conditions: Variable length arguments representing conditions to filter the records.
            **equality_conditions: Keyword arguments representing equality conditions to filter the records.

        Returns:
            None

        Raises:
            None
        """
        with self._get_managed_session() as session:
            query = query_builder(session, model_class, *conditions, **equality_conditions)

            query.update(data)
            session.commit()

    def delete_records_by_conditions(
            self,
            model_class: Type[Any],
            *conditions,
            **equality_conditions
    ) -> None:
        """
        Delete records from the specified model_class table based on the given conditions.

        Args:
            model_class: The SQLAlchemy model class representing the table.
            *conditions: Variable length arguments representing conditions to filter the records.
            **equality_conditions: Keyword arguments representing equality conditions to filter the records.

        Examples:
            # Delete records from the "User" table where the age is greater than 30
            delete_records_by_conditions(User, User.age > 30)

            # Delete records from the "Product" table where the price is equal to 0
            delete_records_by_conditions(Product, price=0)

        Returns:
            None

        Raises:
            None
        """
        with self._get_managed_session() as session:
            query = query_builder(session, model_class, *conditions, **equality_conditions)

            query.delete()
            session.commit()

    def upsert_records(
            self,
            model_class: Type[Any],
            data: list[dict],
            conflict_target: list[str]
    ) -> None:
        """
        Upsert multiple records into the specified model_class table.

        This method performs an upsert operation, which inserts new records or updates existing records based on the
            provided values and conflict target columns.
        That while this approach works fine in most cases, for databases that support native UPSERT statements, such as
            PostgreSQL and MySQL 8.0+, it may be more efficient and avoid concurrency issues to use database-specific UPSERT features.

        Args:
            model_class: The SQLAlchemy model class representing the table.
            data: A list of dictionaries, where each dictionary represents the column-value pairs for a record.
            conflict_target: A list of column names to use as the conflict target for the upsert operation.

        Returns:
            None

        Raises:
            None

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
