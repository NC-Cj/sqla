from __future__ import annotations

from contextlib import contextmanager
from typing import Type, Callable

from sqlalchemy.orm import Session, Query

from sqlax.errors import exc
from sqlax.manager.manager import DatabaseManager
from sqlax.utils.base import Base


def query_builder(session, model_class, *conditions, **equality_conditions) -> Query:
    """
    Build and return a query object based on the provided conditions and equality conditions.

    Args:
        conditions: Variable length arguments representing a set of filter conditions built from
                        SQLAlchemy expressions.
        equality_conditions: Keyword arguments representing equality conditions to filter the query.
        model_class: The SQLAlchemy model class representing the table.
        session: The SQLAlchemy session object.

    Returns:
        A query object representing the constructed query.
    """
    query = session.query(model_class)
    if conditions:
        query = query.filter(*conditions)

    if equality_conditions:
        query = query.filter_by(**equality_conditions)

    return query


class Service:
    def __init__(self, db_manager: DatabaseManager) -> None:
        self._db_manager = db_manager

    @contextmanager
    def _get_managed_session(self) -> Session:
        """返回一个自动管理的Session上下文管理器"""
        session = self._db_manager.get_new_session()
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

        This method creates a session, executes the provided function `do` with the session as the first argument,
        and passes any additional arguments and keyword arguments to the function.
        Finally, it commits the changes made within the session.

        Args:
            do: A function that takes a SQLAlchemy session as the first argument and performs database operations.
            *args: Variable length arguments to be passed to the `do` function.
            **kwargs: Keyword arguments to be passed to the `do` function.

        Returns:
            Any

        Raises:
            None
        """
        with self._get_managed_session() as session:
            return do(session, *args, **kwargs)

    def add(
            self,
            model_class,
            data: dict
    ):
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
            model_class: Type[Base],
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
            model_class: Type[Base],
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
            model_class: Type[Base],
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

        Examples:
            # Upsert multiple records into the "User" table based on the "email" column as the conflict target
            values = [
                {"email": "user1@example.com", "name": "User 1"},
                {"email": "user2@example.com", "name": "User 2"},
                {"email": "user3@example.com", "name": "User 3"},
            ]
            conflict_target = ["email"]
            upsert_records(User, values, conflict_target)
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
