from __future__ import annotations

from contextlib import contextmanager
from typing import Type

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, Query

from src.sqla.errors import exc
from src.sqla.manager.manager import DatabaseManager
from src.sqla.utils.base import Base


def query_builder(conditions, equality_conditions, model_class, session) -> Query:
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
            print(e)
            session.rollback()
            raise exc.DatabaseException(e)
        finally:
            session.close()

    def execute(self):
        with self._get_managed_session() as session:
            # session.execute()
            session.commit()

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
            values: dict,
            *conditions,
            **equality_conditions
    ) -> None:
        """
        Update records in the specified model_class table based on the given conditions with the provided values.

        Args:
            model_class: The SQLAlchemy model class representing the table.
            values: A dictionary containing the column-value pairs to update.
            *conditions: Variable length arguments representing conditions to filter the records.
            **equality_conditions: Keyword arguments representing equality conditions to filter the records.

        Returns:
            None

        Raises:
            None
        """
        with self._get_managed_session() as session:
            query = query_builder(conditions, equality_conditions, model_class, session)

            query.update(values)
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
            query = query_builder(conditions, equality_conditions, model_class, session)

            query.delete()
            session.commit()

    def upsert_record(
            self,
            model_class: Type[Base],
            values: dict,
            conflict_target: list[str]
    ) -> None:
        # 实现更新插入记录的CRUD操作
        with self._get_managed_session() as session:
            stmt = insert(model_class).values(values)

            # 指定冲突时应该更新哪些字段（通常会是主键或唯一索引列）
            on_conflict = stmt.on_conflict_do_update(
                index_elements=conflict_target,
                set_=dict(**values),
                whereclause=stmt.excluded  # 使用excluded关键字引用冲突行的新值
            )

            session.execute(stmt.on_conflict_do_update)
            session.commit()
