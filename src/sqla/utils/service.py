from __future__ import annotations

from contextlib import contextmanager
from typing import Type, Dict, Any

from sqlalchemy import Table
from sqlalchemy.orm import Session

from src.sqla.manager.manager import DatabaseManager


class Service:
    def __init__(self, db_manager: DatabaseManager) -> None:
        self._db_manager = db_manager

    @contextmanager
    def _get_managed_session(self) -> Session:
        """返回一个自动管理的Session上下文管理器"""
        session = self._db_manager.get_new_session()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def execute(self) -> Session:
        return self._get_managed_session()

    def add(self, model_class, data: dict):
        with self.execute() as session:
            instance = model_class(**data)
            session.add(instance)
            session.commit()

            return instance

    def add_all(self, model_class, data: list[dict]) -> list[dict]:
        record_dicts = []

        with self.execute() as session:
            instances = [model_class(**row) for row in data]
            session.add_all(instances)
            session.commit()

            record_dicts.extend(instance.to_dict() for instance in instances)

        return record_dicts

    def update_record(self, model_class: Type[Table], record_id: Any, updated_data: Dict[str, Any]):
        # 实现更新记录的CRUD操作
        # ...
        ...

    def delete_record(self, model_class: Type[Table], record_id: Any):
        # 实现删除记录的CRUD操作
        ...

    def upsert_record(self, model_class: Type[Table], record_id: Any):
        # 实现更新插入记录的CRUD操作
        ...
