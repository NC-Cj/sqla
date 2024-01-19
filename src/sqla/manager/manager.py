import time
from typing import Union

from sqlalchemy import create_engine, exc, Engine, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session


class DatabaseManager:
    def __init__(self, urls: list, timeout_seconds=10) -> None:
        self.urls = urls
        self.timeout_seconds = timeout_seconds
        self._engines = [None] * len(urls)
        self._current_engine_index = 0
        self._session_factory = None

        assert self.urls, "Urls must not be empty"

    def _create_engine_from_url(self, url) -> Union[Engine, None]:
        try:
            engine = create_engine(url)
            engine.connect()  # 连接测试，确保引擎有效
            return engine
        except exc.SQLAlchemyError as e:
            print(f"Failed to connect to the database Error:{e}")
            return None

    def _get_or_create_engine(self, max_attempts=3) -> None:
        start_time = time.monotonic()
        for _ in range(max_attempts):
            if self._engines[self._current_engine_index] is None:
                url = self.urls[self._current_engine_index]
                engine = self._create_engine_from_url(url)
                self._engines[self._current_engine_index] = engine

            if self._engines[self._current_engine_index]:
                return self._engines[self._current_engine_index]

            self._current_engine_index = (self._current_engine_index + 1) % len(self.urls)

            elapsed_time = time.monotonic() - start_time
            if elapsed_time > self.timeout_seconds:
                break  # 超过总超时时间后，终止循环

            time.sleep(2)

        raise RuntimeError("Could not establish a connection to any database within the specified time limit")

    def init_session_factory(self) -> None:
        if engine := self._get_or_create_engine():
            self._session_factory = sessionmaker(bind=engine)
        else:
            raise RuntimeError("No valid database connection could be established")

    def get_new_session(self) -> Session:
        if not self._session_factory:
            self.init_session_factory()

        # 开发者需要管独立管理session
        # scoped_session(self._session_factory)()

        return self._session_factory()

    def get_engine(self) -> Union[Engine, None]:
        if engine := self._engines[self._current_engine_index]:
            return engine

        # 如果当前引擎不可用，尝试重新获取或创建一个新的引擎
        self._get_or_create_engine()

        # 返回当前有效的引擎
        return self._engines[self._current_engine_index] or None

    def reflect_database(self) -> None:
        engine = self.get_engine()
        if engine is None:
            raise RuntimeError("Cannot reflect database because no valid connection exists.")

        metadata_obj = MetaData()
        metadata_obj.reflect(bind=engine)

        self.reflected_tables = {}
        for name in metadata_obj.tables.keys():
            self.reflected_tables[name] = metadata_obj.tables[name]

    def get_reflected_table(self, name: str) -> Union[Table, None]:
        if hasattr(self, "reflected_tables") and name in self.reflected_tables:
            return self.reflected_tables[name]

        return None
