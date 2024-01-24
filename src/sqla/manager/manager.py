import time
from typing import Union

from sqlalchemy import Engine, create_engine, exc, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session


def create_engine_from_url(url, **kwargs) -> Union[Engine, None]:
    try:
        engine = create_engine(url, **kwargs)
        engine.connect()  # 连接测试，确保引擎有效
        return engine
    except exc.SQLAlchemyError as e:
        print(f"Failed to connect to the database Error:{e}")
        return None


class DatabaseManager:
    def __init__(self, urls: list) -> None:
        self.urls = urls
        self._engines = {}
        self._current_engine_index_url = ""
        self._session_factory = None

        assert self.urls, "Urls must not be empty"

    def _get_or_create_engine(self, **kwargs) -> None:
        retry_count = 3
        for _ in range(retry_count):
            for url in self.urls:
                self._current_engine_index_url = url

                if self._engines.get(url) is None:
                    engine = create_engine_from_url(url, **kwargs)
                    self._engines[url] = engine

                if self._engines[url]:
                    return self._engines[url]

            time.sleep(1)

        raise RuntimeError("Could not establish a connection to any database within the specified time limit")

    def init_session_factory(self, **kwargs) -> None:
        if engine := self._get_or_create_engine(**kwargs):
            self._session_factory = sessionmaker(bind=engine)
        else:
            raise RuntimeError("No valid database connection could be established")

    def get_new_session(self, **kwargs) -> Session:
        if not self._session_factory:
            self.init_session_factory(**kwargs)

        # 开发者需要管独立管理session
        # scoped_session(self._session_factory)()

        return self._session_factory()

    def get_engine(self, **kwargs) -> Union[Engine, None]:
        if engine := self._engines.get(self._current_engine_index_url):
            return engine

        # 如果当前引擎不可用，尝试重新获取或创建一个新的引擎
        self._get_or_create_engine(**kwargs)

        # 返回当前有效的引擎
        return self._engines[self._current_engine_index_url] or None

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
