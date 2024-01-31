import time
from typing import Union, Any, Optional

from sqlalchemy import Engine, create_engine, exc, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session

from sqlax.errors.exc import InitializeDatabaseException

_EXC_MSG = "No valid connection exists"


def create_engine_from_url(url, **kwargs) -> Union[Engine, None]:
    """
    Creates a SQLAlchemy engine from a given URL.

    This function creates a SQLAlchemy engine using the provided URL and optional keyword arguments. It then tests the connection to ensure the engine is valid. If the connection is successful, the engine is returned. If there is an error connecting to the database, an error message is printed and None is returned.

    Args:
        url (str): The URL used to create the SQLAlchemy engine.
        **kwargs: Additional keyword arguments to be passed to the create_engine function.

    Returns:
        Union[Engine, None]: The created SQLAlchemy engine if the connection is successful, otherwise None.

    Raises:
        SQLAlchemyError: If there is an error connecting to the database.

    Example:
        engine = create_engine_from_url("postgresql://user:password@localhost/mydatabase")
    """

    try:
        engine = create_engine(url, **kwargs)
        engine.connect()  # ping connection is normal
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
        """
        Get or create a SQLAlchemy engine.

        This method attempts to get an existing engine from the `_engines` dictionary based on the provided URL.
        If an engine does not exist for the URL, a new engine is created using the `create_engine_from_url` function.
        The created engine is then stored in the `_engines` dictionary for future use.

        Args:
            **kwargs: Additional keyword arguments to be passed to the `create_engine_from_url` function.

        Returns:
            sqlalchemy.engine.Engine or None: The SQLAlchemy engine associated with the provided URL.

        Raises:
            InitializeDatabaseException: If an engine cannot be created after multiple retries.
        """
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

        raise InitializeDatabaseException(_EXC_MSG)

    def init_session_factory(self, **kwargs) -> None:
        """
        Initialize the session factory.

        This method initializes the session factory by creating a SQLAlchemy engine using the `_get_or_create_engine` method.
        If an engine is successfully created, a session factory is created using the `sessionmaker` function and bound to the engine.
        Otherwise, an exception is raised.

        Args:
            **kwargs: Additional keyword arguments to be passed to the `_get_or_create_engine` method.

        Returns:
            None

        Raises:
            InitializeDatabaseException: If an engine cannot be created.

        Example:
            ```python
            db_manager = DatabaseManager(urls)
            db_manager.init_session_factory()
            ```
        """
        if engine := self._get_or_create_engine(**kwargs):
            self._session_factory = sessionmaker(bind=engine)
        else:
            raise InitializeDatabaseException(_EXC_MSG)

    def get_new_session(self, **kwargs) -> Session:
        """
        Get a new SQLAlchemy session.

        This method returns a new SQLAlchemy session by first checking if the session factory has been initialized.
        If the session factory is not yet initialized, it is initialized using the `init_session_factory` method.
        Once the session factory is available, a new session is created and returned.

        Args:
            **kwargs: Additional keyword arguments to be passed to the `init_session_factory` method.

        Returns:
            sqlalchemy.orm.Session: A new SQLAlchemy session.

        Example:
            ```python
            db_manager = DatabaseManager(urls)
            session = db_manager.get_new_session()
            ```
        """
        if not self._session_factory:
            self.init_session_factory(**kwargs)

        # Developers need to manage sessions independently.
        # scoped_session(self._session_factory)()

        return self._session_factory()

    def get_engine(self, **kwargs) -> Union[Engine, None]:
        """
        Get the SQLAlchemy engine.

        This method returns the SQLAlchemy engine associated with the current engine index URL.
        If an engine is already available for the current URL, it is returned.
        Otherwise, the method attempts to get or create a new engine using the `_get_or_create_engine` method.
        Finally, the method returns the engine associated with the current URL, or None if it is not available.

        Args:
            **kwargs: Additional keyword arguments to be passed to the `_get_or_create_engine` method.

        Returns:
            sqlalchemy.engine.Engine or None: The SQLAlchemy engine associated with the current engine index URL.

        Example:
            ```python
            db_manager = DatabaseManager(urls)
            engine = db_manager.get_engine()
            ```
        """
        if engine := self._engines.get(self._current_engine_index_url):
            return engine

        # 如果当前引擎不可用，尝试重新获取或创建一个新的引擎
        self._get_or_create_engine(**kwargs)

        # 返回当前有效的引擎
        return self._engines[self._current_engine_index_url] or None

    def reflect_database(self) -> None:
        """
        Reflect the database schema.

        This method reflects the database schema by retrieving the SQLAlchemy engine using the `get_engine` method.
        If the engine is not available, an exception is raised.
        Once the engine is obtained, a `MetaData` object is created and used to reflect the schema by binding it to the engine.
        The reflected tables are then stored in the `_reflected_tables` attribute of the class.

        Returns:
            None

        Raises:
            InitializeDatabaseException: If the engine is not available.

        Example:
            ```python
            db_manager = DatabaseManager(urls)
            db_manager.reflect_database()
            ```
        """
        engine = self.get_engine()
        if engine is None:
            raise InitializeDatabaseException(_EXC_MSG)

        metadata_obj = MetaData()
        metadata_obj.reflect(bind=engine)

        self._reflected_tables = {}
        for name in metadata_obj.tables.keys():
            self._reflected_tables[name] = metadata_obj.tables[name]

    def get_reflected_table(self, name: str) -> Union[Table, None]:
        """
        Get a reflected table by name.

        This method retrieves a reflected table by name from the `_reflected_tables` attribute of the class.
        If the `_reflected_tables` attribute exists and the table with the provided name is found, it is returned.
        Otherwise, None is returned.

        Args:
            name (str): The name of the reflected table.

        Returns:
            sqlalchemy.Table or None: The reflected table with the provided name, or None if it does not exist.

        Example:
            ```python
            db_manager = DatabaseManager(urls)
            db_manager._reflected_tables()
            user_table = db_manager.get_reflected_table("users")
            ```
        """
        if hasattr(self, "_reflected_tables") and name in self._reflected_tables:
            return self._reflected_tables[name]

        return None

    @property
    def get_tables(self) -> Optional[dict[Any, Any]]:
        """
        Get the reflected tables.

        This property returns the reflected tables stored in the `_reflected_tables` attribute of the class.
        If the `_reflected_tables` attribute exists, it is returned.
        Otherwise, None is returned.

        Returns:
            dict[Any, Any] or None: The reflected tables, or None if they do not exist.

        Example:
            ```python
            db_manager = DatabaseManager(urls)
            db_manager._reflected_tables()
            tables = db_manager.get_tables
            ```
        """
        return self._reflected_tables if hasattr(self, "_reflected_tables") else None
