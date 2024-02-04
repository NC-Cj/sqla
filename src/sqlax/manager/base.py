import abc

from sqlalchemy import Engine
from sqlalchemy.orm import Session


class ManagerInterface(metaclass=abc.ABCMeta):
    """
    Interface for a database manager.

    This abstract base class defines the interface for a database manager.
    Subclasses of this class are expected to implement the `get_new_session` method,
    which should return a new SQLAlchemy session.

    Returns:
        sqlalchemy.orm.Session: A new SQLAlchemy session.

    Raises:
        None
    """

    @abc.abstractmethod
    def get_new_session(self, **kwargs) -> Session:
        pass

    @abc.abstractmethod
    def get_engine(self, **kwargs) -> Engine:
        pass
