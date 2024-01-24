from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class BaseMixin:

    @classmethod
    def quick_find_by(cls, session: Session, key, value):
        if isinstance(value, (list, tuple)):
            query = session.query(cls).filter(getattr(cls, key).in_(value)).all()
        else:
            query = session.query(cls).filter_by(**{key: value}).all()

        return query

    def to_dict(self) -> dict:
        """将模型对象转换为Python字典，通常包含所有数据字段"""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
