from datetime import datetime

from sqlalchemy import Column, Integer, String, UniqueConstraint, DateTime, func

from src.sqla.manager.manager import DatabaseManager
from src.sqla.utils import service
from src.sqla.utils.base import Base, BaseMixin

urls = ["sqlite:///database.db"]
db_manager = DatabaseManager(urls)


class User(Base, BaseMixin):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), nullable=False, unique=True)
    email = Column(String(100), nullable=False, unique=True)
    password = Column(String(256), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # 添加唯一约束以确保username和email字段的唯一性
    __table_args__ = (
        UniqueConstraint('username', 'email', name='_user_unique_constraint'),
    )

    def __repr__(self):
        return f"<User(id={self.id}, username={self.username}, email={self.email})>"


def initialize():
    engine = db_manager.get_engine()
    Base.metadata.create_all(engine)


def main():
    initialize()

    db_controller = service.Service(db_manager)
    session = db_manager.get_new_session()

    q = service.query_builder(session=session, model_class=User)
    print("Empty data ==> ", q.all())

    db_controller.add(model_class=User, data={"username": "admin",
                                              "email": "admin@domain.com",
                                              "password": "admin123"})  # Add a row
    many_fake_user = [
        {
            "username": "tom",
            "email": "tom@domain.com",
            "password": "tom123"
        },
        {
            "username": "jack",
            "email": "jack@domain.com",
            "password": "jack123"
        }
    ]
    db_controller.add_all(model_class=User, data=many_fake_user)  # Add many row
    print(User.quick_find_by(session, "username", "admin"))
    print(User.quick_find_by(session, "username", ("admin", "jack")))

    db_controller.delete_records_by_conditions(User, username="jack")
    update_data = {"password": "123456"}
    db_controller.update_record_by_conditions(User, update_data, func.lower(User.username).like('t%'))


if __name__ == '__main__':
    main()
