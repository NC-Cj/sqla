from sqlalchemy.orm import Session


class BaseMixin:

    def to_dict(self) -> dict:
        """将模型对象转换为Python字典，通常包含所有数据字段"""
        result = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        return result

    def update_from_dict(self, data_dict):
        """根据给定的字典更新模型对象的属性值"""
        for key, value in data_dict.items():
            if hasattr(self, key):
                setattr(self, key, value)

    @classmethod
    def find_by_id(cls, session: Session, id):
        """通过ID查找数据库中指定记录对应的模型实例"""
        query = session.query(cls).filter(cls.id == id)
        return query.first()

    @classmethod
    def create(cls, session: Session, **kwargs):
        """创建一个新的模型实例并在数据库中插入该记录"""
        instance = cls(**kwargs)
        session.add(instance)
        session.commit()
        return instance

    @classmethod
    def delete_by(cls, session: Session, **kwargs):
        """删除匹配给定条件的所有记录对应的模型实例"""
        query = session.query(cls).filter_by(**kwargs)
        query.delete(synchronize_session='fetch')

        session.commit()

    def soft_delete(self, session: Session):
        """对当前模型实例执行软删除操作（例如，设置一个is_deleted标志位）"""
        if hasattr(self, 'is_deleted'):
            self.is_deleted = True

        session.commit()
