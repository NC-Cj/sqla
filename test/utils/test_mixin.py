import pytest
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from src.sqlax.utils.mixin import ModelMixin

# Setup for the tests: create an in-memory SQLite database and a dummy model
engine = create_engine('sqlite:///memory.db')
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# Dummy model to test ModelMixin
class DummyModel(ModelMixin, Base):
    __tablename__ = 'dummy'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)


# Create the table
Base.metadata.create_all(bind=engine)

# Test data for parametrization
happy_path_data = [
    ('hp_1', 'name', 'John', ['John']),
    ('hp_2', 'name', ['John', 'Jane'], ['John', 'Jane']),
    ('hp_3', 'age', 30, [30]),
]

edge_case_data = [
    ('ec_1', 'name', '', []),
    ('ec_2', 'age', 0, [0]),
]

error_case_data = [
    ('err_1', 'nonexistent', 'John'),
    ('err_2', 'name', None),
]


def test_init():
    session = SessionLocal()
    # Pre-populate the database with test data
    john_data = DummyModel(name='John', age=25)
    jane_data = DummyModel(name='Jane', age=30)
    session.add_all([john_data, jane_data])
    session.commit()


@pytest.mark.parametrize("test_id, key, value, expected", happy_path_data)
def test_quick_find_by_happy_path(test_id, key, value, expected):
    # Arrange
    session = SessionLocal()

    # Act
    query = DummyModel.quick_find_by(session, key, value)
    result = query.all()

    # Assert
    assert len(result) == len(expected)
    for record in result:
        assert getattr(record, key) in expected


@pytest.mark.parametrize("test_id, key, value, expected", edge_case_data)
def test_quick_find_by_edge_cases(test_id, key, value, expected):
    # Arrange
    session = SessionLocal()
    # Pre-populate the database with test data
    session.add_all([DummyModel(name=''), DummyModel(age=0)])
    session.commit()

    # Act
    query = DummyModel.quick_find_by(session, key, value)
    result = query.all()

    # Assert
    assert len(result) == len(expected)
    for record in result:
        assert getattr(record, key) in expected


@pytest.mark.parametrize("test_id, key, value", error_case_data)
def test_quick_find_by_error_cases(test_id, key, value):
    # Arrange
    session = SessionLocal()
    session.add(DummyModel(name='John'))
    session.commit()

    # Act & Assert
    with pytest.raises(AttributeError):
        DummyModel.quick_find_by(session, key, value)


def test_to_dict():
    # Arrange
    session = SessionLocal()
    dummy = DummyModel(name='Tom', age=21)
    session.add(dummy)
    session.commit()

    # Act
    result = dummy.to_dict()

    # Assert
    assert result == {'id': dummy.id, 'name': 'Tom', 'age': 21}
