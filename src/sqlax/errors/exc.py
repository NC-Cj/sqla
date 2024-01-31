class DatabaseException(Exception):
    """Database exception base class"""

    def __init__(self, message=None):
        self.message = message or "Database related error"
        super().__init__(self.message)

    def __str__(self):
        return f"error message: {self.message}"


class InitializeDatabaseException(DatabaseException):
    """Initialize database exception"""

    def __init__(self, message: str = None):
        super().__init__(message=message)


# 定义一些具体的数据库异常子类
class RecordNotFoundException(DatabaseException):
    """Record No Exception Found"""

    def __init__(self, message: str = None):
        super().__init__(message=message)


class DuplicateRecordException(DatabaseException):
    """Duplicate record exception"""

    def __init__(self, message: str = None):
        super().__init__(message=message)
