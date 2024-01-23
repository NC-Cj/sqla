class DatabaseException(Exception):
    """数据库异常基类"""

    def __init__(self, message = None):
        self.message = message or "Database related error"
        super().__init__(self.message)

    def __str__(self):
        return f"error message: {self.message}"


# 定义一些具体的数据库异常子类
class RecordNotFoundException(DatabaseException):
    """记录未找到异常"""

    def __init__(self, message: str = None):
        super().__init__(message=message)


class DuplicateRecordException(DatabaseException):
    """重复记录异常"""

    def __init__(self, message: str = None):
        super().__init__(message=message)
