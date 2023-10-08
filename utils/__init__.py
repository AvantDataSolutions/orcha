from typing import Any
from pydantic import BaseModel


class pydantic_utils:

    @staticmethod
    def is_basemodel_equal(model1: BaseModel, model2: BaseModel) -> bool:
        if type(model1) != type(model2):
            return False
        for field in model1.__fields__:
            if getattr(model1, field) != getattr(model2, field):
                return False
        return True


    class BaseModelExtended(BaseModel):

        @classmethod
        def from_sql(cls, rows: list):
            return [
                cls(**row)
                for row in rows
            ]


class http_utils():
    # Can't import the typing hints for flask or fastapi
    # because they're never both imported so one
    # throws a 'module not found' error
    @staticmethod
    def get_flask_real_ip(request: Any):
        # X-Forwarded-For can be contaminated by a malcious user
        # attempting to spoof their IP address, however the true
        # IP address will still be included and will be the last IP
        if not request.headers.getlist("X-Forwarded-For"):
            ip = request.remote_addr
        else:
            ip = request.headers.getlist("X-Forwarded-For")[0]
        return ip

    @staticmethod
    def get_fastapi_real_ip(request: Any):
        if not request.headers.getlist("X-Forwarded-For"):
            ip = request.client.host if request.client else None
        else:
            ip = request.headers.getlist("X-Forwarded-For")[0]
        return ip