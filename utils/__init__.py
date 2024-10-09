from typing import Any
from pydantic import BaseModel


class pydantic_utils:
    """
    A class containing utility functions for Pydantic models
    """

    @staticmethod
    def is_basemodel_equal(model1: BaseModel, model2: BaseModel) -> bool:
        """
        Check if two Pydantic models are equal.
        ### Parameters:
        - `model1`: The first Pydantic model.
        - `model2`: The second Pydantic model.
        ### Returns:
        A boolean indicating if the models are equal.
        """
        if type(model1) != type(model2):
            return False
        for field in model1.__fields__:
            if getattr(model1, field) != getattr(model2, field):
                return False
        return True


    class BaseModelExtended(BaseModel):
        """
        An extension of the Pydantic BaseModel that adds
        additional functionality.
        """

        @classmethod
        def from_sql(cls, rows: list):
            """
            Create a list of Pydantic models from a list of
            database rows.
            ### Parameters:
            - `rows`: A list of database rows.
            ### Returns:
            A list of Pydantic models.
            """

            return [
                cls(**row)
                for row in rows
            ]


class http_utils():
    """
    A class containing utility functions for handling HTTP requests
    """
    # Can't import the typing hints for flask or fastapi
    # because they're never both imported so one
    # throws a 'module not found' error
    @staticmethod
    def get_flask_real_ip(request: Any):
        """
        Get the real IP address of the client making the request. This
        uses the X-Forwarded-For header to get the IP address if the
        request is forwarded through a proxy.
        ### Parameters:
        - `request`: The Flask request object.
        ### Returns:
        The IP address of the client.
        """

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
        """
        Get the real IP address of the client making the request. This
        uses the X-Forwarded-For header to get the IP address if the
        request is forwarded through a proxy.
        ### Parameters:
        - `request`: The FastAPI request object.
        ### Returns:
        The IP address of the client.
        """
        if not request.headers.getlist("X-Forwarded-For"):
            ip = request.client.host if request.client else None
        else:
            ip = request.headers.getlist("X-Forwarded-For")[0]
        return ip