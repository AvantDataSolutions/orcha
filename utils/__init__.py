import ast
import inspect
from typing import Any, Callable

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


def get_config_keys(fnc: Callable) -> dict:
    """
    Inspects a function and returns a dictionary of all config keys used in the function.
    #### Parameters
    - fnc: The function to inspect
    #### Returns
    - dict: A dictionary of all config keys used in the function,
    with the key as the dictionary key and the default value as the dictionary value
    """
    source = inspect.getsource(fnc)
    tree = ast.parse(source)

    class ConfigKeyVisitor(ast.NodeVisitor):
        def __init__(self):
            self.keys = set()
            self.defaults = {}

        def visit_Subscript(self, node):
            if isinstance(node.value, ast.Name) and node.value.id == 'config':
                if isinstance(node.slice, ast.Constant):  # For Python 3.8+
                    self.keys.add(node.slice.value)
            self.generic_visit(node)

        def visit_Call(self, node):
            if isinstance(node.func, ast.Attribute) and node.func.attr == 'get':
                if isinstance(node.func.value, ast.Name) and node.func.value.id == 'config':
                    if len(node.args) > 0:
                        key = None
                        if isinstance(node.args[0], ast.Constant):  # For Python 3.8+
                            key = node.args[0].value
                        elif isinstance(node.args[0], ast.Str):  # For older versions
                            key = node.args[0].s

                        if key:
                            self.keys.add(key)
                            if len(node.args) > 1:
                                default_value = node.args[1]
                                if isinstance(default_value, ast.Constant):  # For Python 3.8+
                                    self.defaults[key] = default_value.value
            self.generic_visit(node)

    visitor = ConfigKeyVisitor()
    visitor.visit(tree)

    values = {}
    for key in visitor.keys:
        if key in visitor.defaults:
            values[key] = visitor.defaults[key]
        else:
            values[key] = None

    return values