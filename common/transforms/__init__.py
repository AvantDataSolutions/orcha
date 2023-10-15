from dataclasses import dataclass
import pandas as pd
from orcha.core.module_base import TransformBase, module_function

class TrimWhitespaceTransform(TransformBase):
    """
    Trims whitespace from all string columns
    """
    module_idk: str = 'trim_whitespace_transform'
    name: str = 'trim_whitespace_transform'
    description: str = 'Trims whitespace from all string columns'


    @classmethod
    @module_function
    def transform(cls, data: pd.DataFrame) -> pd.DataFrame:
        raise Exception('This is a deliberate error')
        return data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

