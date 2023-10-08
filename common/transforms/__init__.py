from dataclasses import dataclass
import pandas as pd
from orcha.core.module_base import TransformBase, module_function

@dataclass
class TrimWhitespaceTransform(TransformBase):
    """
    Trims whitespace from all string columns
    """

    @module_function
    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

