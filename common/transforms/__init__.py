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
        return data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)


class ConvertDateTimeToStringTransform(TransformBase):
    """
    Converts all datetimes to strings in the given format
    """
    module_idk: str = 'datetime_to_string_transform'
    name: str = 'datetime_to_string_transform'
    description: str = 'Converts all datetimes to strings in the given format'


    @classmethod
    @module_function
    def transform(cls, data: pd.DataFrame, format: str, notnull_as_none = True) -> pd.DataFrame:
        for col in data.columns:
            if pd.api.types.is_datetime64_any_dtype(data[col]):
                data[col] = data[col].dt.strftime(format).replace('NaT', None)
            if notnull_as_none:
                data[col] = data[col].where(data[col].notnull(), None)
        return data