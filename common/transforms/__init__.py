import pandas as pd
from orcha.core.module_base import TransformBase

trim_whitespace_transform = TransformBase(
    module_idk='trim_whitespace_transform',
    description='Trims whitespace from all string columns'
)

def trim_whitespace_transform_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return data.map(lambda x: x.strip() if isinstance(x, str) else x)

trim_whitespace_transform.transform = trim_whitespace_transform_func


datetime_to_string_transform = TransformBase(
    module_idk='datetime_to_string_transform',
    description='Converts all datetimes to strings in the given format'
)

def datetime_to_string_transform_func(data: pd.DataFrame, format: str, notnull_as_none = True, **kwargs) -> pd.DataFrame:
    for col in data.columns:
        if pd.api.types.is_datetime64_any_dtype(data[col]):
            data[col] = data[col].dt.strftime(format).replace('NaT', None)
        if notnull_as_none:
            data[col] = data[col].where(data[col].notnull(), None)
    return data

datetime_to_string_transform.transform = datetime_to_string_transform_func