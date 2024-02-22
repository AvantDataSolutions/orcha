from typing import TypedDict

import pandas as pd

from orcha.core.module_base import TransformBase


def trim_whitespace_transform_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return data.map(lambda x: x.strip() if isinstance(x, str) else x)

trim_whitespace_transform = TransformBase[pd.DataFrame](
    module_idk='trim_whitespace_transform',
    description='Trims whitespace from all string columns',
    transform_func=trim_whitespace_transform_func,
    create_inputs=pd.DataFrame
)


td = TypedDict('td', {'data': pd.DataFrame, 'format': str, 'notnull_as_none': bool})
def datetime_to_string_transform_func(inputs: td, **kwargs) -> pd.DataFrame:
    data = inputs['data']
    format = inputs['format']
    notnull_as_none = inputs['notnull_as_none']
    for col in data.columns:
        if pd.api.types.is_datetime64_any_dtype(data[col]):
            data[col] = data[col].dt.strftime(format).replace('NaT', None)
        if notnull_as_none:
            data[col] = data[col].where(data[col].notnull(), None)
    return data

datetime_to_string_transform = TransformBase[td](
    module_idk='datetime_to_string_transform',
    description='Converts all datetimes to strings in the given format',
    transform_func=datetime_to_string_transform_func,
    create_inputs=td
)