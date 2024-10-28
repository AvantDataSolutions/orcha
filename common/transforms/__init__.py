from typing import List, Optional, TypedDict

import pandas as pd
from orcha.core.module_base import TransformBase


def _trim_whitespace_transform_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return data.map(lambda x: x.strip() if isinstance(x, str) else x)

trim_whitespace_transform = TransformBase[pd.DataFrame](
    module_idk='trim_whitespace_transform',
    description='Trims whitespace from all string columns',
    transform_func=_trim_whitespace_transform_func,
    create_inputs=pd.DataFrame
)


_td = TypedDict('_td', {'data': pd.DataFrame, 'format': str, 'notnull_as_none': bool})
def _datetime_to_string_transform_func(inputs: _td, **kwargs) -> pd.DataFrame:
    data = inputs['data']
    format = inputs['format']
    notnull_as_none = inputs['notnull_as_none']
    for col in data.columns:
        if pd.api.types.is_datetime64_any_dtype(data[col]):
            data[col] = data[col].dt.strftime(format).replace('NaT', None)
        if notnull_as_none:
            data[col] = data[col].where(data[col].notnull(), None)
    return data

datetime_to_string_transform = TransformBase[_td](
    module_idk='datetime_to_string_transform',
    description='Converts all datetimes to strings in the given format',
    transform_func=_datetime_to_string_transform_func,
    create_inputs=_td
)
"""
Transform that converts all datetime columns to strings in the given format
and optionally replaces NaT with None, typically useful for writing to SQL databases
### Inputs
- `data` (pd.DataFrame): The DataFrame to transform
- `format` (str): The format to convert the datetime to
- `notnull_as_none` (bool): If True, replaces NaT with None
"""


_diff_inputs = TypedDict('_diff_inputs', {
    'new_df': pd.DataFrame,
    'old_df': pd.DataFrame,
    'add_updated_col': Optional[bool],
    'updated_col_name': Optional[str]
})


def _keep_changed_transform_func(inputs: _diff_inputs, **kwargs) -> pd.DataFrame:
    new_df = inputs['new_df']
    old_df = inputs['old_df']
    add_updated = inputs['add_updated_col']
    updated_name = inputs['updated_col_name']
    if updated_name is None:
        updated_name = 'updated_at'
    if updated_name in new_df.columns:
        raise ValueError(f'Updated col name "{updated_name}" already exists in new_df')

    # Ensure both dataframes have the same columns
    if set(new_df.columns) != set(old_df.columns):
        raise ValueError("Dataframes do not have the same columns")

    # Perform a full outer join on the key columns
    merged_df = new_df.merge(old_df, how='outer', indicator=True, on=new_df.columns.tolist())

    # Find the rows that exist only in one of the dataframes
    diff_df = merged_df[merged_df['_merge'] == 'left_only']

    # Drop the merge indicator column
    diff_df = diff_df.drop(columns=['_merge'])

    if add_updated:
        diff_df[updated_name] = pd.Timestamp.now()

    return diff_df


keep_changed_rows_transform = TransformBase[_diff_inputs](
    module_idk='diff_transform',
    description='Returns only the columns from "new_df" which are not in old_df',
    transform_func=_keep_changed_transform_func,
    create_inputs=_diff_inputs
)
