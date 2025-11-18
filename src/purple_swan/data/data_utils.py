from dataclasses import dataclass, fields
from typing import Type, TypeVar, List
import pandas as pd
from purple_swan.data.models.models import T



def df_to_dataclasses(df: pd.DataFrame, cls: Type[T]) -> List[T]:
    """
    Convert a DataFrame to a list of dataclass instances of type `cls`.
    Column names must match field names. Extra columns are ignored.
    """
    # Get field names from dataclass
    field_names = {f.name for f in fields(cls)}

    # Convert rows to dicts
    records = df.to_dict(orient="records")

    instances: List[T] = []
    for rec in records:
        # Filter row dict to only dataclass fields
        data = {k: v for k, v in rec.items() if k in field_names}
        instances.append(cls(**data))
    return instances

