from dataclasses import dataclass, fields
from typing import Type, TypeVar, List, get_args, get_origin
import pandas as pd
import inspect
from purple_swan.data.models.models import T
from purple_swan.data.loaders.data_loader import DataLoader



def df_to_dataclasses(df: pd.DataFrame, cls: Type[T] | TypeVar) -> List[T]:
    """
    Convert a DataFrame to a list of dataclass instances of type `cls`.
    Column names must match field names. Extra columns are ignored.
    
    If `cls` is a TypeVar, the function will attempt to extract the concrete type
    from the calling class's generic type parameters.
    """
    # If cls is a TypeVar, try to extract the concrete type from the caller
    if isinstance(cls, TypeVar):
        cls = _extract_concrete_type_from_caller(cls)
    
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


def _extract_concrete_type_from_caller(typevar: TypeVar) -> Type:
    """
    Extract the concrete type from the calling class's generic type parameters.
    Inspects the call stack to find the calling class and extracts its type parameter.
    """
    # Get the calling frame (skip this function and df_to_dataclasses)
    frame = inspect.currentframe()
    try:
        # Go up 2 frames: _extract_concrete_type_from_caller -> df_to_dataclasses -> caller
        caller_frame = frame.f_back.f_back
        if caller_frame is None:
            raise TypeError(f"Cannot extract concrete type for TypeVar {typevar}. No caller frame found.")
        
        # Get 'self' from the caller's local variables
        self_obj = caller_frame.f_locals.get('self')
        if self_obj is None:
            raise TypeError(f"Cannot extract concrete type for TypeVar {typevar}. Caller is not a method.")
        
        # Check the class's __args__ directly (for parameterized classes)
        if hasattr(self_obj.__class__, '__args__'):
            args = get_args(self_obj.__class__)
            if args:
                potential_type = args[0]
                # Check if it's a concrete class (not a TypeVar)
                if not isinstance(potential_type, TypeVar) and potential_type is not typevar:
                    return potential_type
        
        # If not found, check __orig_bases__ to find the concrete type from base classes
        if hasattr(self_obj.__class__, '__orig_bases__'):
            for base in self_obj.__class__.__orig_bases__:
                origin = get_origin(base)
                if origin is not None:
                    try:
                        if issubclass(origin, DataLoader):
                            args = get_args(base)
                            if args:
                                potential_type = args[0]
                                # Check if it's a concrete class (not a TypeVar)
                                if not isinstance(potential_type, TypeVar) and potential_type is not typevar:
                                    return potential_type
                    except (TypeError, AttributeError):
                        # origin might not be a class, skip
                        continue
        
        # If still not found, check MRO for parameterized base classes
        for base_cls in self_obj.__class__.__mro__:
            if hasattr(base_cls, '__args__') and base_cls is not self_obj.__class__:
                args = get_args(base_cls)
                if args:
                    potential_type = args[0]
                    if not isinstance(potential_type, TypeVar) and potential_type is not typevar:
                        return potential_type
        
        raise TypeError(f"Could not determine concrete type for TypeVar {typevar} in {self_obj.__class__}. "
                      f"Make sure the class is properly parameterized, e.g., FileSourceDataLoader[Portfolio]")
    finally:
        del frame

