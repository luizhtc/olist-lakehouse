"""Functions and methods for accessing Kaggle data sources.

Execution of these methods require KAGGLE_API_TOKEN defined environment variable."""

from typing import Any

import kagglehub
import pandas as pd
from kagglehub import KaggleDatasetAdapter


def load_into_pandas_dataframe(
    handle: str,
    path: str,
    pandas_kwargs: dict[str, Any] = None
) -> pd.DataFrame:
    """Loads a Kaggle dataset into a Pandas DataFrame.

    Args:
        handle (str): The dataset handle.
        path (str): The dataset path within given handle.
        pandas_kwargs (dict): Optional set of kwargs to be passed to the pandas read method.

    Returns:
        A pandas DataFrame read from Kaggle if successful, if the dataset or handle could
        not be found, this will raise an error.
    """
    pandas_df = kagglehub.dataset_load(
        adapter=KaggleDatasetAdapter.PANDAS,
        handle=handle,
        path=path,
        pandas_kwargs=pandas_kwargs,
    )
    return pandas_df
