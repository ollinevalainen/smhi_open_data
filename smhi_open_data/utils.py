import pandas as pd
from datetime import datetime
from math import cos, asin, sqrt, pi
from typing import Union

from smhi_open_data.smhi_open_data.enums import Parameter

# Constants
CONST_EARTH_RADIUS = 6371  # km
CONST_EARTH_DIAMETER = 12742  # km
EPOCH = datetime.utcfromtimestamp(0)

# constants for archived parameters
# depending on parameter the archived csv from SMHI has different data columns
ARCHIVED_PARAMETER_GROUP1 = [Parameter.TemperaturePast1h.name, Parameter.Humidity.name]
ARCHIVED_PARAMETER_GROUP2 = [Parameter.PrecipPast24hAt06.name]


def date2microseconds(date: datetime) -> int:
    return int((date - EPOCH).total_seconds() * 1000000.0)


def microseconds2date(microseconds: float) -> datetime:
    return datetime.utcfromtimestamp(microseconds / 1000000)


def try_parse_float(x: Union[str, float, int]) -> Union[float, str]:
    try:
        return float(x)
    except ValueError:
        return x


# From Stackoverflow answer
# https://stackoverflow.com/a/21623206/2538589
def distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in km between two geographical points.

    Args:
        lat1 (float): Latitude of point 1.
        lon1 (float): Longitude of point 1.
        lat2 (float): Latitude of point 2.
        lon2 (float): Longitude of point 2.

    Returns:
        float: Haversine distance in km between point 1 and 2.
    """
    p = pi / 180.0
    a = (
        0.5
        - cos((lat2 - lat1) * p) / 2.0
        + cos(lat1 * p) * cos(lat2 * p) * (1.0 - cos((lon2 - lon1) * p)) / 2
    )
    return CONST_EARTH_DIAMETER * asin(sqrt(a))  # 2*R*asin...


def json_to_dataframe(json_data: dict, parameter: Parameter) -> pd.DataFrame:
    """Turn JSON data to pandas DataFrameß
    """
    # create a data frame from the JSON data
    df = pd.DataFrame(json_data)
    if parameter.name in ARCHIVED_PARAMETER_GROUP2:
        # convert timestamps to datetime
        df["date"] = pd.to_datetime(df.ref, utc=True)
        # convert value to float64
        df["value"] = df.value.astype(float)
        df = df.iloc[:, [-1, 3, 4]]
    else:
        # convert timestamps to datetime
        df["date"] = pd.to_datetime(df.date, unit="ms", utc=True)
        # convert value to float64
        df["value"] = df.value.astype(float)
    return df


def format_archived_dataframe(df: pd.DataFrame, parameter: Parameter) -> pd.DataFrame:
    """Format SMHI archived data to unified DataFrame format.
    """

    if parameter.name in ARCHIVED_PARAMETER_GROUP1:
        df["date"] = pd.to_datetime(df.Datum, utc=True) + pd.to_timedelta(
            df["Tid (UTC)"]
        )
        df = df.iloc[:, [-1, 2, 3]]
        # rename the columns
        df.columns = ["date", parameter.name, "quality"]
        df = df.copy()
        # convert value to float64
        df[parameter.name] = df[parameter.name].astype(float)
    elif parameter.name in ARCHIVED_PARAMETER_GROUP2:

        df["date"] = pd.to_datetime(df["Representativt dygn"], utc=True)
        df = df.iloc[:, [-1, 3, 4]]
        # rename the columns
        df.columns = ["date", parameter.name, "quality"]
        df = df.copy()
        # convert value to float64
        df[parameter.name] = df[parameter.name].astype(float)

    else:
        raise NotImplementedError(
            """format_archived_dataframe function not yet implemented or tested with parameter {}""".format(
                parameter.name
            )
        )

    return df


def combine_archived_and_latest_months(
    corrected_df: pd.DataFrame, latest_months_df: pd.DataFrame, combine_since: str
) -> pd.DataFrame:
    """Combine archived and latest months data to a single dataframe. Latest months' data is appended after latest archived observation.
    """

    combined_df = (
        corrected_df[corrected_df.date >= combine_since]
        .append(latest_months_df[latest_months_df.date > corrected_df.date.max()])
        .reset_index(drop=True)
    )

    return combined_df
