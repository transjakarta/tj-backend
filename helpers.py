import utm, copy
import pandas as pd
import numpy as np
import datetime as dt
from typing import Union, Callable


def get_utm_crs(lat: float, lon: float) -> dict:
    zone = utm.from_latlon(lat, lon)[2]
    result = f"EPSG:326{zone:02d}" if lat >= 0 else f"EPSG:327{zone:02d}"
    return result


def drop_feature_ids(collection: dict) -> dict:
    new_features = []
    for f in collection["features"]:
        new_f = copy.deepcopy(f)
        if "id" in new_f:
            del new_f["id"]
        new_features.append(new_f)

    collection["features"] = new_features
    return collection


def timestr_to_seconds(
    x: Union[dt.date, str], *, inverse: bool = False, mod24: bool = False
) -> int:
    if not inverse:
        try:
            hours, mins, seconds = x.split(":")
            result = int(hours) * 3600 + int(mins) * 60 + int(seconds)
            if mod24:
                result %= 24 * 3600
        except Exception:
            result = np.nan
    else:
        try:
            seconds = int(x)
            if mod24:
                seconds %= 24 * 3600
            hours, remainder = divmod(seconds, 3600)
            mins, secs = divmod(remainder, 60)
            result = f"{hours:02d}:{mins:02d}:{secs:02d}"
        except Exception:
            result = np.nan
    return result


def is_not_null(df: pd.DataFrame, col_name: str) -> bool:
    if (
        isinstance(df, pd.DataFrame)
        and col_name in df.columns
        and df[col_name].notnull().any()
    ):
        return True
    else:
        return False


def is_metric(dist_units: str) -> bool:
    return dist_units in ["m", "km"]


def get_convert_dist(
    dist_units_in: str, dist_units_out: str
) -> Callable[[float], float]:
    di, do = dist_units_in, dist_units_out
    DU = ["ft", "mi", "m", "km"]
    if not (di in DU and do in DU):
        raise ValueError(f"Distance units must lie in {DU}")

    d = {
        "ft": {"ft": 1, "m": 0.3048, "mi": 1 / 5280, "km": 0.000_304_8},
        "m": {"ft": 1 / 0.3048, "m": 1, "mi": 1 / 1609.344, "km": 1 / 1000},
        "mi": {"ft": 5280, "m": 1609.344, "mi": 1, "km": 1.609_344},
        "km": {"ft": 1 / 0.000_304_8, "m": 1000, "mi": 1 / 1.609_344, "km": 1},
    }
    return lambda x: d[di][do] * x
