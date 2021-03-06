from datetime import time
from pandas.core.frame import DataFrame
from dashboard_components.api_abstraction import aggregate
import pandas as pd
from functools import lru_cache


def fix_dates(df):
    if "byQuarter" in df.columns:
        df['x_axes'] = pd.to_datetime(
            df['byQuarter'].str.replace(r'(Q\d) (\d+)', r'\2-\1'), errors='coerce')
    elif "byYear" in df.columns:
        df["x_axes"] = pd.to_datetime(df.byYear)
    elif "byMonth" in df.columns:
        df["x_axes"] = pd.to_datetime(df.byMonth)

    df = df.sort_values("x_axes", ascending=True)
    return df


def number_of_cases(log_id, api_key, tfs=[]) -> int:
    """Returns the number of cases."""

    df = aggregate(api_key,
                   trace_filter_sequence=tfs,
                   log_id=log_id,
                   metric="frequency",
                   aggregation_function="sum"
                   )
    return int(df["frequency"].iloc[0])


def average_cases_per_month(log_id, api_key, tfs=[]) -> float:
    """Returns the average number of cases per month as float with 2 digits."""
    df = aggregate(api_key=api_key,
                   log_id=log_id,
                   trace_filter_sequence=tfs,
                   metric="frequency",
                   grouping="byMonth",
                   date_type="startDate",
                   max_amount_attributes=10000
                   )
    avg = round(df.caseCount.mean(), 2)
    return float(avg)


def median_case_duration(log_id, api_key, tfs=[]) -> float:
    """Returns median case duration in seconds."""
    df = aggregate(api_key=api_key,
                   log_id=log_id,
                   trace_filter_sequence=tfs,
                   metric="duration",
                   aggregation_function="median"
                   )
    return float(df["duration"].values[0] / 1000)


def percentile_duration(log_id, api_key, percentile, tfs=[]) -> float:
    """Returns duration at given percentile in seconds."""
    df = aggregate(api_key=api_key,
                   log_id=log_id,
                   trace_filter_sequence=tfs,
                   metric="duration",
                   percentile=percentile
                   )
    return float(df["duration"].values[0] / 1000)


def cases_per_weekday(log_id, api_key, tfs=[]) -> pd.DataFrame:
    """Returns a dataframe with the number of cases starting and
       ending on each weekday, ordered by the weekday."""

    # pulling the data
    df_start = aggregate(api_key=api_key,
                         log_id=log_id,
                         trace_filter_sequence=tfs,
                         metric="frequency",
                         grouping="byDayOfWeek",
                         date_type="startDate",
                         max_amount_attributes=10000
                         )

    df_end = aggregate(api_key=api_key,
                       log_id=log_id,
                       trace_filter_sequence=tfs,
                       metric="frequency",
                       grouping="byDayOfWeek",
                       date_type="endDate",
                       max_amount_attributes=10000
                       )
    # renaming the columns
    df_end = df_end[["caseCount", "byDayOfWeek"]]
    df_end. columns = ["casesEnded", "byDayOfWeek"]

    df_start = df_start[["caseCount", "byDayOfWeek"]]
    df_start. columns = ["casesStarted", "byDayOfWeek"]

    # merging the two dataframes
    days = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
    df_comb = pd.merge(left=df_start, right=df_end, on="byDayOfWeek")
    df_comb["ordering_column"] = df_comb.byDayOfWeek.apply(lambda x: days.index(x))
    df_comb = df_comb.sort_values("ordering_column").drop(columns=["ordering_column"])

    return df_comb

def activity_per_time_unit(api_key, log_id, tfs, activities: list, time_unit: str) -> pd.DataFrame:
    if time_unit not in ["byYear", "byMonth", "byQuarter", "byDayOfWeek", "byDayOfYear", "byHourOfDay"]:
        raise Exception("Frequency Parameter not valid. Needs to be in: \
                    [ byYear, byMonth, byQuarter, byDayOfWeek, byDayOfYear, byHourOfDay ]")

    df = aggregate(api_key=api_key,
                   log_id=log_id,
                   trace_filter_sequence=tfs,
                   metric="frequency",
                   grouping=time_unit,
                   date_type="startDate",
                   secondary_grouping="byActivity",
                   secondary_activities=activities,
                   values_from="allEvents"
                   )
    return df


@lru_cache(maxsize=5)
def cases_per_time_unit(api_key, log_id, tfs, time_unit: str) -> pd.DataFrame:
    if time_unit not in ["byYear", "byMonth", "byQuarter", "byDayOfWeek", "byDayOfYear", "byHourOfDay"]:
        raise Exception("Frequency Parameter not valid. Needs to be in: \
                    [ byYear, byMonth, byQuarter, byDayOfWeek, byDayOfYear, byHourOfDay ]")

    df = aggregate(api_key=api_key,
                   log_id=log_id,
                   trace_filter_sequence=tfs,
                   metric="frequency", 
                   grouping=time_unit,
                   date_type="startDate", 
                   max_amount_attributes=10000,
                   values_from="allCases")
    df = fix_dates(df)
    return df


@lru_cache(maxsize=5)
def avg_growth_rate(api_key, log_id, tfs, time_unit: str, time_length: int = 12) -> float:
    df = aggregate(api_key=api_key,
                   log_id=log_id,
                   trace_filter_sequence=tfs,
                   metric="frequency", 
                   grouping=time_unit,
                   date_type="startDate", 
                   max_amount_attributes=10000,
                   values_from="allCases")
    df = fix_dates(df)
    df = df.sort_values("x_axes", ascending=True)

    df["C"] = df.frequency.pct_change().mul(100).round(2)
    length = - time_length
    avg_gr = df["C"][length:].mean()
    return round(avg_gr, 2)