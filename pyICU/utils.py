# Desc: Utility functions for pyICU
# Date: 2021-04-17
import logging
import pandas as pd
import numpy as np
import re
from datetime import timedelta
from typing import List, Tuple
from .logger import CustomLogger

logger: logging.Logger = CustomLogger().get_logger()
# set logging level to DEBUG
logger.setLevel(logging.DEBUG)


def is_timestamp(s: str) -> bool:
    """Define a regular expression pattern for common timestamp formats"""
    timestamp_pattern = r"\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?"

    return bool(re.match(timestamp_pattern, str(s)))


def convert_timestamp_columns(df: pd.DataFrame) -> None:
    """Convert all columns that contain timestamps to datetime objects"""
    for column in df.columns:
        if all(df[column].dropna().apply(is_timestamp)):
            df[column] = pd.to_datetime(df[column])
            logger.info(f"Converted '{column}' to a timestamp.")
        else:
            logger.info(f"Column '{column}' does not contain timestamps.")


def check_or_convert_timestamp_columns(
    mapper: Tuple[pd.DataFrame, List[str]], convert_datetimes: bool = False
) -> None:
    """Checks if all columns in a list contain timestamps and converts them to datetime objects if convert_datetimes is True."""
    for pair in mapper:
        df = pair[0]
        columns = pair[1]
        for column in columns:
            if all(df.loc[:, column].dropna().apply(is_timestamp)):
                if convert_datetimes:
                    # Use .loc to make the modification explicit
                    df[column] = pd.to_datetime(df[column])
                    logger.info(f"Converted '{column}' to a timestamp.")
            else:
                logger.info(f"Column '{column}' does not contain timestamps.")


def intervention_after_event(
    event: pd.DataFrame,
    intervention: pd.DataFrame,
    event_time_indicator: str,
    observation_time: int,
    sample_indicator: str = "subject_id",
    intervention_time_indicator: str = "charttime",
    convert_datetimes: bool = False,
) -> List:
    """
    Checks if an intervention occurred within a specified time window after an event.

    Takes two dataframes, one containing events and one containing interventions, and returns a list of unique sample IDs for which an intervention occurred within a specified time window after an event.

    Parameters
    ----------
    event : pd.DataFrame
        Pandas DataFrame containing the event data.

        Expected structure:
        - Rows represent individual samples.
        - Columns contain the following:
            - '{sample_indicator}': Sample indicator (default: 'subject_id').
            - '{event_time_indicator}': Timestamp when the event occurred.

    intervention : pd.DataFrame
        Pandas DataFrame containing the continuous intervention data.

        Expected structure:
        - Rows represent individual samples.
        - Columns contain the following:
            - '{sample_indicator}': Sample indicator (default: 'subject_id').
            - '{intervention_time_indicator}': Timestamp indicating the start of the intervention (default: 'charttime').
    event_time_indicator : str
        Column name of the event timestamp column.
    observation_time : int
        Time window in hours after the event to check for interventions.
    sample_indicator : str, optional
        Column name of the sample indicator column, by default "subject_id"
    intervention_time_indicator : str, optional
        Column name of the intervention timestamps column, by default "charttime"
    convert_datetimes : bool, optional
        If True, converts all columns that contain timestamps to datetime objects, by default False

    Returns
    -------
    List
        List of unique sample IDs for which an intervention occurred within a specified time window after an event.

    Raises
    ------
    ValueError
        Raised if the time indicator columns do not contain timestamps and convert_datetimes is False.
    """
    # check if time indicator cols are timestamps and convert or throw error
    check_or_convert_timestamp_columns(
        [
            (event, [event_time_indicator]),
            (intervention, [intervention_time_indicator]),
        ],
        convert_datetimes,
    )

    time_window = pd.Timedelta(hours=observation_time)

    merged_df = pd.merge(event, intervention, on=sample_indicator, how="inner")
    logging.debug(f"merged_df: {merged_df}")
    intervention_after_event_df = merged_df[
        (
            merged_df[intervention_time_indicator] <= (
                merged_df[event_time_indicator] + time_window)
        ) & (merged_df[intervention_time_indicator] >= merged_df[event_time_indicator])
    ]
    return intervention_after_event_df[sample_indicator].unique().tolist()


def continous_intervention_after_event(
    event: pd.DataFrame,
    intervention: pd.DataFrame,
    event_time_indicator: str,
    observation_time: int,
    sample_indicator: str = "subject_id",
    intervention_time_start_indicator: str = "starttime",
    intervention_time_end_indicator: str = "endtime",
    convert_datetimes: bool = False,
) -> List:
    """
    Checks if an intervention occurred within a specified time window after an event.

    Takes two dataframes, one containing events and one containing continous interventions (e.g. drug infusions over a certain time period), and returns a list of unique sample IDs for which an intervention occurred within a specified time window after an event.

    Parameters
    ----------
    event : pd.DataFrame
        Pandas DataFrame containing the event data.

        Expected structure:
        - Rows represent individual samples.
        - Columns contain the following:
            - '{sample_indicator}': Sample indicator (default: 'subject_id').
            - '{event_time_indicator}': Timestamp when the event occurred.

    intervention : pd.DataFrame
        Pandas DataFrame containing the continuous intervention data.

        Expected structure:
        - Rows represent individual samples.
        - Columns contain the following:
            - '{sample_indicator}': Sample indicator (default: 'subject_id').
            - '{intervention_time_start_indicator}': Timestamp indicating the start of the intervention (default: 'starttime').
            - '{intervention_time_end_indicator}': Timestamp indicating the end of the intervention (default: 'endtime').


    event_time_indicator : str
        Column name of the event timestamp column.
    observation_time : int
        Time window in hours after the event to check for interventions.
    sample_indicator : str, optional
        Column name of the sample indicator column, by default "subject_id"
    intervention_time_start_indicator : str, optional
        Column name of the intervention timestamps column indicating the start of the intervention, by default "charttime"
    intervention_time_end_indicator : str, optional
        Column name of the intervention timestamps column indicating the end of the intervention, by default "charttime"
    convert_datetimes : bool, optional
        If True, converts all columns that contain timestamps to datetime objects, by default False

    Returns
    -------
    List
        List of unique sample IDs for which an intervention occurred within a specified time window after an event.

    Raises
    ------
    ValueError
        Raised if the time indicator columns do not contain timestamps and convert_datetimes is False.
    """
    # check if time indicator cols are timestamps and convert or throw error
    check_or_convert_timestamp_columns(
        [
            (event, [event_time_indicator]),
            (
                intervention,
                [intervention_time_start_indicator,
                    intervention_time_end_indicator],
            ),
        ],
        convert_datetimes,
    )

    time_window = pd.Timedelta(hours=observation_time)

    merged_df = pd.merge(event, intervention, on=sample_indicator, how="inner")
    logging.debug(f"merged_df: {merged_df}")

    continous_intervention_after_event_df = merged_df[
        (
            (
                merged_df[intervention_time_start_indicator] > merged_df[event_time_indicator]
            ) & (
                merged_df[intervention_time_start_indicator] < merged_df[event_time_indicator] + time_window
            )
        ) | (
            (
                merged_df[intervention_time_start_indicator] < merged_df[event_time_indicator]
            ) & (
                merged_df[intervention_time_end_indicator] > merged_df[event_time_indicator]
            )
        )
    ]

    return continous_intervention_after_event_df[sample_indicator].unique().tolist()


def calculate_time_overlap(
    event_time, observation_time, intervention_time_start, intervention_time_end
):
    observation_window_end = event_time + timedelta(hours=observation_time)
    # Check if the intervention overlaps with the observation window
    if (
        intervention_time_start <= observation_window_end and intervention_time_end >= event_time
    ):
        # Calculate the overlap time
        overlap_start = max(intervention_time_start, event_time)
        overlap_end = min(intervention_time_end, observation_window_end)
        # calculate overlap in hours
        overlap = (overlap_end - overlap_start).total_seconds() / 3600
        return overlap
    else:
        return 0


def dosage_during_event(
    event: pd.DataFrame,
    intervention: pd.DataFrame,
    event_time_indicator: str,
    observation_time: int,
    sample_indicator: str = "subject_id",
    intervention_time_start_indicator: str = "starttime",
    intervention_time_end_indicator: str = "endtime",
    intervention_dosage_indicator: str = "amount",
    convert_datetimes: bool = False,
    check_for_duplicates: bool = True,
) -> pd.DataFrame:
    """
    Calculates the dosage of an intervention that occurred within a specified time window after an event.

    Takes two dataframes, one containing events and one containing interventions such as medications that were administered to a patient together with the start and end of the administration. Returns a dataframe containing the dosage of the intervention that occurred within a specified time window after an event.

    Parameters
    ----------
    event : pd.DataFrame
        Pandas DataFrame containing the event data.

        Expected structure:
        - Rows represent individual samples.
        - Columns contain the following:
            - '{sample_indicator}': Sample indicator (default: 'subject_id').
            - '{event_time_indicator}': Timestamp when the event occurred.

    intervention : pd.DataFrame
        Pandas DataFrame containing the continuous intervention data.

        Expected structure:
        - Rows represent individual samples.
        - Columns contain the following:
            - '{sample_indicator}': Sample indicator (default: 'subject_id').
            - '{intervention_time_start_indicator}': Timestamp indicating the start of the intervention (default: 'starttime').
            - '{intervention_time_end_indicator}': Timestamp indicating the end of the intervention (default: 'endtime').
            - '{intervention_dosage_indicator}': Dosage of the intervention (default: 'amount').

    event_time_indicator : str
        Column name of the event timestamp column.
    observation_time : int
        Time window in hours after the event to check for interventions.
    sample_indicator : str, optional
        Column name of the sample indicator column, by default "subject_id"
    intervention_time_indicator : str, optional
        Column name of the intervention timestamps column, by default "charttime"
    convert_datetimes : bool, optional
        If True, converts all columns that contain timestamps to datetime objects, by default False
    check_for_duplicates : bool, optional
        If True, removes duplicate rows from the merged dataframe, by merging, there might be some duplicates introduced into the resulting dataframe, especially if the sample size is small or samples are rounded to the nearest hour. By checking for duplicates, these duplicates will be removed. However, by doing so some interventions actually happening at the same timepoints might get deleted, by default True

    Returns
    -------
    List
        List of unique sample IDs for which an intervention occurred within a specified time window after an event.

    Raises
    ------
    ValueError
        Raised if the time indicator columns do not contain timestamps and convert_datetimes is False.
    """
    # check if time indicator cols are timestamps and convert or throw error
    check_or_convert_timestamp_columns(
        [
            (event, [event_time_indicator]),
            (
                intervention,
                [intervention_time_start_indicator,
                    intervention_time_end_indicator],
            ),
        ],
        convert_datetimes,
    )
    merged_df = pd.merge(event, intervention, on=sample_indicator, how="left")
    if check_for_duplicates is True:
        merged_df = merged_df.drop_duplicates()
    # calculate dosage per time
    merged_df["duration_of_intervention"] = (
        merged_df[intervention_time_end_indicator] -
        merged_df[intervention_time_start_indicator]
    )
    merged_df["dosage_per_hour"] = (
        merged_df[intervention_dosage_indicator] /
        merged_df["duration_of_intervention"].dt.total_seconds() * 3600
    )
    # calculate time of dosage during observation window
    merged_df["end_of_observation"] = merged_df[event_time_indicator] + pd.Timedelta(
        hours=observation_time
    )
    merged_df["overlap_in_hours"] = merged_df.apply(
        lambda row: calculate_time_overlap(
            row[event_time_indicator],
            observation_time,
            row[intervention_time_start_indicator],
            row[intervention_time_end_indicator],
        ),
        axis=1,
    )
    # multiply dosage per time with the time of dosage during observation window
    merged_df["dosage_during_overlap"] = (
        merged_df["dosage_per_hour"] * merged_df["overlap_in_hours"]
    )
    # sum up dosages
    dosage_during_event_df = merged_df.groupby(sample_indicator).agg(
        {"dosage_during_overlap": "sum"}
    )
    # rename dosage column
    dosage_during_event_df.rename(
        columns={"dosage_during_overlap": "total_" +
                 intervention_dosage_indicator},
        inplace=True,
    )
    dosage_during_event_df.reset_index(inplace=True)

    return dosage_during_event_df


def count_events_during_observation(
    event: pd.DataFrame,
    intervention: pd.DataFrame,
    event_time_indicator: str,
    observation_time: int,
    sample_indicator: str = "subject_id",
    intervention_time_indicator: str = "charttime",
    convert_datetimes: bool = True,
) -> pd.DataFrame:
    """
    Counts the number of eventsthat occurred within a specified time window after an event.

    Takes two dataframes, one containing events and one containing interventions such as measurements that were taken from a patient
    together with the time of measurement. Returns a dataframe containing the total counts of the intervention that occurred within a specified time window after an event.

    Parameters
    ----------
    event : pd.DataFrame
        Pandas DataFrame containing the event data.

        Expected structure:
        - Rows represent individual samples.
        - Columns contain the following:
            - '{sample_indicator}': Sample indicator (default: 'subject_id').
            - '{event_time_indicator}': Timestamp when the event occurred.

    intervention : pd.DataFrame
        Pandas DataFrame containing the intervention data.

        Expected structure:
        - Rows represent individual samples.
        - Columns contain the following:
            - '{sample_indicator}': Sample indicator (default: 'subject_id').
            - '{intervention_time_indicator}': Timestamp indicating the timing of the intervention (default: 'endtime').
            - '{intervention_value_indicator}': Dosage of the intervention (default: 'valuenum').
    event_time_indicator : str
        Column name of the event timestamp column.
    observation_time : int
        Time window in hours after the event to check for interventions.
    sample_indicator : str, optional
        Column name of the sample indicator column, by default "subject_id"
    intervention_time_indicator : str, optional
        Column name of the intervention timestamps column, by default "charttime"
    convert_datetimes : bool, optional
        If True, converts all columns that contain timestamps to datetime objects, by default False

    Returns
    -------
    List
        List of unique sample IDs for which an intervention occurred within a specified time window after an event.

    Raises
    ------
    ValueError
        Raised if the time indicator columns do not contain timestamps and convert_datetimes is False.
    """
    # check if time indicator cols are timestamps and convert or throw error
    check_or_convert_timestamp_columns(
        [
            (event, [event_time_indicator]),
            (intervention, [intervention_time_indicator]),
        ],
        convert_datetimes,
    )
    merged_df = pd.merge(event, intervention, on=sample_indicator, how="inner")
    merged_df.dropna(subset=[intervention_time_indicator], inplace=True)
    # count interventions during observation window
    merged_df["end_of_observation"] = merged_df[event_time_indicator] + pd.Timedelta(
        hours=observation_time
    )
    # count interventions during observation window
    merged_df["intervention_during_observation"] = merged_df.apply(
        lambda row: 1
        if (
            row[intervention_time_indicator] >= row[event_time_indicator] and row[intervention_time_indicator] <= row["end_of_observation"]
        )
        else 0,
        axis=1,
    )
    intervention_during_observation_df = merged_df.groupby(sample_indicator).agg(
        {"intervention_during_observation": "sum"}
    )
    intervention_during_observation_df.rename(
        columns={"intervention_during_observation": "total_interventions"}, inplace=True
    )
    intervention_during_observation_df.reset_index(inplace=True)
    return intervention_during_observation_df


def count_events_per_day_during_observation(
    event: pd.DataFrame,
    intervention: pd.DataFrame,
    event_time_indicator: str,
    observation_time: int,
    sample_indicator: str = "subject_id",
    intervention_time_indicator: str = "charttime",
    convert_datetimes: bool = True,
) -> pd.DataFrame:
    """
    Counts the number of events per day that occurred within a specified time window after an event.

    Takes two dataframes, one containing events and one containing interventions such as measurements that were taken from a patient
    together with the time of measurement. Returns a dataframe containing the total counts of the intervention per day that occurred within a specified time window after an event.

    Parameters
    ----------
    event : pd.DataFrame
        Pandas DataFrame containing the event data.

        Expected structure:
        - Rows represent individual samples.
        - Columns contain the following:
            - '{sample_indicator}': Sample indicator (default: 'subject_id').
            - '{event_time_indicator}': Timestamp when the event occurred.

    intervention : pd.DataFrame
        Pandas DataFrame containing the continuous intervention data.

        Expected structure:
        - Rows represent individual samples.
        - Columns contain the following:
            - '{sample_indicator}': Sample indicator (default: 'subject_id').
            - '{intervention_time_indicator}': Timestamp indicating the timing of the intervention (default: 'endtime').
            - '{intervention_dosage_indicator}': Dosage of the intervention (default: 'amount').
    event_time_indicator : str
        Column name of the event timestamp column.
    observation_time : int
        Time window in hours after the event to check for interventions.
    sample_indicator : str, optional
        Column name of the sample indicator column, by default "subject_id"
    intervention_time_indicator : str, optional
        Column name of the intervention timestamps column, by default "charttime"
    convert_datetimes : bool, optional
        If True, converts all columns that contain timestamps to datetime objects, by default False

    Returns
    -------
    List
        List of unique sample IDs for which an intervention occurred within a specified time window after an event.

    Raises
    ------
    ValueError
        Raised if the time indicator columns do not contain timestamps and convert_datetimes is False.
    """
    check_or_convert_timestamp_columns(
        [
            (event, [event_time_indicator]),
            (intervention, [intervention_time_indicator]),
        ],
        convert_datetimes,
    )
    merged_df = pd.merge(event, intervention, on=sample_indicator, how="inner")
    merged_df.dropna(subset=[intervention_time_indicator], inplace=True)
    # exclude rows outside observation window
    merged_df = merged_df.loc[
        (
            (merged_df[intervention_time_indicator] >= merged_df[event_time_indicator]) & (
                merged_df[intervention_time_indicator] <= merged_df[event_time_indicator] +
                pd.Timedelta(observation_time, unit="h")
            )
        )
    ]

    # count interventions per day during observation window
    merged_df[intervention_time_indicator +
              "_day"] = merged_df[intervention_time_indicator].dt.floor('d')
    # count measurements per patient per 24 hours
    grouped_df = merged_df.groupby(
        [sample_indicator, intervention_time_indicator + "_day"]
    ).size().reset_index(name="count_per_day")

    return grouped_df


def time_based_violation(
    threshold,
    delta_value,
    value_dict,
    violation="high",
    sample_indicator="stay_id_value",
) -> list:
    """
    Get the stay_ids of patients who violated a time based KDIGO bundle feature.

    A certain set of features in the KDIGO bundle is time based, meaning it needs to be measured within a certain time frame. This function checks if a patient violated this rule. It iterates over each row in the value dict and checks if the stay_id is the same as in the previous row. If it is, it checks if the value is above or below the threshold and if the time difference between the current row and the previous row is within the delta. If all of these conditions are met, the patient violated the rule and the stay_id is added to the list of stay_ids of patients who violated the rule.

    Parameters
    ----------
    threshold : int
        Value that indicates the threshold for the rule.
    delta_value : int
        Time difference between two measurements in hours.
    value_dict : dict
        Dictionary containing the values and timestamps for a target of the KDIGO bundle.
    violation : str, optional
        Indicator if a high value is a violation (e.g. hyperglycaemia) or a low value (e.g. hypotension), by default "high"

    Returns
    -------
    list
        List of stay_ids of patients who violated the rule.
    """
    violation_ids = (
        list()
    )  # empty list to hold stay_ids of patients who violated the rule
    tmp = {}  # empty dict to hold the previous row
    for row in value_dict:  # iterate over each row in the value dict
        try:
            # check if stay_id is the same as in the previous row
            if row[sample_indicator] == tmp[sample_indicator]:
                # extract values into variables
                value = row["valuenum"]
                previous_value = tmp["valuenum"]
                charttime = row["charttime"]
                previous_charttime = tmp["charttime"]

                if (
                    violation == "low"
                ):  # if we are searching for low values (e.g. hypotension)
                    if (
                        value > threshold
                    ):  # if the value is over the threshold, we can skip the rest of the loop
                        tmp = row
                        continue

                    # if the value is below the threshold, we check if the previous value was also below the threshold
                    if (value <= threshold and previous_value <= threshold) and (
                        charttime - previous_charttime < pd.Timedelta(
                            delta_value, unit="h")
                    ):
                        continue
                    if (value <= threshold and previous_value <= threshold) and (
                        charttime - previous_charttime >= pd.Timedelta(
                            delta_value, unit="h")
                    ):
                        # if conditions are met, add stay_id to list
                        violation_ids.append(row[sample_indicator])
                        tmp = row

                if violation == "high":
                    if value < threshold:
                        tmp = row
                        continue
                    if (value >= threshold and previous_value >= threshold) and (
                        charttime - previous_charttime < pd.Timedelta(
                            delta_value, unit="h")
                    ):
                        continue
                    if (value >= threshold and previous_value >= threshold) and (
                        charttime - previous_charttime >= pd.Timedelta(
                            delta_value, unit="h")
                    ):
                        # if conditions are met, add stay_id to list
                        violation_ids.append(row[sample_indicator])
                        tmp = row
            else:
                tmp = row
                continue
        except (
            KeyError
        ):  # if the previous row is empty, this is the first iteration in the loop, skip it
            tmp = row
            continue

    return violation_ids


def get_time_based_violation_dict(
    target_df: pd.DataFrame,
    sample_df: pd.DataFrame,
    observation_time: str,
    delta: int,
    time_indicator: str = "charttime",
    sample_indicator: str = "stay_id",
) -> np.ndarray:
    """

    Parameters
    ----------
    target_df : pandas.DataFrame
        Dataframe containing the values and timestamps for a target of the KDIGO bundle.
    sample_df : pandas.DataFrame
        Dataframe containing the samples of patients we want to investigate.
    observation_time : string
        String indicator of the column containing the observation time from which we start the observation.
    delta : int
        Time window for observation time in hours.
    delta_value : int
        Time difference between two measurements in hours.
    threshold : int
        Value that indicates the threshold for the rule.
    violation : str, optional
        Indicator if a high value is a violation (e.g. hyperglycaemia) or a low value (e.g. hypotension), by default "high"
    time_indicator : str, optional
        String indicator of the column containing the timestamps in target_df, by default "charttime"
    sample_indicator : str, optional
        String indicator of the column containing the samples in both dataframes, by default "stay_id"

    Returns
    -------
    numpy.ndarray
        Array containing the stay_ids of patients who violated the bundle.
    """

    df = pd.merge(
        target_df, sample_df, on=sample_indicator, how="left"
    )  # merge the target df with the samples df
    df.sort_values(by=[sample_indicator, time_indicator], inplace=True)
    df[observation_time] = pd.to_datetime(
        df[observation_time]
    )  # ensure observation time is timestamp
    df.dropna(
        subset=[observation_time], inplace=True
    )  # drop rows where the observation time is missing

    df = df.loc[
        (
            (df[time_indicator] > df[observation_time]) & (
                df[time_indicator] < df[observation_time] +
                pd.Timedelta(delta, unit="h")
            )
        ),
        :,
    ].copy()  # select rows where the timestamp is within the observation time + delta

    df.sort_values(
        by=[sample_indicator, time_indicator], inplace=True
    )  # sort the dataframe by stay_id and timestamp

    df[time_indicator] = pd.to_datetime(
        df[time_indicator]
    )  # ensure time indicator column is timestamp

    value_dict = df.to_dict("records")  # convert the dataframe to a dict

    return value_dict
