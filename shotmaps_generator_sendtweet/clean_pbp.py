"""
This module contains methods for cleaning scraped play by play data frames.
Huge thanks to Matt Barlowe for his help & providing a lot of this code!
"""

import logging

import numpy as np
import pandas as pd


def clean_df(df):
    """
    This function cleans the newly created dataframe to fill in any missing
    values and fill down merged values.

    Inputs:
    df - merged data frame

    Outputs:
    df - df with all values filled down and cleaned
    """

    # Fills all NA values with zeroes
    df["xc"] = df["xc"].replace("", "0", regex=False)
    df["yc"] = df["yc"].replace("", "0", regex=False)
    df.loc[:, ("xc")] = df.loc[:, ("xc")].fillna(0).astype(int)
    df.loc[:, ("yc")] = df.loc[:, ("yc")].fillna(0).astype(int)

    # Fills NA values with the names of the appropriate teams
    df.loc[:, ("away_team")] = df.loc[:, ("away_team")].fillna(df.away_team.unique()[0])
    df.loc[:, ("home_team")] = df.loc[:, ("home_team")].fillna(df.home_team.unique()[0])

    # Calculates new running scores to fill in the NaNs
    df.loc[:, ("away_score")] = np.where(
        (df.event == "GOAL") & (df.ev_team == df.away_team.unique()[0]), 1, 0
    ).cumsum()
    df.loc[:, ("home_score")] = np.where(
        (df.event == "GOAL") & (df.ev_team == df.home_team.unique()[0]), 1, 0
    ).cumsum()

    return df


def run_all_stats(pbp_df):
    """
    This function runs all the functions below to clean the dataframe
    and perform other stat generation functions.

    Inputs:
    pbp_df - basic scraped pbp_df

    Outputs:
    pbp_df - pbp_df with all fixes and stats applied
    """

    # Run all cleaning & stat generation functions
    pbp_df = fix_seconds_elapsed(pbp_df)
    pbp_df = fixed_blocked_shots(pbp_df)
    pbp_df = calc_time_diff(pbp_df)
    pbp_df = calc_shot_metrics(pbp_df)
    pbp_df = calc_distance_togoal(pbp_df)
    pbp_df = calc_score_diff(pbp_df)
    pbp_df = calc_rebound(pbp_df)
    pbp_df = calc_rush_shot(pbp_df)
    pbp_df = calc_is_home(pbp_df)
    pbp_df = calc_was_tied(pbp_df)
    pbp_df = calc_shot_quality_area(pbp_df)
    pbp_df = calc_shot_danger(pbp_df)
    pbp_df = calc_is_scoring_chance(pbp_df)
    pbp_df = calc_team_strength(pbp_df)

    return pbp_df


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Other Stuff (Including DF Apply Functions)
# ------------------------------------------------------------------------------


def point_in_triangle(point, triangle):
    """Returns True if the point is inside the triangle
    and returns False if it falls outside.
    - The argument *point* is a tuple with two elements
    containing the X,Y coordinates respectively.
    - The argument *triangle* is a tuple with three elements each
    element consisting of a tuple of X,Y coordinates.
    """

    x, y = point
    ax, ay = triangle[0]
    bx, by = triangle[1]
    cx, cy = triangle[2]

    # Segment A to B
    side_1 = (x - bx) * (ay - by) - (ax - bx) * (y - by)
    # Segment B to C
    side_2 = (x - cx) * (by - cy) - (bx - cx) * (y - cy)
    # Segment C to A
    side_3 = (x - ax) * (cy - ay) - (cx - ax) * (y - ay)
    # All the signs must be positive or all negative
    return (side_1 < 0.0) == (side_2 < 0.0) == (side_3 < 0.0)


def dfapply_danger_area(row):
    x = abs(row["xc"])
    y = abs(row["yc"])
    corsi = bool(row["is_corsi"] == 1)
    md_triangle = ((70, 23), (90, 8), (70, 8))

    if not corsi:
        return None

    # Shots in the non-offensive zones are scored as 0
    if row["ev_zone"] != "Off" and corsi:
        return 0

    # High Danger Shots = 3
    if 69 <= x <= 89 and y <= 9:
        return 3
    # Medium Danger Coordinates & Lower Triangle
    elif (44 <= x <= 54 and y <= 9) or (54 <= x <= 69 and y <= 22) or point_in_triangle((x, y), md_triangle):
        return 2
    # Any other offensive zone coordinates are low danger
    elif row["ev_zone"] == "Off":
        return 1
    else:
        return 0


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# PBP Cleaning Methods
# ------------------------------------------------------------------------------


def fix_seconds_elapsed(pbp_df):
    """
    This function fixes the seconds elapsed column to tally the total seconds
    elapsed for the whole game instead of just seconds elapsed for the period

    Inputs:
    pbp_df - pbp_df without seconds_elapsed fixed

    Outputs:
    pbp_df - pbp_df with seconds_elapsed correctly calculated
    """

    logging.info("Fixing seconds elapsed within dataframe.")
    pbp_df.loc[:, "seconds_elapsed"] = pbp_df.loc[:, "seconds_elapsed"] + (1200 * (pbp_df.period - 1))

    return pbp_df


def calc_distance_togoal(pbp_df):
    """
    This function calculates the distance from the coordinate given for the
    event to the center of the goal

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with distance calculated
    """
    logging.info("Calculating shot distance within dataframe.")
    pbp_df.loc[:, ("distance_togoal")] = np.sqrt((87.95 - abs(pbp_df.xc)) ** 2 + pbp_df.yc ** 2)

    return pbp_df


def fixed_blocked_shots(pbp_df):
    """
    This function switches the p1 and p2 of blocked shots because Harry's
    scraper lists p1 as the blocker instead of the shooter. It also adds a
    SHOT_QUALITY_BLOCKED column for calculating danger.

    Inputs:
    pbp_df - dataframe of play by play to be cleaned

    Outputs:
    pbp_df - cleaned dataframe
    """

    logging.info("Switching blocked shots P1 & P2 within dataframe.")

    # Create new columns to switch blocked shots
    pbp_df.loc[:, ("new_p1_name")] = np.where(pbp_df.event == "BLOCK", pbp_df.p2_name, pbp_df.p1_name)
    pbp_df.loc[:, ("new_p2_name")] = np.where(pbp_df.event == "BLOCK", pbp_df.p1_name, pbp_df.p2_name)
    pbp_df.loc[:, ("new_p1_id")] = np.where(pbp_df.event == "BLOCK", pbp_df.p2_id, pbp_df.p1_id)
    pbp_df.loc[:, ("new_p2_id")] = np.where(pbp_df.event == "BLOCK", pbp_df.p1_id, pbp_df.p2_id)

    # Saving new columns as old columns
    pbp_df.loc[:, ("p1_name")] = pbp_df["new_p1_name"]
    pbp_df.loc[:, ("p2_name")] = pbp_df["new_p2_name"]
    pbp_df.loc[:, ("p1_id")] = pbp_df["new_p1_id"]
    pbp_df.loc[:, ("p2_id")] = pbp_df["new_p2_id"]

    # Drop unused columns
    pbp_df = pbp_df.drop(["new_p1_name", "new_p2_name", "new_p1_id", "new_p2_id"], axis=1)

    # Add an extra SHOT_QUALITY_BLOCKED column
    pbp_df.loc[:, ("shot_quality_blocked")] = np.where(pbp_df.event == "BLOCK", -1, 0)

    # Flip Off & Def Zones for Blocked Shots
    flipped_zones = {"Off": "Def", "Neu": "Neu", "Def": "Off"}
    pbp_df["ev_zone"].replace(flipped_zones, inplace=True)

    # Return cleaned DF
    return pbp_df


def calc_shot_metrics(pbp_df):
    """
    function to calculate whether an event is a corsi or fenwick event

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    pbp_df - play by play dataframe with corsi and fenwick columns calculated
    """
    logging.info("Calculating shot metrics within dataframe.")

    corsi = ["SHOT", "BLOCK", "MISS", "GOAL"]
    fenwick = ["SHOT", "MISS", "GOAL"]
    shot = ["SHOT", "GOAL"]

    pbp_df.loc[:, ("is_corsi")] = np.where(pbp_df.event.isin(corsi), 1, 0)
    pbp_df.loc[:, ("is_fenwick")] = np.where(pbp_df.event.isin(fenwick), 1, 0)
    pbp_df.loc[:, ("is_shot")] = np.where(pbp_df.event.isin(shot), 1, 0)
    pbp_df.loc[:, ("is_goal")] = np.where(pbp_df.event == "GOAL", 1, 0)

    return pbp_df


def calc_is_home(pbp_df):
    """
    Function determines whether event was taken by the home team or not

    Inputs:
    pbp_df - play by play dataframe

    Outputs:
    pbp_df - play by play dataframe
    """
    logging.info("Flagging home team within dataframe.")

    pbp_df.loc[:, ("is_home")] = np.where(pbp_df.ev_team == pbp_df.home_team, 1, 0)

    return pbp_df


def calc_score_diff(pbp_df):
    """
    Function to calculate score differential for score adjustment caps at
    +/- 3 due to Micah Blake McCurdy's (@Ineffectivemath on Twitter) adjustment
    method.

    Input:
    pbp_df - play by play df

    Output:
    pbp_df - play by play df with score diff calculated
    """
    logging.info("Calculating score differential within dataframe.")

    pbp_df.loc[:, ("score_diff")] = pbp_df.home_score - pbp_df.away_score

    pbp_df.loc[:, ("score_diff")] = np.where(pbp_df.score_diff < -3, -3, pbp_df.score_diff)

    pbp_df.loc[:, ("score_diff")] = np.where(pbp_df.score_diff > 3, 3, pbp_df.score_diff)

    return pbp_df


def calc_time_diff(pbp_df):
    """
    This function calculates the time difference between events

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with time difference calculated
    """
    logging.info("Calculating time differential between events within dataframe.")
    pbp_df.loc[:, ("time_diff")] = pbp_df.seconds_elapsed - pbp_df.seconds_elapsed.shift(1)

    return pbp_df


def calc_was_tied(pbp_df):
    """
    This function determines if the score was tied when a goal was scored.

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with newly added was_tied column
    """
    logging.info("Calculating flag if previous event had a tie score.")
    pbp_df.loc[:, ("was_tied")] = np.where((pbp_df.event == "GOAL") & (pbp_df.score_diff.shift(1) == 0), 1, 0)

    return pbp_df


def calc_rebound(pbp_df):
    """
    This function calculates whether the corsi event was generated off of a
    goalie rebound by looking at the time difference between the current event
    and the last event and checking that last even was a shot as well

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with rebound calculated
    """
    logging.info("Determining if this event is a rebound within dataframe.")
    pbp_df.loc[:, ("is_rebound")] = np.where(
        (pbp_df.time_diff < 4)
        & (
            (pbp_df.is_corsi == 1)
            &
            #  ((pbp_df.event.isin(['SHOT', 'GOAL'])) &
            # (pbp_df.event.shift(1) == 'SHOT') &
            (pbp_df.is_corsi.shift(1) == 1)
            & (pbp_df.ev_team == pbp_df.ev_team.shift(1))
        ),
        1,
        0,
    )

    return pbp_df


def calc_rush_shot(pbp_df):
    """
    This function calculates whether the corsi event was generated off the rush
    by looking at the time difference between the last even and whether the last
    event occured in the neutral zone

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with is_rush calculated
    """

    logging.info("Determining if this event was on a rush within dataframe.")

    # Multiplying the x-coord of this event and the previous event allows to determine
    # if the event happened in separate zones. The <= 26 check is for the neutral zone.
    pbp_df.loc[:, ("is_rush")] = np.where(
        (pbp_df.time_diff < 5)
        & (pbp_df.event.isin(["SHOT", "MISS", "BLOCK", "GOAL"]))
        & ((pbp_df.xc.shift(1) * pbp_df.xc < 0) | (abs(pbp_df.xc.shift(1)) <= 26)),
        1,
        0,
    )

    return pbp_df


def calc_shot_quality_area(pbp_df):
    """
    This function calculates the area in which the corsi event was generated and
    applies a numeric score (0 - 3).
    (via https://www.naturalstattrick.com/glossary.php?lines)

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with shot_quality_area calculated
    """
    logging.info("Calculating shot quality danger by area within the dataframe.")
    pbp_df["shot_quality_area"] = pbp_df.apply(dfapply_danger_area, axis=1)

    return pbp_df


def calc_shot_danger(pbp_df):
    """
    This function uses shot quality events to determine the overall
    danger of a shot (high danger, med danger, low danger).
    (via https://www.naturalstattrick.com/glossary.php?lines)

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with shot_danger calculated
    """
    logging.info("Calculating total shot danger within the dataframe (HD, MD, LD).")
    sum_cols = ["shot_quality_area", "is_rush", "is_rebound", "shot_quality_blocked"]
    pbp_df["shot_danger"] = pbp_df[sum_cols].sum(axis=1)

    return pbp_df


def calc_is_scoring_chance(pbp_df):
    """
    This function uses the shot danger to determine if a corsi event
    was a scoring chance or not.
    (via https://www.naturalstattrick.com/glossary.php?lines)

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with shot_danger calculated
    """
    logging.info("Calculating scoring chances within the dataframe.")
    pbp_df.loc[:, ("is_scoring_chance")] = np.where((pbp_df.shot_danger >= 2), 1, 0)

    return pbp_df


def calc_team_strength(pbp_df):
    """
    This function adds flags for even strength, power play and
    penalty kill situations. Strength is homexaway.

    Input:
    pbp_df - play by play dataframe

    Output:
    pbp_df - play by play dataframe with strengths calculated
    """
    logging.info("Calculating team strengths within the dataframe.")

    even = ["5x5", "4x4", "3x3"]
    home_pp = ["6x4", "5x4", "5x3", "4x3"]
    home_pk = ["4x6", "4x5", "3x5", "3x4"]

    pbp_df.loc[:, ("is_even_strength")] = np.where(pbp_df.strength.isin(even), 1, 0)
    pbp_df.loc[:, ("is_home_pp")] = np.where(pbp_df.strength.isin(home_pp), 1, 0)
    pbp_df.loc[:, ("is_home_pk")] = np.where(pbp_df.strength.isin(home_pk), 1, 0)
    pbp_df.loc[:, ("is_away_pp")] = np.where(pbp_df.strength.isin(home_pk), 1, 0)
    pbp_df.loc[:, ("is_away_pk")] = np.where(pbp_df.strength.isin(home_pp), 1, 0)

    return pbp_df


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Shotmap Dataframe Modifications
# ------------------------------------------------------------------------------


def fix_df_periods(df):
    """
    Fixes dataframe for even number periods (by flipping coordinates).

    :param df: shots dataframe
    :return df: shots dataframe with fixed periods
    """
    df.loc[df.period % 2 == 0, ["xc", "yc"]] *= -1
    return df


def df_remove_blocked_shots(df):
    """
    Removes blocked shots from a dataframe (once CF stats are calculated).
    Blocked shot coordinates are at the blocked location, not at the shot location.

    :param df: shots dataframe
    :return df: shots dataframe with blocked shots removed
    """
    nonblock_df = df.loc[df["event"] != "BLOCK"]
    return nonblock_df


def df_remove_zerozero(df):
    """
    Removes events at (0,0) coordinates from a dataframe (once CF stats are calculated).
    For some reason some shots appear at (0,0) - not sure why but they screw up KDE.

    :param df: shots dataframe
    :return df: shots dataframe with without events at (0,0)
    """
    nonzero_df = df.loc[(df["xc"] != 0) & (df["yc"] != 0)]
    return nonzero_df


def split_df(df, home_team):
    """
    Splits a two-team shots dataframe into two separate dataframes (pref & other)
    with their shot coordinates all moved to the same size (per df).

    :param df: shots dataframe
    :param team_abbrev: 3-letter team abbreviation

    :return pref_df: preferred team shots dataframe
    :return other_df: other team shots dataframe
    """
    # Separate into 'Home' & 'Away' Teams
    home_df = df.loc[df["ev_team"] == home_team]
    away_df = df.loc[df["ev_team"] != home_team]

    # Verify all coordinates are on the same side
    # Preferred on the right, Other on the left
    home_df.loc[home_df.xc < 0, ["xc", "yc"]] *= -1
    away_df.loc[away_df.xc > 0, ["xc", "yc"]] *= -1

    return home_df, away_df
