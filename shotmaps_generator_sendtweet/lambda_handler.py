import json
import logging
import math
import os

import requests
import tweepy
import pandas as pd

# Custom Imports
import clean_pbp
import shotmap

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ordinal = lambda n: "%d%s" % (n, "tsnrhtdd"[(math.floor(n / 10) % 10 != 1) * (n % 10 < 4) * n % 10 :: 4])


def send_shotmap_discord(testing: bool, images: list, text: str):
    """ Takes a completed shotmap path & some text and sends out a message to a Discord webhook.
        Discord webhook URLs are stored in environment variables.

    Args:
        images: A list of paths to the completed shotmap(s)
        text: Any text to send alongside the images

    Returns:
        True if Discord sent or error if failed
    """

    # Create an empty list of files
    files = dict()

    webhook_url = os.environ.get("DISCORD_URL") if not testing else os.environ.get("DEBUG_DISCORD_URL")
    payload = {"content": text}

    for idx, image in enumerate(images):
        files_key = f"file{idx}"
        files[files_key] = open(image, "rb")

    response = requests.post(webhook_url, files=files, data=payload)


def send_shotmap_tweet(testing: bool, images: list, tweet_text: str):
    """ Takes a completed shotmap path & some text and sends out a tweet.
        Twitter keys are stored in environment variables.

    Args:
        images: A list of paths to the completed shotmap(s)
        tweet_text: Any text to send alongside the string

    Returns:
        True if tweet sent or TweepyError if failed
    """

    # Get keys from environment variables based on if this is a "test-run" or not.
    if testing:
        consumer_key = os.environ.get("DEBUG_TWTR_CONSUMER_KEY")
        consumer_secret = os.environ.get("DEBUG_TWTR_CONSUMER_SECRET")
        access_token = os.environ.get("DEBUG_TWTR_ACCESS_TOKEN")
        access_secret = os.environ.get("DEBUG_TWTR_ACCESS_SECRET")
    else:
        consumer_key = os.environ.get("TWTR_CONSUMER_KEY")
        consumer_secret = os.environ.get("TWTR_CONSUMER_SECRET")
        access_token = os.environ.get("TWTR_ACCESS_TOKEN")
        access_secret = os.environ.get("TWTR_ACCESS_SECRET")

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    try:
        # For multiple images, use the media upload API
        # https://github.com/tweepy/tweepy/issues/724#issuecomment-215927647
        media_ids = [api.media_upload(i).media_id_string for i in images]
        if api.update_status(status=tweet_text, media_ids=media_ids):
            return True
    except tweepy.error.TweepError as e:
        return e


def get_team_from_abbreviation(abbreviation: str):
    """ Takes a team abbreviation and returns the team short name.

    Args:
        abbreviation: 3-character team abbreviation

    Returns:
        {team_name, short_name}: A dictionary of team_name & short name
    """

    team_name_dict = {
        "NJD": {"team_name": "New Jersey Devils", "short_name": "Devils"},
        "NYI": {"team_name": "New York Islanders", "short_name": "Islanders"},
        "NYR": {"team_name": "New York Rangers", "short_name": "Rangers"},
        "PHI": {"team_name": "Philadelphia Flyers", "short_name": "Flyers"},
        "PIT": {"team_name": "Pittsburgh Penguins", "short_name": "Penguins"},
        "BOS": {"team_name": "Boston Bruins", "short_name": "Bruins"},
        "BUF": {"team_name": "Buffalo Sabres", "short_name": "Sabres"},
        "MTL": {"team_name": "Montréal Canadiens", "short_name": "Canadiens"},
        "OTT": {"team_name": "Ottawa Senators", "short_name": "Senators"},
        "TOR": {"team_name": "Toronto Maple Leafs", "short_name": "Maple Leafs"},
        "CAR": {"team_name": "Carolina Hurricanes", "short_name": "Hurricanes"},
        "FLA": {"team_name": "Florida Panthers", "short_name": "Panthers"},
        "TBL": {"team_name": "Tampa Bay Lightning", "short_name": "Lightning"},
        "WSH": {"team_name": "Washington Capitals", "short_name": "Capitals"},
        "CHI": {"team_name": "Chicago Blackhawks", "short_name": "Blackhawks"},
        "DET": {"team_name": "Detroit Red Wings", "short_name": "Red Wings"},
        "NSH": {"team_name": "Nashville Predators", "short_name": "Predators"},
        "STL": {"team_name": "St. Louis Blues", "short_name": "Blues"},
        "CGY": {"team_name": "Calgary Flames", "short_name": "Flames"},
        "COL": {"team_name": "Colorado Avalanche", "short_name": "Avalanche"},
        "EDM": {"team_name": "Edmonton Oilers", "short_name": "Oilers"},
        "VAN": {"team_name": "Vancouver Canucks", "short_name": "Canucks"},
        "ANA": {"team_name": "Anaheim Ducks", "short_name": "Ducks"},
        "DAL": {"team_name": "Dallas Stars", "short_name": "Stars"},
        "LAK": {"team_name": "Los Angeles Kings", "short_name": "Kings"},
        "SJS": {"team_name": "San Jose Sharks", "short_name": "Sharks"},
        "CBJ": {"team_name": "Columbus Blue Jackets", "short_name": "Blue Jackets"},
        "MIN": {"team_name": "Minnesota Wild", "short_name": "Wild"},
        "WPG": {"team_name": "Winnipeg Jets", "short_name": "Jets"},
        "ARI": {"team_name": "Arizona Coyotes", "short_name": "Coyotes"},
        "VGK": {"team_name": "Vegas Golden Knights", "short_name": "Golden Knights"},
    }

    return team_name_dict.get(abbreviation)


def lambda_handler(event, context):
    # logging.info(event)
    testing = event.get("testing")

    # Get the JSON-serialized DataFrame from the payload & convert back to a DataFrame
    pbp_json = event.get("pbp_json")
    pbp_json = json.dumps(pbp_json) if isinstance(pbp_json, dict) else pbp_json
    pbp_df = pd.read_json(pbp_json)

    # Fix Team Abbreviations (in both DFs)
    team_corrections = {"L.A": "LAK", "N.J": "NJD", "S.J": "SJS", "T.B": "TBL"}
    pbp_df.replace(
        {"ev_team": team_corrections, "home_team": team_corrections, "away_team": team_corrections},
        inplace=True,
    )

    # Get Home & Away team names from DF
    home_team = pbp_df.home_team.unique()[0]
    away_team = pbp_df.away_team.unique()[0]

    cols = " ".join(pbp_df.columns)
    df_stats = f"{cols}\nDF Stats: {len(pbp_df.index)} rows x {len(pbp_df.columns)} columns\nHome: {home_team}\nAway: {away_team}"
    print(df_stats)

    # Fix elapsed seconds first
    pbp_df = clean_pbp.fix_seconds_elapsed(pbp_df)
    pbp_df = clean_pbp.clean_df(pbp_df)

    # Then run all other cleaning & stat generation functions at once
    pbp_df = clean_pbp.run_all_stats(pbp_df)

    # Get the final event (period end or game end)
    game_end_events = len(pbp_df.loc[pbp_df["event"] == "GEND"])
    game_end = True if game_end_events > 0 else False

    # Filter out for Corsi-only events
    logging.info("Removing all non-corsi events as they should not be graphed.")
    pbp_df = pbp_df.loc[pbp_df["is_corsi"] == 1]

    pbp_df.replace(to_replace=["", "NA"], value=pd.np.nan, inplace=True)

    logging.info("Fixing periods & splitting dataframes into two teams.")
    pbp_df = clean_pbp.fix_df_periods(pbp_df)

    # Sort the dataframe by seconds elapsed so the last row is the latest event
    pbp_df = pbp_df.sort_values("seconds_elapsed")

    logging.info("Extracting only corsi events to graph on the shotmap.")
    home_df, away_df = clean_pbp.split_df(pbp_df, home_team)

    logging.info("Create 5v5 versions of each home & away shotmap.")
    home_df_5v5 = home_df.loc[home_df["strength"] == "5x5"]
    away_df_5v5 = away_df.loc[away_df["strength"] == "5x5"]

    # Instead of using the last row, try using the max function across those columns
    period = pbp_df.period.max()
    period_ordinal = ordinal(period)
    home_score = pbp_df.home_score.max()
    away_score = pbp_df.away_score.max()

    # Now that we have the scores, we can do one more game end check
    # If the period is 3 or higher and the scores are difference
    game_end = True if not game_end and period >= 3 and away_score != home_score else game_end

    home_team_names = get_team_from_abbreviation(home_team)
    away_team_names = get_team_from_abbreviation(away_team)

    # Generate the Shotmap
    shotmap_details = {
        "home": home_team_names,
        "away": away_team_names,
        "period": period_ordinal,
        "game_end": game_end,
    }
    completed_path = shotmap.generate_shotmap(
        home_df=home_df, away_df=away_df, details=shotmap_details, strength="All"
    )
    completed_path_5v5 = shotmap.generate_shotmap(
        home_df=home_df_5v5, away_df=away_df_5v5, details=shotmap_details, strength="5v5"
    )

    # Generate Tweet Strings Dynamically
    home_team_short = home_team_names["short_name"]
    away_team_short = away_team_names["short_name"]
    game_hashtag = f"#{away_team}vs{home_team}"
    game_status = "game" if game_end else f"{period_ordinal} period"

    if home_score == away_score:
        tweet_text = (
            f"At the end of the {period_ordinal} period, the {home_team_short} & "
            f"{away_team_short} are tied at 1."
            f"\n\n{game_hashtag}"
        )

    else:
        if home_score > away_score and game_end:
            lead_trail_status = "defeat"
        elif home_score > away_score and not game_end:
            lead_trail_status = "lead"
        elif home_score < away_score and game_end:
            lead_trail_status = "lose to"
        elif home_score < away_score and not game_end:
            lead_trail_status = "trail"

        tweet_text = (
            f"At the end of the {game_status} the {home_team_short} "
            f"{lead_trail_status} the {away_team_short} by a score of {home_score} to {away_score}."
            f"\n\n{game_hashtag}"
        )

    # Send the completed shotmap tweet
    shotmap_files = [completed_path, completed_path_5v5]
    status = send_shotmap_tweet(testing=testing, images=shotmap_files, tweet_text=tweet_text)
    discord_status = send_shotmap_discord(testing=testing, images=shotmap_files, text=tweet_text)

