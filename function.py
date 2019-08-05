import os
import tempfile

import boto3
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns
import tweepy
from PIL import Image

# These are the only events we want to get coordinates for
MAPPED_EVENTS = ('SHOT', 'MISSED_SHOT', 'GOAL')

def all_plays_parser(home_team: str, away_team: str, all_plays: dict):
    """ Takes the JSON object of all game events and generates a pandas dataframe.

    Args:
        home_team: Home Team name
        away_team: Away Team Name
        all_plays: All plays JSON dictionary (allPlays) from livefeed endpoint

    Returns:
        TBD
    """

    home_events = list()
    away_events = list()

    # Loop through all plays and build a list of mapped events
    for play in all_plays:
        event_type = play['result']['eventTypeId']

        # If play is not mapped, continue (skips loop iteration)
        if event_type not in MAPPED_EVENTS:
            continue

        # Team & Period Attributes are needed for other stuff
        team = play['team']['name']
        period = play['about']['period']

        try:
            coords_x = play['coordinates']['x']
            coords_y = play['coordinates']['y']
        except KeyError:
            continue

        # Flip coordinates if 2nd period (or overtime)
        if period % 2 == 0:
            coords_x = coords_x * -1
            coords_y = coords_y * -1

        # If play is outside of the grid, skip it (unless its a Goal)
        if event_type != "GOAL" and (abs(coords_x) > 100 or abs(coords_y) > 42.5):
            continue

        # If the event is a goal, get the strength
        strength = play['result']['strength']['code'] if event_type == 'GOAL' else 'N/A'

        event = {}
        event['id'] = id
        event['period'] = period
        event['event_type'] = event_type
        event['strength'] = strength
        event['coords_x'] = coords_x
        event['coords_y'] = coords_y

        if team == home_team:
            if event['coords_x'] < 0:
                event['coords_x'] = event['coords_x'] * -1
                event['coords_y'] = event['coords_y'] * -1
            home_events.append(event)
        else:
            if event['coords_x'] > 0:
                event['coords_x'] = event['coords_x'] * -1
                event['coords_y'] = event['coords_y'] * -1
            away_events.append(event)

    # Convert lists to dataframes
    home_df = pd.DataFrame(home_events)
    away_df = pd.DataFrame(away_events)

    return home_df, away_df


def get_image_from_s3(bucket: str, key: str) -> Image:
    """ Gets the base image from S3 bucket.

    Args:
        bucket: S3 Bucket name
        key: key (filename) in the corresponding S3 bucket

    Returns:
        Image: a PIL.Image instance
    """

    temp_file_path = os.path.join('/tmp/', 'shotmap-blank-rink.png')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.download_file(key, temp_file_path)
    img = Image.open(temp_file_path)

    return img


def generate_shotmap(home_df: pd.DataFrame, away_df: pd.DataFrame):
    """ Takes two dataframes (home & away) and plots them onto the blank ring image.

    Args:
        home_df (DataFrame): the DataFrame of Home Team events
        away_df (DataFrame): the DataFrame of Away Team events

    Returns:
        completed_path: The path to the completed shotmap
    """

    # S3 Bucket & Key for the blank shotmap image
    s3_bucket = os.environ.get('S3_BUCKET')
    shotmap_blank_rink = os.environ.get('SHOTMAP_BLANK')

    MY_DPI = 96
    IMG_WIDTH = 1024
    IMG_HEIGHT = 440
    FIG_WIDTH = IMG_WIDTH / MY_DPI
    FIG_HEIGHT = IMG_HEIGHT / MY_DPI
    fig = plt.figure(figsize=(FIG_WIDTH, FIG_HEIGHT), dpi=MY_DPI)
    ax = fig.add_subplot(111)

    ax_extent = [-100, 100, -42.5, 42.5]
    img = get_image_from_s3(s3_bucket, shotmap_blank_rink)
    ax.imshow(img, extent=ax_extent)

    # Draw the heatmap portion of the graph
    sns.set_style("white")
    sns.kdeplot(home_df.coords_x, home_df.coords_y, cmap='Reds', shade=True, bw=0.2, cut=100, shade_lowest=False, alpha=0.9, ax=ax)
    sns.kdeplot(away_df.coords_x, away_df.coords_y, cmap="Blues", shade=True, bw=0.2, cut=100, shade_lowest=False, alpha=0.9, ax=ax)

    # Set X / Y Limits
    ax.set_xlim(-100, 100)
    ax.set_ylim(-42, 42)

    # Hide all axes & bounding boxes
    ax.collections[0].set_alpha(0)
    ax.axes.get_xaxis().set_visible(False)
    ax.axes.get_yaxis().set_visible(False)
    ax.set_frame_on(False)
    ax.axis('off')

    completed_path = os.path.join('/tmp', "completed-shotmap.png")
    fig.savefig(completed_path, dpi=400, bbox_inches='tight')

    return completed_path


def send_shotmap_tweet(completed_path: str, tweet_text: str):
    """ Takes a completed shotmap path & some text and sends out a tweet.
        Twitter keys are stored in environment variables.

    Args:
        completed_path: The path to the completed shotmap
        tweet_text: Any text to send alongside the string

    Returns:
        True if tweet sent or TweepyError if failed
    """

    consumer_key = os.environ.get('debug_twtr_consumer_key')
    consumer_secret = os.environ.get('debug_twtr_consumer_secret')
    access_token = os.environ.get('debug_twtr_access_token')
    access_secret = os.environ.get('debug_twtr_access_secret')
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    try:
        if api.update_with_media(completed_path, tweet_text):
            return True
    except tweepy.error.TweepError as e:
        return e


def lambda_handler(event, context):
    # Until we have a trigger for this function, just go get the live feed of a particular game.
    game_id = os.environ.get('GAMEID')
    feed = requests.get(f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live').json()
    home_team = feed['gameData']['teams']['home']['name']
    away_team = feed['gameData']['teams']['away']['name']
    all_plays = feed['liveData']['plays']['allPlays']

    home_df, away_df = all_plays_parser(home_team, away_team, all_plays)

    home_df_json = home_df.to_json()
    away_df_json = away_df.to_json()

    return_dict = dict()
    return_dict['home'] = home_df_json
    return_dict['away'] = away_df_json

    # Generate the Shotmap
    completed_path = generate_shotmap(home_df=home_df, away_df=away_df)

    # Send the compelted shotmap tweet
    status = send_shotmap_tweet(completed_path=completed_path, tweet_text="Sent from our new AWS Lambda function!")
    return_dict['tweet'] = status

    return return_dict
