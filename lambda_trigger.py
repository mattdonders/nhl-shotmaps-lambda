import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

import boto3
import dateutil.parser
import requests
import yaml
from crontab import CronTab
from dateutil import tz

SLEEP_TIME = 60

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config.yml")
LOGS_PATH = os.path.join(PROJECT_ROOT, "logs")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gameid", help="the nhl game id to check", action="store", required=True)
    arguments = parser.parse_args()
    return arguments


def get_livefeed(game_id):
    url = f"https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live"
    logging.info("Getting the latest livefeed payload from URL : %s", url)
    r = requests.get(url).json()
    return r


def get_intermission_info(livefeed):
    logging.info("Getting the intermission status from the last livefeed response.")
    linescore = livefeed["liveData"]["linescore"]
    intermission_info = linescore["intermissionInfo"]
    is_intermission = intermission_info["inIntermission"]

    return is_intermission, intermission_info


def trigger_lambda(game_id, lambda_arn, home_score, away_score):
    logging.info("Triggering the AWS Shotmaps Lambda now!")
    lambda_client = boto3.client("lambda")
    payload = {"game_id": game_id, "testing": False, "home_score": home_score, "away_score": away_score}
    invoke_response = lambda_client.invoke(
        FunctionName=lambda_arn, InvocationType="RequestResponse", Payload=json.dumps(payload)
    )

    return invoke_response


if __name__ == "__main__":
    args = parse_arguments()
    game_id = args.gameid

    # Load Configuration File
    with open(CONFIG_PATH) as ymlfile:
        config = yaml.load(ymlfile, Loader=yaml.FullLoader)

    LAMBDA_ARN = config["script"]["aws_lambda_arn"]

    # Setup basic logging functionality
    log_file_name = datetime.now().strftime(config["script"]["trigger_log_file"] + "-" + game_id + ".log")
    log_file = os.path.join(LOGS_PATH, log_file_name)
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s - %(module)s - %(levelname)s - %(message)s",
    )

    # Loop over this until the game ends and we use sys.exit to complete.
    while True:
        try:
            # Get the livefeed & intermission information
            livefeed = get_livefeed(game_id)

            # Check game status
            game_state = livefeed["gameData"]["status"]["abstractGameState"]

            if game_state == "Final":
                logging.info("Game is now final - send one final (end of game) shotmap & exit.")
                lambda_response = trigger_lambda(game_id=game_id, lambda_arn=LAMBDA_ARN)
                logging.info(lambda_response)
                sys.exit()

            if game_state == "Preview":
                logging.info("Game is in Preview - sleep for designated game time before looping.")
                time.sleep(SLEEP_TIME)
                continue

            # Get required attributes for lambda trigger
            linescore = livefeed["liveData"]["linescore"]
            home_score = linescore["teams"]["home"]["goals"]
            away_score = linescore["teams"]["away"]["goals"]

            period = livefeed["liveData"]["linescore"]["currentPeriod"]
            period_ordinal = livefeed["liveData"]["linescore"]["currentPeriodOrdinal"]
            period_remain = livefeed["liveData"]["linescore"]["currentPeriodTimeRemaining"]

            is_intermission, intermission_info = get_intermission_info(livefeed)
            if is_intermission:
                lambda_response = trigger_lambda(
                    game_id=game_id, lambda_arn=LAMBDA_ARN, home_score=home_score, away_score=away_score
                )
                logging.info(lambda_response)

                logging.info(
                    "Game is currently in intermission. Add 60 seconds to intermission time to avoid a re-trigger."
                )
                sleep_time = intermission_info["intermissionTimeRemaining"] + 60
                logging.info("Sleeping for %s seconds now.", sleep_time)
                time.sleep(sleep_time)
            else:
                logging.info(
                    "Not currently in intermission - %s remaining in the %s period.",
                    period_remain,
                    period_ordinal,
                )

                logging.info("-" * 60)
                time.sleep(SLEEP_TIME)
        except Exception as e:
            logging.warning("Ran into an exception during this loop iteration - sleep & try again.")
            logging.warning(e)
            time.sleep(SLEEP_TIME)
