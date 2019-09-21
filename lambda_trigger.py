import argparse
import json
import logging
import sys
import time
from datetime import datetime

import boto3
import dateutil.parser
import requests
from crontab import CronTab
from dateutil import tz

SLEEP_TIME = 60
LAMBDA_ARN = ""

# Setup basic logging functionality
logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s - %(module)s - %(levelname)s - %(message)s",
)


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


def trigger_lambda(game_id):
    logging.info("Triggering the AWS Shotmaps Lambda now!")
    lambda_client = boto3.client("lambda")
    payload = {"game_id": game_id, "testing": False}
    invoke_response = lambda_client.invoke(
        FunctionName=LAMBDA_ARN, InvocationType="RequestResponse", Payload=json.dumps(payload)
    )

    return invoke_response


if __name__ == "__main__":
    args = parse_arguments()
    game_id = args.gameid

    # Loop over this until the game ends and we use sys.exit to complete.
    while True:
        # Get the livefeed & intermission information
        livefeed = get_livefeed(game_id)

        # Check game status
        game_state = livefeed["gameData"]["status"]["abstractGameState"]

        if game_state == "Final":
            logging.info("Game is now final - send one final (end of game) shotmap & exit.")
            lambda_response = trigger_lambda(game_id)
            logging.info(lambda_response)
            sys.exit()

        period = livefeed["liveData"]["linescore"]["currentPeriod"]
        period_ordinal = livefeed["liveData"]["linescore"]["currentPeriodOrdinal"]
        period_remain = livefeed["liveData"]["linescore"]["currentPeriodTimeRemaining"]

        is_intermission, intermission_info = get_intermission_info(livefeed)
        if is_intermission:
            lambda_response = trigger_lambda(game_id)
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
