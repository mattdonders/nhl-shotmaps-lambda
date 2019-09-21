import argparse
import json
import logging
import os
from datetime import datetime

from boto3 import client as boto3_client
import hockey_scraper

lambda_client = boto3_client("lambda")

logger = logging.getLogger()
logger.setLevel(logging.DEBUG) if os.environ.get("LOGLEVEL") == "DEBUG" else logger.setLevel(logging.INFO)

# Test-based global variables
TESTING = False
TEST_TOPICS = [
    "arn:aws:sns:us-east-1:024303108096:POC-NHLP3-Play-by-Play-Events",
    "arn:aws:sns:us-east-1:627812672245:NHLP3-Play-by-Play-Events",
]



def get_game_id(event: dict):
    """ Takes the event & tries to determine & validate the Game ID.

    Args:
        event: event passed into the AWS Lambda

    Returns:
        dict: {status, game_id, msg}
    """

    global TESTING

    # Get the Game ID from the event parameter passed into the handler or via environment variable
    print(event)

    game_id = event.get("game_id")
    game_id = game_id if game_id is not None else os.environ.get("GAMEID")

    # If we are using the lambda trigger function, get the testing variable
    trigger_testing = event.get("testing")
    TESTING = trigger_testing if trigger_testing is not None else TESTING

    if not game_id and event.get("Records") is not None:
        logging.info(
            "Game ID is still none - this is an SNS message & the gameId is contained in the message field."
        )
        sns_record = event.get("Records")
        sns_record = sns_record[0]
        sns_event = sns_record.get("Sns")
        topic = sns_event.get("TopicArn")

        # Set TESTING global based on topic name
        TESTING = False if topic in TEST_TOPICS else True

        msg = sns_event.get("Message")
        msg_json = json.loads(msg)
        game_id = msg_json.get("game_id")

    if not game_id or game_id is None:
        logging.error("An NHL Game ID is required for this script to run.")
        return {"status": False, "msg": "An NHL Game ID is required for this script to run."}

    # Validate Game ID meets all criteria to actually run this script
    # In-Season game validation will happen at the API endpoint
    game_id = str(game_id)
    season = game_id[0:4]
    game_type = game_id[4:6]
    game_number = game_id[6:10]

    if int(season) > datetime.now().year:
        return {"status": False, "msg": "Invalid season detected in the specified Game ID."}

    if int(game_type) > 4:
        return {"status": False, "msg": "Invalid game type detected in the specified Game ID."}

    if int(game_number) > 1271:
        return {"status": False, "msg": "Invalid game number detected in the specified Game ID."}

    # If all validations pass, return our game_id
    return {"status": True, "game_id": game_id}


def lambda_handler(event, context):
    LAMBDA_GENERATOR = os.environ.get("LAMBDA_GENERATOR")
    game_id_dict = get_game_id(event)
    game_id_status = game_id_dict["status"]

    if not game_id_status:
        logging.error(game_id_dict["msg"])
        return {"status": False, "msg": game_id_dict["msg"]}

    # If status is True, set the game_id variable
    game_id = game_id_dict["game_id"]

    scraped_data = hockey_scraper.scrape_games([game_id], False, data_format="Pandas")
    pbp = scraped_data.get("pbp")

    # fmt: off
    cols_to_drop = ['awayPlayer1', 'awayPlayer1_id', 'awayPlayer2', 'awayPlayer2_id', 'awayPlayer3',
            'awayPlayer3_id', 'awayPlayer4', 'awayPlayer4_id', 'awayPlayer5', 'awayPlayer5_id', 'awayPlayer6',
            'awayPlayer6_id', 'homePlayer1', 'homePlayer1_id', 'homePlayer2', 'homePlayer2_id', 'homePlayer3',
            'homePlayer3_id', 'homePlayer4', 'homePlayer4_id', 'homePlayer5', 'homePlayer5_id', 'homePlayer6',
            'homePlayer6_id', 'Description', 'Home_Coach', 'Away_Coach']
    # fmt: on

    pbp = pbp.drop(cols_to_drop, axis=1)
    pbp.columns = map(str.lower, pbp.columns)

    pbp_json = pbp.to_json()
    payload = {"pbp_json": pbp_json, "testing": TESTING}

    logging.info("Scraping completed. Triggering the generator & twitter Lambda.")

    invoke_response = lambda_client.invoke(
        FunctionName=LAMBDA_GENERATOR, InvocationType="Event", Payload=json.dumps(payload)
    )

    print(invoke_response)
