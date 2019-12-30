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

        msg = sns_event.get("Message")
        msg_json = json.loads(msg)
        game_id = msg_json.get("gamePk")

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


def check_db_for_last_period(game_id):
    dynamo_client = boto3_client("dynamodb")
    response = dynamo_client.get_item(
        TableName='nhl-shotmaps-tracking',
        Key={'gamePk': {'N': game_id}}
    )

    try:
        item = response['Item']
        last_period_processed = int(item['lastPeriodProcessed']['N'])
    except KeyError:
        logging.info("NEW Game Detected - record does not exist yet.")
        last_period_processed = 0

    return last_period_processed


def is_event_period_newer(game_id, event_period):
    db_last_period = check_db_for_last_period(game_id)
    return bool(event_period > db_last_period)


def lambda_handler(event, context):
    LAMBDA_GENERATOR = os.environ.get("LAMBDA_GENERATOR")
    IS_SNS_TRIGGER = bool(event.get("Records"))

    game_id_dict = get_game_id(event)
    game_id_status = game_id_dict["status"]

    if IS_SNS_TRIGGER:
        msg = event['Records'][0]['Sns']['Message']

        # Convert Message back to JSON Object
        msg = json.loads(msg)

        game_id = msg['gamePk']
        period = msg['play']['about']['period']
        goals = msg['play']['about']['goals']
        home_score = goals['home']
        away_score = goals['away']
    else:
        # Get scores directly from event payload
        home_score = event.get("home_score")
        away_score = event.get("away_score")

    if not game_id_status:
        logging.error(game_id_dict["msg"])
        return {"status": False, "msg": game_id_dict["msg"]}

    # If status is True, set the game_id variable
    game_id = game_id_dict["game_id"]

    # Check that the event period is greater than the last processed period
    is_event_newer = is_event_period_newer(game_id, period)
    if not is_event_newer:
        logging.error("The event received for %s is not newer than the last "
                      "event recorded in the database - skip this record.", game_id)

        return {
            'status': 409,
            'body': 'A shotmap was already produced for this event.'
        }

    # If all of the above checks pass, scrape the game.
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
    payload = {"pbp_json": pbp_json, "game_id": game_id, "testing": TESTING, "home_score": home_score, "away_score": away_score}
    small_payload = {"game_id": game_id, "testing": TESTING, "home_score": home_score, "away_score": away_score}

    logging.info("Scraping completed. Triggering the generator & twitter Lambda.")

    invoke_response = lambda_client.invoke(
        FunctionName=LAMBDA_GENERATOR, InvocationType="Event", Payload=json.dumps(payload)
    )

    print(invoke_response)

    return {
        'body': small_payload
    }
