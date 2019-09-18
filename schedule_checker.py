import logging
import sys
from datetime import datetime

import dateutil.parser
import requests
from crontab import CronTab
from dateutil import tz

# Setup basic logging functionality
logging.basicConfig(
    level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S", format="%(asctime)s - %(module)s - %(levelname)s - %(message)s"
)

# Set UTZ & Local Timezones
from_zone = tz.tzutc()
to_zone = tz.tzlocal()

# Get a instance of the current user crontab
cron = CronTab(user=True)

# Remove all old lambda trigger functions
logging.info("Removing all old Lambda trigger schedules.")
cron.remove_all(comment="Lambda Shotmap Trigger")


def is_game_today():
    today = datetime.now()
    # url = f"https://statsapi.web.nhl.com/api/v1/schedule?date=2019-09-16&expand=schedule.linescore"
    url = f"https://statsapi.web.nhl.com/api/v1/schedule?date={today:%Y-%m-%d}&expand=schedule.linescore"

    schedule = requests.get(url).json()
    total_games = schedule["dates"][0]["totalGames"]

    if total_games == 0:
        return False, None

    games = schedule["dates"][0]["games"]
    return True, games


if __name__ == "__main__":
    game_today, games = is_game_today()
    if not game_today:
        logging.info("No games scheduled today - nothing to setup via cron. Exiting now.")
        sys.exit()

    for game in games:
        game_id = game["gamePk"]
        game_date = game["gameDate"]
        game_date_parsed = dateutil.parser.parse(game_date)

        home_team = game["teams"]["home"]["team"]["name"]
        away_team = game["teams"]["away"]["team"]["name"]

        # Convert to local time zone
        game_date_local = game_date_parsed.astimezone(to_zone)
        game_date_local_str = datetime.strftime(game_date_local, "%I:%M %p")
        minute = game_date_local.minute
        hour = game_date_local.hour

        # Generate crontab object
        logging.info("Creating crontab object for %s vs. %s (%s) @ %s", home_team, away_team, game_id, game_date_local_str)
        cmd = f"python lambda_trigger.py --gameId={game_id}"
        job = cron.new(command=cmd, comment="Lambda Shotmap Trigger")
        job.minute.on(minute)
        job.hour.on(hour)
        # cron = f"{hour} {minute} * * * python lambda_trigger.py --gameId={game_id}"
        # print(cron)

    print(cron)
