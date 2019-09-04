import logging
import os
import time

import boto3
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from PIL import Image, ImageFont, ImageOps, ImageDraw

import clean_pbp

def draw_centered_text(draw, left, top, width, text, color, font, vertical=False, height=None):
    """ Draws text (at least) horizontally centered in a specified width. Can also
        center vertically if specified.

    Args:
        draw: Current PIL draw Object
        left: left coordinate (x) of the bounding box
        top: top coordinate (y) of the bounding box
        width: width of the bounding box
        text: text to draw
        color: color to draw the text in
        font: ImageFont instance

        vertical: align vertically
        height: height of the box to align vertically

    Returns:
        None
    """

    # Get text size (string length & font)
    w, h = draw.textsize(text, font)
    left_new = left + ((width - w) / 2)

    if not vertical:
        coords_new = (left_new, top)
        # Draw the text with the new coordinates
        draw.text(coords_new, text, fill=color, font=font, align="center")
    else:
        _, offset_y = font.getoffset(text)
        top_new = top + ((height - h - offset_y) / 2)
        coords_new = (left_new, top_new)
        # Draw the text with the new coordinates
        draw.text(coords_new, text, fill=color, font=font, align="center")


def shotmap_image(shotmap_file, shotmap_desc, stats_string):
    """
    Resizes the shotmap image and adds title, other text & shape legends.

    :param shotmap_file: location of the shotmap file image
    :param shotmap_desc: shotmap description text (joined opts dictionary)

    :return shotmap_final: final version of the shotmap image
    """

    # Load Font Files from S3
    s3 = boto3.resource('s3')
    s3_bucket = os.environ.get('S3_BUCKET')
    bucket = s3.Bucket(s3_bucket)
    fontpath_opensans_bold = os.path.join('/tmp/', 'OpenSans-Bold.ttf')
    bucket.download_file('OpenSans-Bold.ttf', fontpath_opensans_bold)

    # Add text to our Image (title, subtitles, etc)
    # Setup Fonts, Constants & Sizing
    FONT_COLOR_BLACK = (0, 0, 0)
    # FONT_OPENSANS_BOLD = os.path.join(PROJECT_ROOT, 'resources/fonts/OpenSans-Bold.ttf')
    FONT_OPENSANS_BOLD = fontpath_opensans_bold
    # FONT_DEJAVU_SANS = os.path.join(PROJECT_ROOT, 'resources/fonts/DejaVuSans.ttf')
    TITLE_FONT = ImageFont.truetype(FONT_OPENSANS_BOLD, 28)
    SUBTITLE_FONT = ImageFont.truetype(FONT_OPENSANS_BOLD, 15)
    LEGEND_FONT = ImageFont.truetype(FONT_OPENSANS_BOLD, 12)

    # Re-Load our Saved Image, Crop & Resize
    output_img = Image.open(shotmap_file)
    w,h = output_img.size

    # Resize the Image (Width = 1024, Height = Ratio'd)
    resize_ratio = 1024 / w
    resize_wh = (int(w * resize_ratio), int(h * resize_ratio))
    resized = output_img.resize(resize_wh, resample=Image.BILINEAR)
    resized_w, resized_h = resized.size

    # Set Padding & Re-Cropping Sizes
    padding = 80
    left_crop = 60
    right_crop = resized_w + (padding * 2) - left_crop
    bottom_crop = resized_h + (padding * 1.75)

    resized = ImageOps.expand(resized, padding, (255, 255, 255)).crop((left_crop, 0, right_crop, bottom_crop))
    resized_w, resized_h = resized.size
    half_resized_w = resized_w / 2
    third_resized_w = resized_w / 3

    # Create the Draw Object & Place Text
    draw = ImageDraw.Draw(resized)

    draw_centered_text(draw, 0, 5, resized_w, shotmap_desc, FONT_COLOR_BLACK, SUBTITLE_FONT)

    draw_centered_text(draw, 0, resized_h - 62, third_resized_w, 'Chances Against', FONT_COLOR_BLACK, SUBTITLE_FONT)
    draw_centered_text(draw, 2 * third_resized_w, resized_h - 62, third_resized_w, 'Chances For', FONT_COLOR_BLACK, SUBTITLE_FONT)

    draw_centered_text(draw, 0, 75, resized_w, stats_string, FONT_COLOR_BLACK, LEGEND_FONT)

    filename = f'Rink-Shotmap-Generated-{int(time.time())}-Final.png'
    final_shotmap = os.path.join('/tmp/', filename)
    resized.save(final_shotmap)
    logging.info('Returning filename - %s', final_shotmap)
    return final_shotmap


def generate_shotmap(home_df: pd.DataFrame, away_df: pd.DataFrame):
    """ Takes two dataframes (home & away), calculates advanced stats
        and then calls a function to plot them onto the blank ring image.

    Args:
        home_df (DataFrame): the DataFrame of Home Team events
        away_df (DataFrame): the DataFrame of Away Team events

    Returns:
        completed_path: The path to the completed shotmap
    """

    # Get Home & Away team names from DF
    home_team = home_df.home_team.unique()[0]
    away_team = away_df.away_team.unique()[0]

    # Calculate Corsi before removing blocked shots
    corsi_for = len(home_df.index)
    corsi_against = len(away_df.index)
    corsi_for_percent = 100 * (corsi_for / (corsi_for + corsi_against))

    # Calculate Goals For / Against
    goals_for = len(home_df[(home_df['is_goal'] == 1)])
    goals_against = len(away_df[(away_df['is_goal'] == 1)])

    # Calculate Scoring Chances For / Against
    scf = len(home_df[(home_df['is_scoring_chance'] == 1)])
    sca = len(away_df[(away_df['is_scoring_chance'] == 1)])

    # Calculate HD Scoring Chances For / Against
    hdcf = len(home_df[(home_df['shot_danger'] > 2)])
    hdca = len(away_df[(away_df['shot_danger'] > 2)])

    # Calculate Shooting Percentage
    shots = len(home_df[(home_df['is_shot'] == 1)])
    goals = len(home_df[(home_df['is_goal'] == 1)])
    sh_percent = 100 * (goals / shots) if shots > 0 else 0

    # Calculate Average Shot Distance
    total_shot_distance = home_df['distance_togoal'].sum()
    avg_shot_distance = total_shot_distance / corsi_for

    # Calculate On Target Shots
    logging.info(f'On Target - shots: {shots}, CF: {corsi_for} ')
    on_target = 100 * (shots / corsi_for)

    # Removed Blocked Shots & (0,0) Events
    # Coordinates are wrong for Heatmaps
    logging.info('Removing blocked shots from shotmaps as coordinates are at location of block, not shot.')
    home_df = clean_pbp.df_remove_blocked_shots(home_df)
    away_df = clean_pbp.df_remove_blocked_shots(away_df)

    logging.info('Removing events at (0,0) coordinates - no idea why the exist.')
    home_df = clean_pbp.df_remove_zerozero(home_df)
    away_df = clean_pbp.df_remove_zerozero(away_df)

    stats_string = (f'CF - {corsi_for}, CA - {corsi_against}, CF% - {corsi_for_percent:.2f}% | '
                    f'GF - {goals_for}, GA - {goals_against} | SCF - {scf}, SCA - {sca} | '
                    f'HDCF - {hdcf}, HDCA - {hdca}')

    completed_path = plot_shotmap(home_df=home_df, away_df=away_df)

    # Take the completed shotmap & make the image better for tweeting.
    shotmap_credit = 'Matt Donders via NHL Shotmaps (@shotmaps)'
    description = "Sample Description Would Go Here"
    shotmap_title = f'{home_team} vs. {away_team} Shotmap \n{description}\n{shotmap_credit}'
    shotmap_file_final = shotmap_image(completed_path, shotmap_title, stats_string)
    logging.info('Shotmap Final File - %s', shotmap_file_final)

    return shotmap_file_final


def get_image_from_s3(bucket: str, key: str) -> Image:
    """ Gets the base image from S3 bucket.

    Args:
        bucket: S3 Bucket name
        key: key (filename) in the corresponding S3 bucket

    Returns:
        Image: a PIL.Image instance
    """

    temp_file_path = os.path.join('/tmp/', key)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.download_file(key, temp_file_path)
    img = Image.open(temp_file_path)

    return img


def plot_shotmap(home_df: pd.DataFrame, away_df: pd.DataFrame):
    """ Takes two dataframes (home & away) and plots them onto the blank rink image.

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
    sns.kdeplot(home_df.xc, home_df.yc, cmap='Reds', shade=True, bw=0.2, cut=100, shade_lowest=False, alpha=0.9, ax=ax)
    sns.kdeplot(away_df.xc, away_df.yc, cmap="Blues", shade=True, bw=0.2, cut=100, shade_lowest=False, alpha=0.9, ax=ax)

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