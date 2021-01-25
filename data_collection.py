# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import random
import re
import requests
# import shadow_useragent

from bs4 import BeautifulSoup
from calendar import monthrange
from datetime import datetime
from fake_headers import Headers
# from fake_useragent import UserAgent
from Google import Create_Service
from time import sleep
from urllib.request import Request, urlopen

pd.set_option('display.max_columns', None)

""" - CREDITS - """

"""
The code works with Google's OAuth ID 2.0 system and the "Google.py" script available at the following link :
https://learndataanalysis.org/google-py-file-source-code/
"""

""" - SCRIPT INFORMATION - """

"""
@file_name: data_collection.py
@author: Dylan "dyl-m" Monfret

Objective: Retrieve the number of views on YouTube to report on the audience of music through official channels. And
this every week, every month, but also by tracks, by artists and by labels. 

The tracks concerned can be found in the following Twitter thread: 
https://twitter.com/Dyl_M_DJ/status/1347261399773421570

- Summary -

1. Get basic informations from a dataframe.
2. Retrieve data from diverse platform (YouTube and 1001Tracklists).
3. Divide the data into different tables on different sheets for export to Excel.

"""

""" - PREPARATORY ELEMENTS - """

alias = {"PBH & Jack": "PBH & Jack Shizzle", "Daffy Muffin": "Lucas & Steve", "AREA21": ["Martin Garrix", "Maejor"],
         "Ytram": "Martin Garrix", "Major Lazer": ["Diplo", "Walshy Fire", "Ape Drums"], "Big Pineapple": "Don Diablo",
         "VIRTUAL SELF": "Porter Robinson", "Streex": "Razihel", "Jack Ü": ["Skrillex", "Diplo"],
         "Axwell Λ Ingrosso": ["Sebastian Ingrosso", "Axwell"], "Bastille": "Dan Smith", "Dan Smith": "Bastille",
         "Swedish House Mafia": ["Axwell", "Sebastian Ingrosso", "Steve Angello"], "NWYR": "W&W",
         "Shindeai": ["STARRYSKY", "Tai Wuang"], "Sasha": "STARRYSKY", "Casseurs Flowters": ["OrelSan", "Gringe"],
         "Sinnoh Fusion Ensemble": "insaneintherainmusic", "Destroid": ["Excision", "Far Too Loud"],
         "Jeffrey Sutorius": "Dash Berlin", "Jaxxwell": ["Hardwell", "Blasterjaxx"]}

weak_alias = {"AvB": "Armin van Buuren", "Rising Star": "Armin van Buuren", "NLW": "Afrojack",
              "Jayden Jaxx": "Crime Zcene", "Chill Harris": "Kill Paris", "DJ Afrojack": "Afrojack",
              "Ravitez": "Chico Rose", "GRX": "Martin Garrix", "Kerafix & Vultaire": "KEVU",
              "Lush & Simon": ["Simon Says", "Zen/It"], "Matthew Ros": "MWRS", "Grant Bowtie": "Grant",
              "M.E.G. & N.E.R.A.K.": "DJ M.E.G.", "MEG / NERAK": "DJ M.E.G.", "Dzeko & Torres": "Dzeko",
              "X-Teef": "Stemalø", "Paris & Simo": "Prince Paris", "The Eden Project": "EDEN", "Astra": "ASHWYN",
              "Slips & Slurs": "Slippy", "Vorwerk": "Maarten Vorwerk", "Will & Tim": "NewGamePlus",
              "DBSTF": "D-Block & S-te-Fan", "Maître Gims": "GIMS", "Muzzy": "MUZZ", "Richard Caddock": "Keepsake",
              "Joey Rumble": "Modern Revolt", "Michelle McKenna": "Michelle Platnum", "Ben Lepper": "Cloud Cage",
              "Juventa": "Jordin Post"}

""" - LOCAL FUNCTIONS - """


def api_get_videos_views(list_videos_ids, a_service):
    """
    A function to get views of 50 videos at once.

    :param a_service: Access to Google API
    :param list_videos_ids: A list of video IDs, maximum size 50.
    :return: a dictionary associating video id and views of said video.
    """

    views = list()
    chunks50 = [list(sub_list) for sub_list in
                np.array_split(np.array(list_videos_ids), len(list_videos_ids) // 50 + 1)]

    for chunk in chunks50:
        request = a_service.videos().list(id=",".join(chunk), part='statistics', maxResults=50).execute()
        views += [element['statistics']['viewCount'] for element in request["items"]]

    id_and_duration = {video_id: {'views': int(views)} for video_id, views in zip(list_videos_ids, views)}

    return id_and_duration


def clean_html(raw_html):
    """
    A function to remove HTLM tags.

    :param raw_html: Unprocessed HTML code.
    :return: Processed HTML code.
    """

    cleaner = re.compile('<.*?>')
    clean_text = re.sub(cleaner, '', str(raw_html))

    return clean_text


def data_by_artist(dataframe):
    """
    A function that groups and sums statistics by artist.

    :param dataframe: A reference dataframe.
    :return: Result of the groupby.
    """

    dataframe.Artist = dataframe.fillna('NONE').Artist.apply(lambda x: x.split(', '))
    dataframe = dataframe.explode('Artist').groupby('Artist').sum()

    return dataframe


def data_by_label(dataframe):
    """
    A function that groups and sums statistics by label.

    :param dataframe: A reference dataframe.
    :return: Result of the groupby.
    """

    dataframe.Label = dataframe.fillna('NONE').Label.apply(lambda x: x.split(', '))
    dataframe = dataframe.explode('Label').groupby('Label').sum()
    dataframe = dataframe.drop('NONE')

    return dataframe


def data_sorted_1001trl(df_track, df_artist, df_label):
    """
    A function to select and sort 1001Tracklists.com stats.

    :param df_track: A dataframe of tracks.
    :param df_artist: A dataframe of artists.
    :param df_label: A dataframe of labels.
    :return: Dataframes sorted.
    """

    df_track = df_track[["Artist", 'Track_Name', 'Label', '1001T_TotPlays', '1001T_Supports']] \
        .sort_values(['1001T_TotPlays', '1001T_Supports', "Track_Name"], ascending=[False, False, True])

    df_artist = df_artist[['1001T_TotPlays', '1001T_Supports']] \
        .sort_values(['1001T_TotPlays', '1001T_Supports', "Artist"], ascending=[False, False, True])

    df_label = df_label[['1001T_TotPlays', '1001T_Supports']] \
        .sort_values(['1001T_TotPlays', '1001T_Supports', "Label"], ascending=[False, False, True])

    return df_track, df_artist, df_label


def data_sorted_youtube(df_track, df_artist, df_label):
    """
    A function to select and sort YouTube stats.

    :param df_track: A dataframe of tracks.
    :param df_artist: A dataframe of artists.
    :param df_label: A dataframe of labels.
    :return: Dataframes sorted.
    """

    df_track = df_track[["Artist", 'Track_Name', 'Label', "YouTube_Views"]] \
        .sort_values(["YouTube_Views", "Track_Name"], ascending=[False, True])

    df_artist = df_artist[["YouTube_Views"]].sort_values(["YouTube_Views", "Artist"], ascending=[False, True])

    df_label = df_label[["YouTube_Views"]].sort_values(["YouTube_Views", "Label"], ascending=[False, True])

    return df_track, df_artist, df_label


def export(data_frame, month_number, week_day_start, week_day_end, week_number):
    """
    A function to export statistics.

    :param data_frame: From function 'get_data'.
    :param month_number: From function 'get_data'.
    :param week_day_start: From function 'get_data'.
    :param week_day_end: From function 'get_data'.
    :param week_number: Indicates the number of the week to be analyzed.
    :return: A complete dataset with needed statistics.
    """

    weelky_folder = "Weekly_Reports/Weekly_Data/"
    montly_folder = "Monthly_Reports/Monthly_Data/"

    alltime_by_track, weeek_by_track, month_by_track = get_data(data_frame, month_number, week_day_start, week_day_end)

    alltime_by_artist = data_by_artist(alltime_by_track)
    alltime_by_label = data_by_label(alltime_by_track)

    week_by_artist = data_by_artist(weeek_by_track)
    week_by_label = data_by_label(weeek_by_track)

    month_by_artist = data_by_artist(month_by_track)
    month_by_label = data_by_label(month_by_track)

    alltime_by_track.Artist = alltime_by_track.Artist.apply(lambda x: ', '.join(x))
    weeek_by_track.Artist = weeek_by_track.Artist.apply(lambda x: ', '.join(x))
    month_by_track.Artist = month_by_track.Artist.apply(lambda x: ', '.join(x))

    alltime_by_track.Label = alltime_by_track.Label.apply(lambda x: ', '.join(x))
    weeek_by_track.Label = weeek_by_track.Label.apply(lambda x: ', '.join(x))
    month_by_track.Label = month_by_track.Label.apply(lambda x: ', '.join(x))

    export_alltime_part(alltime_by_track, alltime_by_artist, alltime_by_label)
    export_weekly_part(weeek_by_track, week_by_artist, week_by_label, weelky_folder, week_number)
    export_monthly_part(month_by_track, month_by_artist, month_by_label, montly_folder, month_number)

    return 'Data correctly exported :)'


def export_alltime_part(df_by_track, df_by_artist, df_by_label):
    """
    Part of the function 'export', processing data from 'alltime' dataframes.

    :param df_by_track: All time DF by track.
    :param df_by_artist: All time DF by artist.
    :param df_by_label: All time DF by label.
    """

    alltime_track_youtube, alltime_artist_youtube, alltime_label_youtube = data_sorted_youtube(df_by_track,
                                                                                               df_by_artist,
                                                                                               df_by_label)

    alltime_track_1001trl, alltime_artist_1001trl, alltime_label_1001trl = data_sorted_1001trl(df_by_track,
                                                                                               df_by_artist,
                                                                                               df_by_label)

    writer = pd.ExcelWriter('2021 Charts OUT All Time.xlsx', engine='xlsxwriter')

    alltime_track_youtube.to_excel(writer, sheet_name='By_Track_YouTube', index=False)
    alltime_track_1001trl.to_excel(writer, sheet_name='By_Track_1001Tracklists', index=False)
    alltime_artist_youtube.to_excel(writer, sheet_name='By_Artist_YouTube')
    alltime_artist_1001trl.to_excel(writer, sheet_name='By_Artist_1001Tracklists')
    alltime_label_youtube.to_excel(writer, sheet_name='By_Label_YouTube')
    alltime_label_1001trl.to_excel(writer, sheet_name='By_Label_1001Tracklists')

    writer.save()


def export_monthly_part(df_by_track, df_by_artist, df_by_label, folder, month_number):
    """
    Part of the function 'export', processing data from monthly dataframes.

    :param df_by_track: Monthly DF by track.
    :param df_by_artist: Monthly DF by artist.
    :param df_by_label: Monthly DF by label.
    :param folder: Destination folder for export.
    :param month_number: Number of the month being analyzed.
    """

    month_track_youtube, month_artist_youtube, month_label_youtube = data_sorted_youtube(df_by_track,
                                                                                         df_by_artist,
                                                                                         df_by_label)

    month_track_1001trl, month_artist_1001trl, month_label_1001trl = data_sorted_1001trl(df_by_track,
                                                                                         df_by_artist,
                                                                                         df_by_label)

    writer = pd.ExcelWriter(f'{folder}2021 Charts Month {month_number}.xlsx', engine='xlsxwriter')

    month_track_youtube.to_excel(writer, sheet_name='By_Track_YouTube', index=False)
    month_track_1001trl.to_excel(writer, sheet_name='By_Track_1001Tracklists', index=False)
    month_artist_youtube.to_excel(writer, sheet_name='By_Artist_YouTube')
    month_artist_1001trl.to_excel(writer, sheet_name='By_Artist_1001Tracklists')
    month_label_youtube.to_excel(writer, sheet_name='By_Label_YouTube')
    month_label_1001trl.to_excel(writer, sheet_name='By_Label_1001Tracklists')

    writer.save()


def export_weekly_part(df_by_track, df_by_artist, df_by_label, folder, week_number):
    """
    Part of the function 'export', processing data from weekly dataframes.

    :param df_by_track: Weekly DF by track.
    :param df_by_artist: Weekly DF by artist.
    :param df_by_label: Weekly DF by label.
    :param folder: Destination folder for export.
    :param week_number: Number of the week being analyzed.
    """

    week_track_youtube, week_artist_youtube, week_label_youtube = data_sorted_youtube(df_by_track,
                                                                                      df_by_artist,
                                                                                      df_by_label)

    week_track_1001trl, week_artist_1001trl, week_label_1001trl = data_sorted_1001trl(df_by_track,
                                                                                      df_by_artist,
                                                                                      df_by_label)

    writer = pd.ExcelWriter(f'{folder}2021 Charts Week {week_number}.xlsx', engine='xlsxwriter')

    week_track_youtube.to_excel(writer, sheet_name='By_Track_YouTube', index=False)
    week_track_1001trl.to_excel(writer, sheet_name='By_Track_1001Tracklists', index=False)
    week_artist_youtube.to_excel(writer, sheet_name='By_Artist_YouTube')
    week_artist_1001trl.to_excel(writer, sheet_name='By_Artist_1001Tracklists')
    week_label_youtube.to_excel(writer, sheet_name='By_Label_YouTube')
    week_label_1001trl.to_excel(writer, sheet_name='By_Label_1001Tracklists')

    writer.save()


def find_alias(dataframe):
    """
    A function to find alis among artists' names.

    :param dataframe: A reference dataframe.
    :return: Augmented dataframe with alias.
    """

    for an_idx, a_row in dataframe.iterrows():
        artists = a_row.Artist.split(', ')
        for artist in artists:
            if artist in weak_alias.keys():
                if isinstance(weak_alias[artist], list):
                    for an_alias in weak_alias[artist]:
                        dataframe.loc[an_idx, 'Artist'] += f', {an_alias}'
                else:
                    dataframe.loc[an_idx, 'Artist'] += f', {weak_alias[artist]}'
            elif artist in alias.keys():
                if isinstance(alias[artist], list):
                    for an_alias in alias[artist]:
                        dataframe.loc[an_idx, 'Artist'] += f', {an_alias}'
                else:
                    dataframe.loc[an_idx, 'Artist'] += f', {alias[artist]}'
    return dataframe


def get_1001tracklists_data(dataframe):
    """
    A function to retrieve data from 1001Tracklists.com

    :param dataframe: A reference dataframe (with 1001Tracklists Track ID)
    :return: A dataframe with number of plays and unique DJ supports.
    """

    dataframe["1001T_TotPlays"] = 0
    dataframe["1001T_Supports"] = 0

    for idx, row in dataframe.iterrows():
        if pd.notna(row['1001Tracklists_ID']):
            sleep(random.gauss(3, 1))
            data_1001tt = get_1001tracklists_track_data(row['1001Tracklists_ID'])
            dataframe.loc[idx, "1001T_Supports"], dataframe.loc[idx, "1001T_TotPlays"] = data_1001tt
    return dataframe


def get_1001tracklists_track_data(id_1001tl):
    """
    A function to retrieve 1001Tracklists.com data with a Track ID.

    :param id_1001tl: 1001Tracklists Track ID.
    :return: a list [Unique DJ Supports, Plays]
    """
    print(id_1001tl)
    page_link = f'https://www.1001tracklists.com/track/{id_1001tl}/'
    page_response = requests.get(page_link, headers=Headers().generate())
    soup = BeautifulSoup(page_response.content, "html.parser")

    supports = soup.find_all("span", class_='badge', title="total unique DJ supports")[0]
    int_supp = int(clean_html(supports).replace('x', ''))
    # print(soup)
    try:
        tot_play = soup.find_all("td", colspan="2", text=re.compile('Total Tracklist Plays:.'))[0]
        int_play = int(clean_html(tot_play).replace('x', '').replace('Total Tracklist Plays: ', ''))
    except IndexError:
        int_play = int(0)

    return int_supp, int_play


def get_data(data_frame, month_number, week_day_start, week_day_end):
    """
    A function to get various data from music on these different platforms:
        - YouTube
        - 1001Tracklists #TODO Work In Progress - Needs a VPN for now.
        - Soundcloud #TODO Need Help!
        - Spotify #TODO Need Help!
        - Apple Music #TODO Need Help!
        - Deezer #TODO Need Help!
        - Tiktok (?) #TODO Need Help!

    :param data_frame: A dataframe listing all the information for each track (IDs of the different platforms, labels,
                       artists and release date).
    :param month_number: Number of the month to be analyzed. (int)
    :param week_day_start: First day of the week to be analyzed. (datetime)
    :param week_day_end: Last day of the week to be analyzed. (datetime)
    :return: A complete dataset with needed statistics.
    """

    month_days = monthrange(2021, month_number)[1]
    month_day_start = datetime(2021, month_number, 1)
    month_day_end = datetime(2021, month_number, month_days)

    data = get_youtube_data(data_frame)
    data = get_1001tracklists_data(data)

    data_week = data[(data.Release_Date >= week_day_start) & (data.Release_Date <= week_day_end)]
    data_month = data[(data.Release_Date >= month_day_start) & (data.Release_Date <= month_day_end)]

    return data, data_week, data_month


def get_youtube_data(data_frame):
    """
    A function to get data from YouTube (here, views for each music).

    :param data_frame: A dataframe with the YouTube video IDs associated to each music.
    :return: The same dataframe but with the total number of views for each music.
    """
    client_secret_file = 'code_secret_client.json'
    api_name = 'YouTube'
    api_version = 'v3'
    scopes = ['https://www.googleapis.com/auth/youtube']

    service = Create_Service(client_secret_file, api_name, api_version, scopes)

    df = data_frame.fillna("NONE")
    youtube_dict = dict()

    for an_idx, a_row in df.iterrows():
        youtube_dict[a_row["YouTube_ID1"]] = [an_idx]
        youtube_dict[a_row["YouTube_ID2"]] = [an_idx]
        youtube_dict[a_row["YouTube_ID3"]] = [an_idx]
        youtube_dict[a_row["YouTube_ID4"]] = [an_idx]

    df1 = pd.DataFrame(youtube_dict).transpose().rename({0: "idx"}, axis=1).drop(index="NONE")
    videos = list(df1.index)

    # print(videos)

    video_views = api_get_videos_views(videos, service)

    df2 = pd.DataFrame(video_views).transpose().rename({0: "idx", 1: "views"}, axis=1)

    concat = pd.merge(df1, df2, left_index=True, right_index=True)

    view_sum = concat.groupby("idx").sum()

    final = pd.merge(data_in, view_sum, left_index=True, right_index=True).rename({'views': "YouTube_Views"}, axis=1)

    return final


" - MAIN PROGRAM -"

week_sta_str = "2021-01-18"
week_end_str = "2021-01-24"

week_sta_dt = datetime.strptime(week_sta_str, '%Y-%m-%d')
week_end_dt = datetime.strptime(week_end_str, '%Y-%m-%d')

m_num = 1
w_num = 3

data_in = find_alias(pd.read_excel("2021 Charts IN.xlsx"))

test = export(data_frame=data_in,
              month_number=m_num,
              week_day_start=week_sta_str,
              week_day_end=week_end_str,
              week_number=w_num)

print(test)