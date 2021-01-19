# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import random
import re
import requests
# import shadow_useragent

from bs4 import BeautifulSoup
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
@file_name: script.py
@author: Dylan "dyl-m" Monfret

Objective: Retrieve the number of views on YouTube to report on the audience of music through official channels. And
this every week, every month, but also by tracks, by artists and by labels. 

The tracks concerned can be found in the following Twitter thread: 
https://twitter.com/Dyl_M_DJ/status/1347261399773421570

- Summary -

1. [SOON]

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

old_week = datetime(2021, 1, 1)
lat_week = datetime(2021, 1, 10)

old_moun = datetime(2021, 1, 1)
lat_moun = datetime(2021, 1, 31)

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

    video_views = api_get_videos_views(videos, service)

    df2 = pd.DataFrame(video_views).transpose().rename({0: "idx", 1: "views"}, axis=1)

    concat = pd.merge(df1, df2, left_index=True, right_index=True)

    view_sum = concat.groupby("idx").sum()

    final = pd.merge(data_in, view_sum, left_index=True, right_index=True).rename({'views': "YouTube_Views"}, axis=1)

    return final


def get_1001tracklists_track_data(id_1001tl):
    page_link = f'https://www.1001tracklists.com/track/{id_1001tl}/'
    page_response = requests.get(page_link, headers=Headers().generate())
    soup = BeautifulSoup(page_response.content, "html.parser")

    supports = soup.find_all("span", class_='badge', title="total unique DJ supports")[0]
    tot_play = soup.find_all("td", colspan="2", text=re.compile('Total Tracklist Plays:.'))[0]

    int_supp = int(clean_html(supports).replace('x', ''))
    int_play = int(clean_html(tot_play).replace('x', '').replace('Total Tracklist Plays: ', ''))

    return int_supp, int_play


def get_1001tracklists_data(dataframe):
    dataframe["1001T_TotPlays"] = 0
    dataframe["1001T_Supports"] = 0

    for idx, row in dataframe.iterrows():
        if pd.notna(row['1001Tracklists_ID']):
            sleep(random.gauss(3, 1))
            data_1001tt = get_1001tracklists_track_data(row['1001Tracklists_ID'])
            dataframe.loc[idx, "1001T_Supports"], dataframe.loc[idx, "1001T_TotPlays"] = data_1001tt
    return dataframe


def get_data(data_frame):
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
    :return: A complete dataset with needed statistics.
    """

    all_data = get_youtube_data(data_frame)
    all_data = get_1001tracklists_data(all_data)

    all_data_by_artist = all_data.copy()
    all_data_by_label = all_data.copy()

    all_data_by_artist.Artist = all_data_by_artist.fillna("NONE").Artist.apply(lambda x: x.split(', '))
    all_data_by_label.Label = all_data_by_label.fillna("NONE").Label.apply(lambda x: x.split(', '))

    data_by_track = all_data.sort_values(["YouTube_Views", "Artist"], ascending=[False, True])

    data_by_artist = all_data_by_artist \
        .explode('Artist')[["Artist", "YouTube_Views", '1001T_Supports', '1001T_TotPlays']] \
        .groupby("Artist").sum() \
        .sort_values(["YouTube_Views", "Artist"], ascending=[False, True])

    data_by_label = all_data_by_label \
        .explode('Label')[["Label", "YouTube_Views", '1001T_Supports', '1001T_TotPlays']] \
        .groupby("Label").sum() \
        .sort_values(["YouTube_Views", "Label"], ascending=[False, True]) \
        .drop(index="NONE")

    return data_by_track, data_by_artist, data_by_label


def find_alias(dataframe):
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


def clean_html(raw_html):
    cleaner = re.compile('<.*?>')
    clean_text = re.sub(cleaner, '', str(raw_html))
    return clean_text


" - MAIN PROGRAM -"

# test = get_1001tracklist_data('22pyd7vp')
# print(test)

data_in = pd.read_excel("2021 Charts IN.xlsx")

data_in = find_alias(data_in)

data_week = data_in[(data_in.Release_Date >= old_week) & (data_in.Release_Date <= lat_week)]
# data_moun = data_in[(data_in.Release_Date >= old_moun) & (data_in.Release_Date <= lat_moun)]

all_data_out, all_data_out_artist, all_data_out_label = get_data(data_in)
week_data_out, week_data_out_artist, week_data_out_label = get_data(data_week)
# moun_data_out, moun_data_out_artist, moun_data_out_label = get_data(data_moun)

print(all_data_out)
print(all_data_out_artist)
print(all_data_out_label)

print('\n' + '/' * 100 + '\n')

print(week_data_out)
print(week_data_out_artist)
print(week_data_out_label)

all_writer = pd.ExcelWriter('2021 Charts OUT All Time.xlsx', engine='xlsxwriter')

all_data_out.to_excel(all_writer, sheet_name='By_Track', index=False)
all_data_out_artist.to_excel(all_writer, sheet_name='By_Artist')
all_data_out_label.to_excel(all_writer, sheet_name='By_Label')

all_writer.save()

week_writer = pd.ExcelWriter('Weekly_Reports/Weekly_Data/2021 Charts OUT Week 1.xlsx', engine='xlsxwriter')

week_data_out.to_excel(week_writer, sheet_name='By_Track', index=False)
week_data_out_artist.to_excel(week_writer, sheet_name='By_Artist')
week_data_out_label.to_excel(week_writer, sheet_name='By_Label')

week_writer.save()
