# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

from datetime import datetime
from Google import Create_Service

# import json

# import os
# import webbrowser


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

""" - LOCAL FUNCTIONS - """


def api_get_videos_views(list_videos_ids, a_service):
    """
    A function to get views of 50 videos at once.

    :param a_service:
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
    client_secret_file = 'code_secret_client.json'
    api_name = 'YouTube'
    api_version = 'v3'
    scopes = ['https://www.googleapis.com/auth/youtube']

    service = Create_Service(client_secret_file, api_name, api_version, scopes)

    df = data_frame.fillna("NONE")
    youtube_dict = dict()

    for idx, row in df.iterrows():
        youtube_dict[row["YouTube_ID1"]] = [idx]
        youtube_dict[row["YouTube_ID2"]] = [idx]
        youtube_dict[row["YouTube_ID3"]] = [idx]
        youtube_dict[row["YouTube_ID4"]] = [idx]

    df1 = pd.DataFrame(youtube_dict).transpose().rename({0: "idx"}, axis=1).drop(index="NONE")
    videos = list(df1.index)

    video_views = api_get_videos_views(videos, service)

    df2 = pd.DataFrame(video_views).transpose().rename({0: "idx", 1: "views"}, axis=1)

    concat = pd.merge(df1, df2, left_index=True, right_index=True)

    view_sum = concat.groupby("idx").sum()

    final = pd.merge(data_in, view_sum, left_index=True, right_index=True).rename({'views': "YouTube_Views"}, axis=1)

    return final


def get_data(data_frame):
    final_df = get_youtube_data(data_frame)
    return final_df


" - MAIN PROGRAM -"

data_in = pd.read_excel("2021 Charts IN.xlsx")

my_views = get_data(data_in)
print(my_views)

# my_views.to_excel("2021 Charts OUT.xlsx")
