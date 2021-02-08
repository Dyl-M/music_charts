# -*- coding: utf-8 -*-

import pandas as pd

# pd.set_option('display.max_columns', None)

"""
@file_name: report_writer.py
@author: Dylan "dyl-m" Monfret

Objective: Automatically write report notes for future posts on Twitter.

- Summary -

1. Retrieve information from Excel files
2. Iterate on it to get the TOP 3.
3. Write the results in .txt format.

"""

""" - PREPARATORY ELEMENTS - """

xls1 = pd.ExcelFile('2021 Charts OUT All Time.xlsx')
xls2 = pd.ExcelFile('Weekly_Reports/Weekly_Data/2021 Charts Week 3.xlsx')
# xls3 = pd.ExcelFile('Monthly_Reports/Monthly_Data/2021 Charts Month 1.xlsx')

at_lst = [{"df": pd.read_excel(xls1, 'By_Track_YouTube'), 'stat': ["YouTube_Views"]},
          {"df": pd.read_excel(xls1, 'By_Track_1001Tracklists'), 'stat': ['1001T_Supports', '1001T_TotPlays']},
          {"df": pd.read_excel(xls1, 'By_Track_Soundcloud'), 'stat': ['Soundcloud_Plays']},
          {"df": pd.read_excel(xls1, 'By_Artist_YouTube'), 'stat': ["YouTube_Views"]},
          {"df": pd.read_excel(xls1, 'By_Artist_1001Tracklists'), 'stat': ['1001T_Supports', '1001T_TotPlays']},
          {"df": pd.read_excel(xls1, 'By_Artist_Soundcloud'), 'stat': ['Soundcloud_Plays']},
          {"df": pd.read_excel(xls1, 'By_Label_YouTube'), 'stat': ["YouTube_Views"]},
          {"df": pd.read_excel(xls1, 'By_Label_1001Tracklists'), 'stat': ['1001T_Supports', '1001T_TotPlays']},
          {"df": pd.read_excel(xls1, 'By_Label_Soundcloud'), 'stat': ['Soundcloud_Plays']}, ]

we_lst = [{"df": pd.read_excel(xls2, 'By_Track_YouTube'), 'stat': ["YouTube_Views"]},
          {"df": pd.read_excel(xls2, 'By_Track_1001Tracklists'), 'stat': ['1001T_Supports', '1001T_TotPlays']},
          {"df": pd.read_excel(xls2, 'By_Track_Soundcloud'), 'stat': ['Soundcloud_Plays']},
          {"df": pd.read_excel(xls2, 'By_Artist_YouTube'), 'stat': ["YouTube_Views"]},
          {"df": pd.read_excel(xls2, 'By_Artist_1001Tracklists'), 'stat': ['1001T_Supports', '1001T_TotPlays']},
          {"df": pd.read_excel(xls2, 'By_Artist_Soundcloud'), 'stat': ['Soundcloud_Plays']},
          {"df": pd.read_excel(xls2, 'By_Label_YouTube'), 'stat': ["YouTube_Views"]},
          {"df": pd.read_excel(xls2, 'By_Label_1001Tracklists'), 'stat': ['1001T_Supports', '1001T_TotPlays']},
          {"df": pd.read_excel(xls2, 'By_Label_Soundcloud'), 'stat': ['Soundcloud_Plays']}, ]

# mo_lst = [{"df": pd.read_excel(xls3, 'By_Track_YouTube'), 'stat': ["YouTube_Views"]},
#           {"df": pd.read_excel(xls3, 'By_Track_1001Tracklists'), 'stat': ['1001T_TotPlays', '1001T_Supports']},
#           {"df": pd.read_excel(xls3, 'By_Artist_YouTube'), 'stat': ["YouTube_Views"]},
#           {"df": pd.read_excel(xls3, 'By_Artist_1001Tracklists'), 'stat': ['1001T_TotPlays', '1001T_Supports']},
#           {"df": pd.read_excel(xls3, 'By_Label_YouTube'), 'stat': ["YouTube_Views"]},
#           {"df": pd.read_excel(xls3, 'By_Label_1001Tracklists'), 'stat': ['1001T_TotPlays', '1001T_Supports']}]

""" - LOCAL FUNCTIONS - """


def build_list_we(lst1, lst2):
    return_lst = list()
    for e1, e2 in zip(lst1, lst2):
        return_lst.append(e1)
        return_lst.append(e2)
    return return_lst


def write_report(a_list):
    string = ""
    cpt = 0
    idx_df = 0
    lab_df = ["TRACKS", "ARTISTS", "LABELS"]

    for df in a_list:

        if cpt % 6 == 0:
            string += f"--- {lab_df[idx_df]} ---\n\n"
            idx_df += 1
        if cpt % 2 == 0:
            string += f"/// WEEK ///\n\n"

        else:
            string += f"/// ALL TIME ///\n\n"

        cpt += 1

        if len(df["stat"]) == 1:
            grb = int(
                df["df"].groupby(df["stat"]).count().sort_values(by=df["stat"], ascending=False).head(3).sum().mean())

        else:
            grb = int(
                df["df"].groupby(df["stat"]).count().sort_values(by=df["stat"], ascending=[False, False]).head(
                    3).sum().mean())

        string += df["df"].head(grb).to_string() + '\n' * 2 + '/' * 115 + '\n' * 2

    return string


def make_report():
    merged_list = build_list_we(we_lst, at_lst)

    report = write_report(merged_list)
    print(report)

    with open("Weekly_Reports/Weekly_Notes/W3_Notes.txt.txt", "w", encoding='utf8') as text_file:
        text_file.write(report)


" - MAIN PART -"
