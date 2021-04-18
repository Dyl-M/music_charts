# -*- coding: utf-8 -*-

import pandas as pd

"""
@file_name: report_writer.py
@author: Dylan "dyl-m" Monfret

Objective: Automatically write report notes for future posts on Twitter.

- Summary -

1. Retrieve information from Excel files
2. Iterate on it to get the TOP 3.
3. Write the results in .txt format.

"""

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


def make_report(source_alltime, source_week, week_num):
    at_xlsx = pd.ExcelFile(source_alltime)
    we_xlsx = pd.ExcelFile(source_week)

    at_lst = build_iterators(at_xlsx)
    we_lst = build_iterators(we_xlsx)

    merged_list = build_list_we(we_lst, at_lst)

    report = write_report(merged_list)
    print(report)

    with open(f"../weekly_reports/weekly_notes/W{week_num}_Notes.txt", "w", encoding='utf8') as text_file:
        text_file.write(report)


def build_iterators(xlsx_source):
    prefix = ['By_Track_', 'By_Artist_', 'By_Label_']
    suffix = [{'plat': 'YouTube', 'stats': ["YouTube_Views"]},
              {'plat': '1001Tracklists', 'stats': ['1001T_Supports', '1001T_TotPlays']},
              {'plat': 'Soundcloud', 'stats': ['Soundcloud_Plays']}]

    list_from_source = list()

    for pre in prefix:
        for suf in suffix:
            list_from_source.append({"df": pd.read_excel(xlsx_source, f'{pre}{suf["plat"]}'), 'stat': suf["stats"]})

    return list_from_source


" - MAIN PART -"

if __name__ == '__main__':
    w_num = 4
    report_source_alltime = pd.ExcelFile('../files/2021 Charts OUT All Time.xlsx')
    report_source_week = pd.ExcelFile(f'../weekly_reports/weekly_data/2021 Charts Week {w_num}.xlsx')

    make_report(report_source_alltime, report_source_week, w_num)
