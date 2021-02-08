# -*- coding: utf-8 -*-

import data_collection as dc
import pandas as pd
import report_writer as rw

from datetime import datetime

""" - SCRIPT INFORMATION - """

"""
@file_name: exe.py
@author: Dylan "dyl-m" Monfret

Do the whole execution process.
"""

week_sta_str = "2021-01-18"
week_end_str = "2021-01-24"

week_sta_dt = datetime.strptime(week_sta_str, '%Y-%m-%d')
week_end_dt = datetime.strptime(week_end_str, '%Y-%m-%d')

m_num = 1
w_num = 3

data_in = dc.find_alias(pd.read_excel("2021 Charts IN.xlsx"))

my_export = dc.export(data_frame=data_in,
                      month_number=m_num,
                      week_day_start=week_sta_str,
                      week_day_end=week_end_str,
                      week_number=w_num)

print(my_export)

rw.make_report()

