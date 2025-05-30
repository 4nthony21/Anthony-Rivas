# -*- coding: utf-8 -*-
"""
Created on Thu May 29 19:57:00 2025

@author: Anthony
"""

import pandas as pd
import pyodbc

conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=your_server;'
    'DATABASE=your_database;'
    'UID=your_username;'
    'PWD=your_password'
)

init = "netflix_titles.csv"

df = pd.read_csv(init)

print(df)