# -*- coding: utf-8 -*-
"""
Created on Thu May 29 19:57:00 2025

@author: Anthony
"""

import pandas as pd
import pyodbc

try:
    
    conn = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};'
            'SERVER=DESKTOP-ANTHONY\SQLEXPRESS01;'
            'DATABASE=netflix;'
            'Encrypt=yes;TrustServerCertificate=yes;'
            'UID=netflixproject;'
            'PWD=netflixproject'
    )
    print("OK")
except Exception:
    print('Conexión Fallida')
    
init = "netflix_titles.csv"

df = pd.read_csv(init)

print(df)