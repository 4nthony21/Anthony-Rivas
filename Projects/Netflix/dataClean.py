# -*- coding: utf-8 -*-
"""
Created on Thu May 29 19:57:00 2025

@author: Anthony
"""

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine

try:
    
    URL = sa.engine.URL.create(
    "mssql+pyodbc",
    username="netflixproject",
    password="netflixproject",
    host="DESKTOP-ANTHONY\SQLEXPRESS01",
    database="netflix",
    query={"driver": "ODBC Driver 18 for SQL Server", "Encrypt": "no"}
    )
    
    engine = create_engine(URL)
    
    print("Conexión Exitosa\n")
except Exception:
    print('Conexión Fallida\n')
    
init = "netflix_titles.csv"

df = pd.read_csv(init)

data = {'id': [1,2],'description':['Actor','Director']}
df_positionType = pd.DataFrame(data)

df_list = df['listed_in'].str.split(',')
df_genre = df_list.explode().drop_duplicates()
df_genre = df_genre.to_frame().dropna().reset_index(drop=True).reset_index()
df_genre.columns = ['id','description']
df_genre['id'] = df_genre['id'].add(1)

df_type = df['type'].str.split(',').explode().drop_duplicates()
df_type = df_type.to_frame().dropna().reset_index(drop=True).reset_index()
df_type.columns = ['id','name']
df_type['id'] = df_type['id'].add(1)

df_dir = df['director'].str.split(',').to_frame()
df_dir['positionType'] = 'Director'
df_dir.columns = ['name','positionType']

df_cast = df['cast'].str.split(',').to_frame()
df_cast['positionType'] = 'Actor'
df_cast.columns = ['name','positionType']

df_people = pd.concat([df_dir,df_cast],axis = 0).explode('name').drop_duplicates()
df_people = df_people.dropna().reset_index(drop=True).reset_index()
df_people.columns = ['id','name','positionType']
df_people['id'] = df_people['id'].add(1)
df_people = df_people.merge(df_positionType,left_on = 'positionType',right_on = 'description')
df_people = df_people[['id_x','name','id_y']]
df_people.columns = ['id','name','positionTypeId']

df_rating = df['rating'].str.split(',').explode().drop_duplicates()
df_rating = df_rating.to_frame().dropna()
df_rating = df_rating.drop(df_rating.loc[df_rating['rating'].str.contains('min') == True].index)
df_rating = df_rating.reset_index(drop=True).reset_index()
df_rating.columns = ['id','description']
df_rating['id'] = df_rating['id'].add(1)

df_durationType = df['duration'].str.split(' ',expand = True)[1].drop_duplicates()
df_durationType = df_durationType.to_frame().dropna().reset_index(drop=True).reset_index()
df_durationType.columns = ['id','description']
df_durationType['id'] = df_durationType['id'].add(1)

df_loc = df['country'].str.split(',')
df_country = df_loc.explode().drop_duplicates()
df_country = df_country.to_frame().dropna().reset_index(drop=True).reset_index()
df_country.columns = ['id','name']
df_country['id'] = df_country['id'].add(1)

df_show = df[['show_id','type','title','date_added','release_year','rating','duration','description']]
df_trat = df_show[df_show['rating'].str.contains('min',na = False)][['show_id','rating']]
df_tdur = df_show[['show_id','duration']]
df_tdur = df_tdur.merge(df_trat, on = 'show_id',how = 'left')
df_tdur['duration'] = df_tdur['duration'].fillna(df_tdur['rating'])
df_tdur = df_tdur['duration'].str.split(' ',expand = True)

df_show = df_show.merge(df_type, left_on = 'type', right_on = 'name', how = 'left')\
    .merge(df_rating, left_on = 'rating', right_on = 'description', how = 'left' )
df_tdur = df_tdur.merge(df_durationType,left_on = 1, right_on = 'description')
df_show = pd.concat([df_show,df_tdur],axis = 1)
df_show = df_show = df_show[['show_id','id_x','title','date_added','release_year','id_y','id',0,'description_x']]
df_show.columns = ['id','typeId','title','date_added','release_year','ratingId','durationTypeId','durationQuantity','description']

df_location = pd.concat([df['show_id'],df_loc],axis = 1).explode('country').dropna()
df_location = df_location.merge(df_country,left_on = 'country', right_on = 'name')
df_location = df_location[['show_id','id']].reset_index(drop=True).reset_index()
df_location.columns = ['id','showId','countryId']
df_location['id'] = df_location['id'].add(1)

df_listed = pd.concat([df['show_id'],df_list],axis = 1).explode('listed_in').dropna()
df_listed = df_listed.merge(df_genre, left_on = 'listed_in', right_on = 'description')
df_listed = df_listed[['show_id','id']].reset_index(drop=True).reset_index()
df_listed.columns = ['id','showId','genreId']
df_listed['id'] = df_listed['id'].add(1)

df_dir = df['director'].str.split(',').to_frame()
df_cast = df['cast'].str.split(',').to_frame()

df_dir = pd.concat([df['show_id'],df_dir],axis = 1).explode('director').dropna()
df_cast = pd.concat([df['show_id'],df_cast],axis = 1).explode('cast').dropna()

df_dir.columns = ['show_id','cast']
df_cast.columns = ['show_id','cast']

df_casting = pd.concat([df_dir,df_cast],axis = 0).drop_duplicates()
df_casting = df_casting.merge(df_people, left_on = 'cast', right_on = 'name')
df_casting = df_casting[['show_id','id']].reset_index(drop=True).reset_index()
df_casting.columns = ['id','showId','peopleId']
df_casting['id'] = df_casting['id'].add(1)

with engine.connect() as con:
   df_genre.to_sql(name='genre', con=con, if_exists='append',index = False)
   df_type.to_sql(name='type', con=con, if_exists='append',index = False)
   df_durationType.to_sql(name='durationType', con=con, if_exists='append',index = False)
   df_country.to_sql(name='country', con=con, if_exists='append',index = False)
   df_rating.to_sql(name='rating', con=con, if_exists='append',index = False)
   df_positionType.to_sql(name='positionType', con=con, if_exists='append',index = False)
   df_people.to_sql(name='people', con=con, if_exists='append',index = False)
   df_show.to_sql(name='show', con=con, if_exists='append',index = False)
   df_listed.to_sql(name='listed', con=con, if_exists='append',index = False)
   df_location.to_sql(name='location', con=con, if_exists='append',index = False)
   df_casting.to_sql(name='casting', con=con, if_exists='append',index = False)
 