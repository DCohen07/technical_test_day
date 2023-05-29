from bs4 import BeautifulSoup as bs
import requests 
import wget
import pandas as pd
import numpy as np
import gzip
import shutil
import polars as pl
# install pyarrow


###### RÉCUPÉRATION DU FICHIER SOURCE SUR LES HOTELS ######

hotel_url = 'https://www.data.gouv.fr/fr/datasets/hebergements-collectifs-classes-en-france/'
page = requests.get(hotel_url)
soup = bs(page.content)
print(soup)
links = soup.find_all("a",attrs={'title': 'Télécharger la ressource'})
for a in links : 
    print(a['href'])
    
data_link = links[1]['href']

file = wget.download(data_link)


import numpy as np
import threading
import requests
import time
import concurrent.futures


df = pd.read_csv("0440c8ba-71e6-41f4-bbaa-1c792277de76", delimiter=';',encoding="ISO-8859-1", dtype={"CODE POSTAL": str})

df_hotel = df[df["TYPE D'HÉBERGEMENT"] == "HÔTEL DE TOURISME"].reset_index(drop = True)

df_hotel["ADRESSE"] = df_hotel["ADRESSE"].apply(lambda x: x.replace(" ", "+"))
df_hotel["ADRESSE"] = df_hotel["ADRESSE"].apply(lambda x: x.replace("++", "+"))

adresse = df_hotel["ADRESSE"]
code_postal = [str(i) for i in df_hotel["CODE POSTAL"]]


col_tmp = [add + "+" + code for add,code in zip(adresse, code_postal)]

col_tmp = [i.lower() for i in col_tmp]

df_hotel["concat"] = col_tmp

pl_hotel = pl.from_pandas(df_hotel)

"""
for i in range(df_hotel.shape[0]):

    print(i)
    requete = requests.get("https://api-adresse.data.gouv.fr/search/?q=" + df_hotel["ADRESSE"].loc[i] + "&postcode=" + str(df_hotel["CODE POSTAL"].loc[i])).json()
    
    try:
        coordinates = requete["features"][0]["geometry"]["coordinates"]
        df_hotel["LONGITUDE"].loc[i] = coordinates[0]
        df_hotel["LATITUDE"].loc[i] = coordinates[1]
    except:
        "No coordinates"
# appel de la fonction
"""


"""
for add, code in zip(adresse, code_postal):
    print(add)

    requete = requests.get("https://api-adresse.data.gouv.fr/search/?q=" + add + "&postcode=" + code).json()

    try:
        coordinates = requete["features"][0]["geometry"]["coordinates"]
        a.append(coordinates[0])
        b.append(coordinates[1])
    except:
        "No coordinates"
"""




"""
def func(url):
    requete = requests.get(url).json()

    if requete["features"] != []:
        coordinates = requete["features"][0]["geometry"]["coordinates"]
        a.append(coordinates[0])
        b.append(coordinates[1])
    else:
        pass
    
processed_results = []

start = time.perf_counter()
threads = []
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = [executor.submit(func, url) for url in urls]
        for f in concurrent.futures.as_completed(results):
            processed_results.append(f.result())

finish = time.perf_counter()
print(f'Finished in {round(finish-start, 2)} second(s)')
"""




######### SOLUTION #########

adresse = 'https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/'
page = requests.get(adresse)
soup = bs(page.content)
links = soup.find_all("a")

### Récupération du fichier d'intérêt ###

for a in links:
    if 'adresses-france' in a["href"]:
        wget.download(adresse + a["href"])
    else:
         pass

### Extraction ###

with gzip.open('adresses-france.csv.gz', 'rb') as f_in:
    with open('adresses-france.csv', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


### Traitement ###


import polars as pl

pl_df = pl.read_csv("adresses-france.csv", sep=";", dtypes={"numero":pl.Utf8, "code_postal":pl.Utf8, "code_insee":pl.Utf8})

pl_df = pl_df.fill_null(value="")

col = pl_df[["numero", "rep","nom_voie","code_postal"]].apply(lambda x: "+".join(x))

col = col.with_columns(
    pl.col('apply').str.replace(r"\++", "+")
)

col = col.with_columns(
    pl.col('apply').str.replace(r"\s++", "+")
)

col = col.with_columns(
    pl.col('apply').str.to_lowercase()
)



df_pl_clean = pl.concat([pl_df, col], how="horizontal")

df_pl_clean = df_pl_clean.rename({"apply":"concat"})

pl_join = pl_hotel.join(df_pl_clean, on="concat", how="left").select(["DATE DE CLASSEMENT", "CLASSEMENT", "NOM COMMERCIAL", "ADRESSE", "CODE POSTAL","COMMUNE", "NOMBRE DE CHAMBRES","concat", "x","y","lon","lat"])



pl_join["x"].is_not_null().sum()


pl_join = pl_join.filter(pl.col("x").is_not_null())



### SQL ###


import sqlite3, sqlalchemy
from sqlalchemy import Table, Column, Integer, Float,String, ForeignKey, MetaData, create_engine, text, inspect

engine = create_engine('sqlite:///technical_test.db', echo=True)
meta = MetaData()

connection = engine.raw_connection()
cursor = connection.cursor()

hotel = Table(
    "hotel", meta,
    Column("DATE DE CLASSEMENT", String),
    Column("CLASSEMENT", String),
    Column("NOM COMMERCIAL", String),
    Column("ADRESSE", String),
    Column("CODE POSTAL", String),
    Column("COMMUNE", String),
    Column("NOMBRE DE CHAMBRES", String),
    Column("concat", String),
    Column("x", Float),
    Column("y", Float),
    Column("lon", Float),
    Column("lat", Float),
)

pl_join.to_pandas().to_sql("hotel",con=engine)

with engine.connect() as conn:
   conn.execute(text("SELECT * FROM hotel")).fetchall()
