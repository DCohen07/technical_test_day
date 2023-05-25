from bs4 import BeautifulSoup as bs
import requests 
import wget
import pandas as pd
import numpy as np


###### RÉCUPÉRATION DU FICHIER SOURCE SUR LES HOTELS ######

hotel_url = 'https://www.data.gouv.fr/fr/datasets/hebergements-collectifs-classes-en-france/'
page = requests.get(hotel_url)
soup = bs(page.content, "lxml")
print(soup)
links = soup.find_all("a",attrs={'title': 'Télécharger la ressource'})
for a in links : 
    print(a['href'])
    
data_link = links[1]['href']

file = wget.download(data_link)


import pandas as pd
import numpy as np
import threading
import requests
import time
import concurrent.futures


df = pd.read_csv("0440c8ba-71e6-41f4-bbaa-1c792277de76", delimiter=';',encoding="ISO-8859-1")

df_hotel = df[df["TYPE D'HÉBERGEMENT"] == "HÔTEL DE TOURISME"].reset_index(drop = True)

df_hotel["ADRESSE"] = df_hotel["ADRESSE"].apply(lambda x: x.replace(" ", "+"))

df_hotel["LONGITUDE"] = np.nan
df_hotel["LATITUDE"] = np.nan

adresse = df_hotel["ADRESSE"]
code_postal = [str(i) for i in df_hotel["CODE POSTAL"]]

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

urls = ["https://api-adresse.data.gouv.fr/search/?q=" + add + "&postcode=" + code for add,code in zip(adresse, code_postal)]

a = []
b = []

liste = [i for i in range(len(urls))]


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

