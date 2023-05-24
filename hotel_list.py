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


df = pd.read_csv("0440c8ba-71e6-41f4-bbaa-1c792277de76", delimiter=';',encoding="ISO-8859-1")

df_hotel = df[df["TYPE D'HÉBERGEMENT"] == "HÔTEL DE TOURISME"].reset_index(drop = True)


df_hotel["ADRESSE"] = df_hotel["ADRESSE"].apply(lambda x: x.replace(" ", "+"))

df_hotel["LONGITUDE"] = np.nan
df_hotel["LATITUDE"] = np.nan


#TODO : Tester fonction asynchrone / Multithreading#

for i in range(df_hotel.shape[0]):
    #TODO : Tester fonction asynchrone / Multithreading#

    print(i)
    requete = requests.get("https://api-adresse.data.gouv.fr/search/?q=" + df_hotel["ADRESSE"].loc[i] + "&postcode=" + str(df_hotel["CODE POSTAL"].loc[i])).json()
    
    try:
        coordinates = requete["features"][0]["geometry"]["coordinates"]
        df_hotel["LONGITUDE"].loc[i] = coordinates[0]
        df_hotel["LATITUDE"].loc[i] = coordinates[1]
    
    except:
        "No coordinates"

