import pandas as pd
from bs4 import BeautifulSoup
import requests
from sqlalchemy import create_engine, text


DOMAIN = "https://www.fidal.it/risultati/"


cod = 'REG38222'
anno = '2025'
urls = [f"{DOMAIN}{anno}/{cod}/Iscrizioni/IndexPerGara.html"]
urls.append(f"{DOMAIN}{anno}/{cod}/Risultati/IndexRisultatiPerGara.html")

data = pd.DataFrame(columns=['nome', 'gara'])
for url in urls:
    r = requests.get(url).text
    soup = BeautifulSoup(r, 'html.parser')
    elements = soup.find_all('a')
    
    for element in elements:
        link = element['href']
        if link[0] == '#': continue

        nome = element.text.strip()
        nome = nome[:500] # Tronca se troppo lunghi
        link = link[:500] # Tronca se troppo lunghi
        data.loc[len(data)] = [nome, link]
                            
print(data)
