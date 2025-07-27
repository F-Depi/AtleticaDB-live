import pandas as pd
from func_scrape import *


df_link = pd.read_csv('database_link/indoor_2025/link_risultati.csv')
df_link = df_link[(df_link['Versione Sigma'] == 'Vecchio')]

write_header = True
for ii,row in df_link.iterrows():
    df_temp = scrape_vecchio_corse(row)
    df_temp.to_csv('TEST_vecchio2.csv', mode='a', index=False, header=write_header)
    write_header = False
