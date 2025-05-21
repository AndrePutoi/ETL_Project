import requests
import pandas as pd

url = "http://api.worldbank.org/v2/indicator"
params = {"format": "json", "per_page": 100000000}
response = requests.get(url, params=params)
indicadores = response.json()[1]

#for indicador in indicadores:
#    print(f"ID: {indicador['id']}, Nome: {indicador['name']}")
#    # Aqui você pode adicionar mais lógica para processar os dados conforme necessário

lista=["Population, total" ,
    "GDP growth (annual %)" ,
    "GDP per capita (current US$)",
    "Inflation, GDP deflator (annual %)",
    "International migrant stock, total",
    "International migrant stock (% of population)",
    "Birth rate, crude (per 1,000 people)",
    "Life expectancy at birth, total (years)",
    "Unemployment, total (% of total labor force) (modeled ILO estimate)",
    "Imports of goods and services (% of GDP)",
    "Imports of goods and services (current US$)",
    "Exports of goods and services (% of GDP)",
    "Exports of goods and services (current US$)",
    "Renewable electricity output (% of total electricity output)",
    "Renewable energy consumption (% of total final energy consumption)",
    "Access to electricity (% of population)]",

    "Primary completion rate, total (% of relevant age group)",
    "Educational attainment, Doctoral or equivalent, population 25+, total (%) (cumulative)",
    "Educational attainment, at least completed lower secondary, population 25+, total (%) (cumulative)",
    "Educational attainment, at least Master's or equivalent, population 25+, total (%) (cumulative)",
    "Educational attainment, at least Bachelor's or equivalent, population 25+, total (%) (cumulative)"]

for indicador in indicadores:
    if indicador['name'] in lista:
        print(f"ID: {indicador['id']}, Nome: {indicador['name']}")

        # Aqui você pode adicionar mais lógica para processar os dados conforme necessário
