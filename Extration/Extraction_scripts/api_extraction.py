import requests
import pandas as pd
from datetime import datetime
import os
import json
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "Api_Data")
os.makedirs(DATA_DIR, exist_ok=True)

def get_indicator_metadata(indicator_code):
    url = f"http://api.worldbank.org/v2/indicator/{indicator_code}"
    params = {"format": "json"}

    response = requests.get(url, params=params)
    data = response.json()[1][0]

    return {
        "indicator_id": data["id"],
        "name": data["name"],
        "topics": [t["value"] for t in data.get("topics", [])],
        "source": data["source"]["value"]
    }


def get_country_codes():
    url = "http://api.worldbank.org/v2/country"
    params = {
        "format": "json",
        "per_page": 1000,
    }

    response = requests.get(url, params=params)
    data = response.json()[1]
    print(data)

    country_codes = {
        item["id"]: item["name"]
        for item in data
        if item["region"]["value"] != "Aggregates"
    }

    return country_codes



def get_dataset_country_topic( topic_code,country_code):
    # Obter metadados
    metadata = get_indicator_metadata(topic_code)
    topic_names = ", ".join(metadata["topics"])  # Pode haver mais que um

    url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{topic_code}"
    #ano_atual = datetime.now().year
    params = {
        "format": "json",
        "date": "2000:2024",
        "per_page": 100
    }


    response = requests.get(url, params=params)
    data = response.json()[1] # Visualização clara

    df = pd.DataFrame([{
        "ano": item["date"],
        "valor": item["value"],
        "pais": item["country"]["value"],
        "codigo_pais": item["country"]["id"],
        "indicador": topic_code,
        "categoria": topic_names
    } for item in data])

    return df

def save_dataset_to_csv(df, filename, indicator='Outher', path=DATA_DIR):
    # Cria subpasta com o nome do indicador
    indicador_dir = os.path.join(path, indicator)
    os.makedirs(indicador_dir, exist_ok=True)

    # Salva o arquivo dentro da subpasta
    file_path = os.path.join(indicador_dir, f"{filename}_{indicator}.csv")
    df.to_csv(file_path, index=False)
    print(f"Arquivo {file_path} salvo com sucesso.")

if __name__ == "__main__":
    country_codes = get_country_codes()
    #print(f"Total de países encontrados: {country_codes}")

    """
    Population, total - WB_WDI_SP_POP_TOTL
    GDP growth (annual %) - NY.GDP.MKTP.KD.ZG
    GDP per capita (current US$) - NY.GDP.PCAP.CD
    Inflation, GDP deflator (annual %) - NY.GDP.DEFL.KD.ZG
    International migrant stock, total - SM.POP.TOTL
    International migrant stock (% of population) - SM.POP.TOTL.ZS
    Birth rate, crude (per 1,000 people) - SP.DYN.CBRT.IN
    Life expectancy at birth, total (years) - SP.DYN.LE00.IN
    Unemployment, total (% of total labor force) (modeled ILO estimate) - SL.UEM.TOTL.ZS
    Imports of goods and services (% of GDP) - NE.IMP.GNFS.ZS
    Imports of goods and services (current US$) - NE.IMP.GNFS.CD
    Exports of goods and services (% of GDP) - NE.EXP.GNFS.ZS
    Exports of goods and services (current US$) - NE.EXP.GNFS.CD
    Renewable electricity output (% of total electricity output) - EG.ELC.RNEW.ZS
    Renewable energy consumption (% of total final energy consumption) - EG.FEC.RNEW.ZS
    Access to electricity (% of population) - EG.ELC.ACCS.ZS

    Primary completion rate, total (% of relevant age group) - SE.PRM.CMPT.ZS
    Educational attainment, Doctoral or equivalent, population 25+, total (%) (cumulative) - SE.TER.CUAT.DO.ZS
    Educational attainment, at least completed lower secondary, population 25+, total (%) (cumulative) - SE.SEC.CUAT.LO.ZS
    Educational attainment, at least Master's or equivalent, population 25+, total (%) (cumulative) - SE.TER.CUAT.MS.ZS
    Educational attainment, at least Bachelor's or equivalent, population 25+, total (%) (cumulative) - SE.TER.CUAT.BA.ZS

    """


    # Lista de indicadores e seus nomes
    """
    ID: 6.0.GDP_growth, Nome: GDP growth (annual %)
ID: EG.ELC.RNEW.ZS, Nome: Renewable electricity output (% of total electricity output)
ID: EG.FEC.RNEW.ZS, Nome: Renewable energy consumption (% of total final energy consumption)
ID: NE.EXP.GNFS.CD, Nome: Exports of goods and services (current US$)
ID: NE.EXP.GNFS.ZS, Nome: Exports of goods and services (% of GDP)
ID: NE.IMP.GNFS.CD, Nome: Imports of goods and services (current US$)
ID: NE.IMP.GNFS.ZS, Nome: Imports of goods and services (% of GDP)
ID: NY.GDP.DEFL.87.ZG, Nome: Inflation, GDP deflator (annual %)
ID: NY.GDP.DEFL.KD.ZG, Nome: Inflation, GDP deflator (annual %)
ID: NY.GDP.MKTP.KD.ZG, Nome: GDP growth (annual %)
ID: NY.GDP.MKTP.KN.87.ZG, Nome: GDP growth (annual %)
ID: NY.GDP.PCAP.CD, Nome: GDP per capita (current US$)
ID: SE.PRM.CMPT.ZS, Nome: Primary completion rate, total (% of relevant age group)
ID: SE.SEC.CUAT.LO.ZS, Nome: Educational attainment, at least completed lower secondary, population 25+, total (%) (cumulative)
ID: SE.TER.CUAT.BA.ZS, Nome: Educational attainment, at least Bachelor's or equivalent, population 25+, total (%) (cumulative)
ID: SE.TER.CUAT.DO.ZS, Nome: Educational attainment, Doctoral or equivalent, population 25+, total (%) (cumulative)
ID: SE.TER.CUAT.MS.ZS, Nome: Educational attainment, at least Master's or equivalent, population 25+, total (%) (cumulative)
ID: SL.UEM.TOTL.ZS, Nome: Unemployment, total (% of total labor force) (modeled ILO estimate)
ID: SM.POP.TOTL, Nome: International migrant stock, total
ID: SM.POP.TOTL.ZS, Nome: International migrant stock (% of population)
ID: SP.DYN.CBRT.IN, Nome: Birth rate, crude (per 1,000 people)
ID: SP.DYN.LE00.IN, Nome: Life expectancy at birth, total (years)
ID: SP.POP.TOTL, Nome: Population, total
"""
    indicators = {

        'SP.POP.TOTL':'Population_total',
        'NY.GDP.MKTP.KD.ZG':'GDP_growth_annual_percent',
        'NY.GDP.PCAP.CD':'GDP_per_capita_dolar',
        'SM.POP.TOTL':'International_migrant_stock_total',
        'SM.POP.TOTL.ZS':'International_migrant_stock_percent_of_population',
        'SP.DYN.CBRT.IN':'Birth_rate_crude_per_1000_people',
        'SP.DYN.LE00.IN':'Life_expectancy_at_birth_total_years',
        'SL.UEM.TOTL.ZS':'Unemployment_total_percent_of_total_labor_force__modeled_ILO_estimate',
        'NE.IMP.GNFS.ZS':'Imports_of_goods_and_services_percent_of_GDP',
        'NE.IMP.GNFS.CD':'Imports_of_goods_and_services_current_USD_dollar',
        'NE.EXP.GNFS.ZS':'Exports_of_goods_and_services_percent_of_GDP',
        'NE.EXP.GNFS.CD':'Exports_of_goods_and_services_current_USD_dollar',
        'EG.ELC.RNEW.ZS':'Renewable_electricity_output_percent_of_total_electricity_output', # Falta este
        'EG.FEC.RNEW.ZS':'Renewable_energy_consumption_percent_of_total_final_energy_consumption',
        'EG.ELC.ACCS.UR.ZS':'Access_to_electricity_percent_of_population',#
        'SE.PRM.CMPT.ZS':'Primary_completion_rate_total_percent_of_relevant_age_group',#
        'SE.TER.CUAT.DO.ZS':'Educational_attainment_Doctoral_or_equivalent_population_25_total_percent_cumulative',#
        'SE.SEC.CUAT.LO.ZS':'Educational_attainment_at_least_completed_lower_secondary_population_25-plus_total_percent_cumulative',#
        'SE.TER.CUAT.MS.ZS':"Educational_attainment_at_least_Master_or_equivalent_population_25-plus_total_percent_cumulative",#
        'SE.TER.CUAT.BA.ZS':'Educational_attainment_at_least_Bachelor_or_equivalent_population_25-plus_total_percent_cumulative'#

    }

    short_indicators = {
        'SP.POP.TOTL': ('pop_total', 'pop'),
        'NY.GDP.MKTP.KD.ZG': ('gdp_growth', 'econ'),
        'NY.GDP.PCAP.CD': ('gdp_pc_usd', 'econ'),
        'SM.POP.TOTL': ('mig_stock_total', 'mig'),
        'SM.POP.TOTL.ZS': ('mig_stock_pct', 'mig'),
        'SP.DYN.CBRT.IN': ('birth_rate', 'health'),
        'SP.DYN.LE00.IN': ('life_exp', 'health'),
        'SL.UEM.TOTL.ZS': ('unemp_pct', 'labor'),
        'NE.IMP.GNFS.ZS': ('imports_pct_gdp', 'trade'),
        'NE.IMP.GNFS.CD': ('imports_usd', 'trade'),
        'NE.EXP.GNFS.ZS': ('exports_pct_gdp', 'trade'),
        'NE.EXP.GNFS.CD': ('exports_usd', 'trade'),
        'EG.ELC.RNEW.ZS': ('ren_elc_pct', 'energy'),
        'EG.FEC.RNEW.ZS': ('ren_energy_pct', 'energy'),
        'EG.ELC.ACCS.UR.ZS': ('elec_access_pct', 'infra'),
        'SE.PRM.CMPT.ZS': ('edu_prim_comp', 'edu'),
        'SE.TER.CUAT.DO.ZS': ('edu_phd', 'edu'),
        'SE.SEC.CUAT.LO.ZS': ('edu_sec', 'edu'),
        'SE.TER.CUAT.MS.ZS': ('edu_masters', 'edu'),
        'SE.TER.CUAT.BA.ZS': ('edu_bachelor', 'edu')
    }
    for indicator,indicator_name in indicators.items():
        for country_code, country_name in country_codes.items():
            print(f"Coletando dados para o indicador {indicator} ({indicator_name}) do país {country_name} ({country_code})")
            df = get_dataset_country_topic(indicator, country_code)
            if not df.empty:
                save_dataset_to_csv(df, country_name, indicator=short_indicators[f'{indicator}'][0])
            else:
                print(f"Sem dados para o indicador {indicator} ({indicator_name}) do país {country_name} ({country_code})")
            time.sleep(0.5)
        time.sleep(5)  # Pausa de 1 segundo entre os países para evitar sobrecarga no servidor
