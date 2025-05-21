import requests
import pandas as pd
from datetime import datetime
import os
import json
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "Api_Data")
os.makedirs(DATA_DIR, exist_ok=True)
def get_country_codes():
    url = "http://api.worldbank.org/v2/country"
    params = {
        "format": "json",
        "per_page": 100,
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
        "topic_code": item["value"],
        "pais": item["country"]["value"],
        "codigo_pais": item["country"]["id"]
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


    indicators = {
    }
    for indicator,indicator_name in indicators.items():
        for country_code, country_name in country_codes.items():
            print(f"Coletando dados para o indicador {indicator} ({indicator_name}) do país {country_name} ({country_code})")
            df = get_dataset_country_topic(indicator, country_code)
            if not df.empty:
                save_dataset_to_csv(df, country_name, indicator_name)
            else:
                print(f"Sem dados para o indicador {indicator} ({indicator_name}) do país {country_name} ({country_code})")
            time.sleep(0.5)
        time.sleep(5)  # Pausa de 1 segundo entre os países para evitar sobrecarga no servidor
