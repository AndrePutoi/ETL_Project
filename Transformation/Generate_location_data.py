import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "Generated_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

import requests
import pandas as pd


def extrair_todos_paises():
    """
    Extrai metadados completos sobre todos os países da API do World Bank.

    Retorna:
        DataFrame com colunas úteis para uma DimPais.
    """
    url = "https://api.worldbank.org/v2/country"
    params = {
        "format": "json",
        "per_page": 1000,
        "page": 1
    }

    response = requests.get(url, params=params)
    data_json = response.json()

    total_paises = data_json[0]["total"]
    data = data_json[1]  # Página 1

    # Se houver mais páginas (raro, pois usamos per_page=1000), adicionar aqui

    paises = []

    for item in data:
        # Pular agregados regionais como "World", "OECD", etc.
        if item["region"]["value"] == "Aggregates":
            continue

        paises.append({
            "CodigoISO3": item.get("id"),
            "Nome": item.get("name"),
            "Regiao": item.get("region", {}).get("value", "não definido"),
            "SubRegiao": item.get("adminregion", {}).get("value", "não definido"),
            "Renda": item.get("incomeLevel", {}).get("value", "não definido"),
            "TipoFinanciamento": item.get("lendingType", {}).get("value", "não definido"),
            "Capital": item.get("capitalCity", "não definido"),
            "Latitude": item.get("latitude", "não definido"),
            "Longitude": item.get("longitude", "não definido")
        })

    df = pd.DataFrame(paises)
    df.reset_index(drop=True, inplace=True)
    df.insert(0, "PaisID", df.index + 1)  # Criar ID sequencial

    return df

if __name__ == "__main__":
    df_paises = extrair_todos_paises()
    df_paises.to_csv(os.path.join(OUTPUT_DIR, "Pais_metadados.csv"), index=False)
    print("[+] DimPais.csv gerado com sucesso.")