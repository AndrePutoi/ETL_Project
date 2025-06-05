import pandas as pd
import os
import requests
import pycountry_convert as pc

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
    data = data_json[1]

    paises = []

    for item in data:
        if item["region"]["value"] == "Aggregates":
            continue

        paises.append({
            "IS03_Code": item.get("id"),
            "Country": item.get("name"),
            "Region": item.get("region", {}).get("value", "não definido"),
            "SubRegion": item.get("adminregion", {}).get("value", "não definido"),
            "Income_Level": item.get("incomeLevel", {}).get("value", "não definido"),
            "Lending_Type": item.get("lendingType", {}).get("value", "não definido"),
            "Capital": item.get("capitalCity", "não definido"),
            "Latitude": item.get("latitude", "não definido"),
            "Longitude": item.get("longitude", "não definido")
        })

    df = pd.DataFrame(paises)
    df.reset_index(drop=True, inplace=True)
    df.insert(0, "Country_ID", df.index + 1)

    return df


def get_country_iso3(iso3_code):
    """
    Recebe código ISO-3 e retorna o nome do continente.
    """
    try:
        country_alpha2 = pc.country_alpha3_to_country_alpha2(iso3_code)
        continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
        continent_name = pc.convert_continent_code_to_continent_name(continent_code)
        return continent_name
    except Exception:
        return "não definido"


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    OUTPUT_DIR = os.path.join(BASE_DIR, "..", "Generated_data")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df_paises = extrair_todos_paises()
    df_paises['Continent'] = df_paises['ISO3_code'].apply(get_country_iso3)
    Paises_sem_Continente = df_paises[df_paises['Continent'] == "não definido"]

    print(f"[!] {Paises_sem_Continente['ISO3_code']} países não possuem continente definido:")
    dict_cont_faltante = {
        'CHI': 'North America',
        'SXM': 'North America',
        'TLS': 'Asia',
        'XKX': 'Europe'
    }

    for iso3, continent in dict_cont_faltante.items():
        df_paises.loc[df_paises['ISO3_code'] == iso3, 'Continent'] = continent
        print(f"  - {iso3} -> {continent}")

    print(df_paises['Continent'].value_counts())


    df_paises.to_csv(os.path.join(OUTPUT_DIR, "Pais_metadados.csv"), index=False)
    print("[+] Pais_metadados.csv gerado com sucesso.")

    # Falta o Logging e GG no more
