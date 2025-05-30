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
    data = data_json[1]  # Página 1

    # Se houver mais páginas (raro, pois usamos per_page=1000), adicionar aqui

    paises = []

    for item in data:
        # Pular agregados regionais como "World", "OECD", etc.
        if item["region"]["value"] == "Aggregates":
            continue

        paises.append({
            "ISO3_code": item.get("id"),
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
    df.insert(0, "PaisID", df.index + 1)  # Criar ID sequencial

    return df




def contry_continent(df):
    import pycountry_convert as pc

    country_codes = [
        'ABW', 'AFG', 'AGO', 'ALB', 'AND', 'ARE', 'ARG', 'ARM', 'ASM', 'ATG',
        'AUS', 'AUT', 'AZE', 'BDI', 'BEL', 'BEN', 'BFA', 'BGD', 'BGR', 'BHR',
        'BHS', 'BIH', 'BLR', 'BLZ', 'BMU', 'BOL', 'BRA', 'BRB', 'BRN', 'BTN',
        'BWA', 'CAF', 'CAN', 'CHE', 'CHI', 'CHL', 'CHN', 'CIV', 'CMR', 'COD',
        'COG', 'COL', 'COM', 'CPV', 'CRI', 'CUB', 'CUW', 'CYM', 'CYP', 'CZE',
        'DEU', 'DJI', 'DMA', 'DNK', 'DOM', 'DZA', 'ECU', 'EGY', 'ERI', 'ESP',
        'EST', 'ETH', 'FIN', 'FJI', 'FRA', 'FRO', 'FSM', 'GAB', 'GBR', 'GEO',
        'GHA', 'GIB', 'GIN', 'GMB', 'GNB', 'GNQ', 'GRC', 'GRD', 'GRL', 'GTM',
        'GUM', 'GUY', 'HKG', 'HND', 'HRV', 'HTI', 'HUN', 'IDN', 'IMN', 'IND',
        'IRL', 'IRN', 'IRQ', 'ISL', 'ISR', 'ITA', 'JAM', 'JOR', 'JPN', 'KAZ',
        'KEN', 'KGZ', 'KHM', 'KIR', 'KNA', 'KOR', 'KWT', 'LAO', 'LBN', 'LBR',
        'LBY', 'LCA', 'LIE', 'LKA', 'LSO', 'LTU', 'LUX', 'LVA', 'MAC', 'MAF',
        'MAR', 'MCO', 'MDA', 'MDG', 'MDV', 'MEX', 'MHL', 'MKD', 'MLI', 'MLT',
        'MMR', 'MNE', 'MNG', 'MNP', 'MOZ', 'MRT', 'MUS', 'MWI', 'MYS', 'NAM',
        'NCL', 'NER', 'NGA', 'NIC', 'NLD', 'NOR', 'NPL', 'NRU', 'NZL', 'OMN',
        'PAK', 'PAN', 'PER', 'PHL', 'PLW', 'PNG', 'POL', 'PRI', 'PRK', 'PRT',
        'PRY', 'PSE', 'PYF', 'QAT', 'ROU', 'RUS', 'RWA', 'SAU', 'SDN', 'SEN',
        'SGP', 'SLB', 'SLE', 'SLV', 'SMR', 'SOM', 'SRB', 'SSD', 'STP', 'SUR',
        'SVK', 'SVN', 'SWE', 'SWZ', 'SXM', 'SYC', 'SYR', 'TCA', 'TCD', 'TGO',
        'THA', 'TJK', 'TKM', 'TLS', 'TON', 'TTO', 'TUN', 'TUR', 'TUV', 'TZA',
        'UGA', 'UKR', 'URY', 'USA', 'UZB', 'VCT', 'VEN', 'VGB', 'VIR', 'VNM',
        'VUT', 'WSM', 'XKX', 'YEM', 'ZAF', 'ZMB', 'ZWE'
    ]

    for code in country_codes:
        try:
            # Código ISO-3 para nome de país (alguns não são reconhecidos)
            country_alpha2 = pc.country_alpha3_to_country_alpha2(code)
            continent_code = pc.country_alpha2_to_continent_code(country_alpha2)

            continent_name = pc.convert_continent_code_to_continent_name(continent_code)
            print(f"{code}: {continent_name}")

        except Exception as e:
            print(f"⚠️ Código {code} não encontrado ou erro: {e}")

    return

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    OUTPUT_DIR = os.path.join(BASE_DIR, "..", "Generated_data")
    os.makedirs(OUTPUT_DIR, exist_ok=True)


    df_paises = extrair_todos_paises()
    print(len(df_paises['ISO3_code'].unique()))
    df_paises.to_csv(os.path.join(OUTPUT_DIR, "Pais_metadados.csv"), index=False)
    print("[+] DimPais.csv gerado com sucesso.")