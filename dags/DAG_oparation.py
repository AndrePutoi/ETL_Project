from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import requests
import logging
import pyodbc

def data_extraction():
    logger = logging.getLogger("airflow.task")
    logger.setLevel(logging.INFO)

    # Diretório base onde está o arquivo da DAG
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(dag_dir, "lista_paises_extraida.csv")

    if not os.path.exists(csv_path):
        logger.error(f"Arquivo {csv_path} não encontrado.")
        return

    # Carrega a lista de países (coluna com ISO2 ou ISO3)
    lista_paises = pd.read_csv(csv_path)
    paises_lista = lista_paises.iloc[:, 0].tolist()

    # Mapeamento de indicadores
    short_indicators = {
        "NE.GDI.TOTL.ZS": "GrossCapitalFormation",
        "NY.GNS.ICTR.ZS": "GrossSavings",
        "NE.IMP.GNFS.ZS": "ImportsGDP",
        "NV.IND.TOTL.ZS": "IndustryValueAdded",
        "FP.CPI.TOTL.ZG": "InflationCPI",
        "NY.GDP.DEFL.KD.ZG": "InflationDeflator",
        "NV.MNF.TECH.ZS.UN": "HighTechManufacturing",
        "GC.REV.XGRT.GD.ZS": "RevenueExclGrants",
        "DT.DOD.DSTC.IR.ZS": "ShortTermDebt",
        "DT.TDS.DECT.EX.ZS": "TotalDebtService",
        "FI.RES.TOTL.CD": "TotalReserves",
        "NV.AGR.TOTL.ZS": "AgricultureValueAdded",
        "GC.DOD.TOTL.GD.ZS": "CentralGovDebt",
        "GC.XPN.TOTL.GD.ZS": "ExpenseGDP",
        "NE.EXP.GNFS.ZS": "ExportsGDP",
        "DT.DOD.DECT.GN.ZS": "ExternalDebtStocks",
        "NY.GDP.MKTP.CD": "GDPcurrentUSD",
        "NY.GDP.MKTP.KD.ZG": "GDPgrowth",
        "NY.GDP.PCAP.CD": "GDPperCapita",
        "NY.GDP.PCAP.KD.ZG": "GDPperCapitaGrowth"
    }

    # Obtém ano atual e calcula intervalo
    current_year = datetime.now().year
    start_year = current_year - 4
    date_range = f"{start_year}:{current_year}"

    # Mapeia códigos ISO2 → ISO3
    def get_country_codes_with_iso2_mapping():
        url = "http://api.worldbank.org/v2/country"
        params = {"format": "json", "per_page": 1000}
        response = requests.get(url, params=params)
        data = response.json()[1]
        iso2_to_iso3 = {}
        for item in data:
            if item["region"]["value"] != "Aggregates":
                iso2_to_iso3[item["iso2Code"]] = item["id"]
        return iso2_to_iso3

    iso2_to_iso3 = get_country_codes_with_iso2_mapping()
    paises_iso3 = [iso2_to_iso3.get(p, p) for p in paises_lista]

    final_df = []

    for indicator, indicator_name in short_indicators.items():
        for country_code in paises_iso3:
            url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}"
            params = {"format": "json", "date": date_range, "per_page": 100}
            response = requests.get(url, params=params)

            try:
                data = response.json()[1]
            except Exception:
                logger.warning(f"Nenhum dado encontrado para {country_code} - {indicator}")
                continue

            for item in data:
                final_df.append({
                    "year": item["date"],
                    "Value": item["value"],
                    "country_code": country_code,
                    "indicator": indicator
                })

    df_final = pd.DataFrame(final_df)

    if not df_final.empty:
        output_path = os.path.join(dag_dir, "worldbank_dados_ultimos_5_anos.csv")
        df_final.to_csv(output_path, index=False)
        logger.info(f"Dados salvos em: {output_path}")
    else:
        logger.warning("Nenhum dado coletado.")




def extract_new_data():
    logger = logging.getLogger("airflow.task")
    logger.setLevel(logging.INFO)

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "Final_data")

    # Carregar os novos dados
    new_data_path = os.path.join(BASE_DIR, "worldbank_dados_ultimos_5_anos.csv")
    df_new_raw = pd.read_csv(new_data_path)
    if df_new_raw.empty:
        logger.info("Nenhum novo dado encontrado para carregar.")
        return

    # Caminho dos dados existentes
    existing_data_path = os.path.join(DATA_DIR, "all_indicators_concatenated.csv")

    # Verifica se arquivo existente existe
    if not os.path.exists(existing_data_path):
        logger.info("Arquivo de dados existentes não encontrado. Criando novo com os dados atuais.")
        df_new_raw.to_csv(existing_data_path, index=False)
        return

    df_existing = pd.read_csv(existing_data_path)
    if df_existing.empty:
        logger.info("Nenhum dado existente encontrado para comparar.")
        return

    # Detectar apenas dados novos (excluindo duplicatas exatas)
    df_new = df_new_raw[~df_new_raw.apply(tuple, axis=1).isin(df_existing.apply(tuple, axis=1))]

    if not df_new.empty:
        df_updated = pd.concat([df_existing, df_new], ignore_index=True)
        df_updated.to_csv(existing_data_path, index=False)
        logger.info(f"Novos dados carregados e salvos em {existing_data_path}")
    else:
        logger.info("Nenhum novo dado encontrado para carregar.")

    # Também salva os novos dados separados
    new_data_save_path = os.path.join(BASE_DIR, "new_worldbank_data.csv")
    df_new.to_csv(new_data_save_path, index=False)
    logger.info(f"Novos dados salvos separadamente em {new_data_save_path}")






def load_new_data():
    logger = logging.getLogger("airflow.task")
    logger.setLevel(logging.INFO)

    try:
        # Caminho base da DAG
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))

        # Caminho do arquivo CSV final
        csv_file_path = os.path.join(BASE_DIR,"new_worldbank_data.csv")

        # Lê o CSV
        df = pd.read_csv(csv_file_path)
        df.dropna(inplace=True)

        df.rename(columns={
            'year': 'YEAR',
            'country_code': 'IS03_Code',
            'indicator': 'WB_Code',
            'value': 'Value'
        }, inplace=True, errors='ignore')

        # Conexão com o SQL Server
        server = r'DESKTOP-1QEIURD\SQLEXPRESS'
        database = 'Indicators_DB_test'
        username = 'sa'
        password = 'sa'

        cnxn = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password}'
        )
        cursor = cnxn.cursor()

        # Inserção em lote
        records = list(df[['YEAR', 'ISO3_Code', 'WB_Code', 'Value']].itertuples(index=False, name=None))
        if records:
            cursor.executemany("""
                INSERT INTO FactIndicatores (YEAR, ISO3_Code, WB_Code, Value)
                VALUES (?, ?, ?, ?)
            """, records)

            cnxn.commit()
            logger.info(f"{len(records)} registros inseridos na base de dados.")
        else:
            logger.info("Nenhum dado para inserir no banco.")

    except Exception as e:
        logger.error(f"Erro ao carregar dados para o SQL Server: {e}")
    finally:
        try:
            cursor.close()
            cnxn.close()
        except:
            pass


# Definição do DAG
with DAG(
    dag_id='worldbank_etl',
    schedule='@monthly',  # Executa uma vez por mês
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={'retries': 1},
    tags=['worldbank', 'etl']
) as dag:

    # Tarefa 1: Extração inicial (ou pode ser uma limpeza)
    tarefa_data_extraction = PythonOperator(
        task_id='data_extraction',
        python_callable=data_extraction
    )

    # Tarefa 2: Extração de novos dados
    tarefa_extract_new_data = PythonOperator(
        task_id='extract_new_data',
        python_callable=extract_new_data
    )

    # Tarefa 3: Carregar os novos dados
    tarefa_load_new_data = PythonOperator(
        task_id='load_new_data',
        python_callable=load_new_data
    )

    # Definindo a ordem das tarefas
    tarefa_data_extraction >> tarefa_extract_new_data >> tarefa_load_new_data
