import pyodbc
import pandas as pd
import os

# Caminho base da DAG
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Caminho do arquivo CSV final
csv_file_path = os.path.join(BASE_DIR, "..", "Transformation", "Final_Aggregation", "Final_data", "all_indicators_concatenated.csv")
# Lê o CSV
df = pd.read_csv(csv_file_path)
# Tirar o PaisID
df.dropna(inplace=True)

df.rename(columns={
    'year': 'YEAR',
    'country_code': 'IS03_Code',
    'indicator': 'WB_Code',
    'value': 'Value'
}, inplace=True, errors='ignore')

# Conexão com SQL Server
server = r'DESKTOP-1QEIURD\SQLEXPRESS'
database = 'Indicators_DB_test'
username = 'sa'
password = 'sa'

# String de conexão
cnxn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password}'
)
cursor = cnxn.cursor()

# Inserir dados linha a linha
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO FactIndicatores (YEAR, ISO3_Code, WB_Code, Value)
        VALUES (?, ?, ?, ?, ?)
    """, row.YEAR, row.ISO3_Code, row.WB_Code, row.Value)

# Commit e fecha conexão
cnxn.commit()
cursor.close()
cnxn.close()

print("Upload concluído!")
