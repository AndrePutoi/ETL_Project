import pyodbc
import pandas as pd
import os

# Caminho para o CSV (ajusta para o teu caminho real!)
csv_file_path = r"C:\Users\André Putoi\Documents\ETL_job\Extration\Generated_data\Pais_metadados.csv"

# Lê o CSV
df = pd.read_csv(csv_file_path)


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
        INSERT INTO DimCountry (ISO3_Code,Country,Region,Income_Level,Lending_Type,Capital,Latitude,Longitude,Continent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?,?)
    """, row.ISO3_Code, row.Country, row.Region, row.Income_Level, row.Lending_Type, row.Capital, row.Latitude, row.Longitude, row.Continent)

# Commit e fecha conexão
cnxn.commit()
cursor.close()
cnxn.close()

print("Upload concluído!")
