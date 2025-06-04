import pandas as pd
import calendar
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "..","Generated_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)
def enriquecer_dados_com_ano(df, coluna_ano="year"):
    """
    Adiciona colunas úteis baseadas na coluna 'ano':
    - decada
    - seculo
    - bissexto
    - pos_decada
    - evento
    """
    df[coluna_ano] = df[coluna_ano].astype(int)

    # Década (ex: 2010)
    df["Decade"] = (df[coluna_ano] // 10) * 10

    # Século (ex: 21)
    df["century"] = df[coluna_ano].apply(lambda x: (x - 1) // 100 + 1)

    # Bissexto
    df["leap_year"] = df[coluna_ano].apply(calendar.isleap)

    # Posição na década (0 a 9)
    df["decade_pos"] = df[coluna_ano] % 10
    return df


# Exemplo de uso:
if __name__ == "__main__":
    # Exemplo com anos de 2000 a 2024
    anos = pd.DataFrame({"year": list(range(2000, 2025))})
    df_enriquecido = enriquecer_dados_com_ano(anos)

    # Guarda em CSV (opcional)
    ouput_path = os.path.join(OUTPUT_DIR, "anos_enriquecidos.csv")
    df_enriquecido.to_csv(ouput_path, index=False)
    print(df_enriquecido.head(10))
