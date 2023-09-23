"""
DAG de ETL Lotofacil
"""
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os


df = pd.read_excel("/usr/local/airflow/ETL-ENGENHARIA DE DADOS-DNC/Data/Lotofacil.xlsx")

def extract_data():
    path_file = "Data/Lotofacil.xlsx"
    try:
        df = pd.read_excel(path_file, engine="openpyxl")
        return df
    except:
        print("Erro na leitura do arquivo")
        return None


def remove_cifrao(value):
    if isinstance(value, str) and "R$" in value:
        # Remove o símbolo "R$" e os caracteres de formatação, exceto as vírgulas
        cleaned_value = value.replace("R$", "").replace(".", "").strip()
        # Substitui a vírgula decimal por um ponto
        cleaned_value = cleaned_value.replace(",", ".")
        # Converte o valor para float
        try:
            return float(cleaned_value)
        except Exception:
            (f"O dado: {value} estão em um formato não permitido: {type(value)}")
    return value


def transform_data(df):
    cols = [
        "Rateio 15 acertos",
        "Rateio 14 acertos",
        "Rateio 13 acertos",
        "Rateio 12 acertos",
        "Rateio 11 acertos",
        "Acumulado 15 acertos",
        "Arrecadacao Total",
        "Estimativa Prêmio",
        "Acumulado sorteio especial Lotofácil da Independência",
    ]

    for col in cols:
        df[col] = df[col].apply(remove_cifrao)
    return df


with DAG(
    dag_id="ETL_Lotofacil_v1",
    start_date=datetime(2023, 6, 1),
    schedule="@daily",
    catchup=True,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    #read_data = PythonOperator(task_id="read_data", python_callable=extract_data)
    transform = PythonOperator(task_id="transform_data", python_callable=transform_data)

start >> transform >> end
