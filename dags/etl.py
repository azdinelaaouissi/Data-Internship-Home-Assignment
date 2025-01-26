from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.extract import extract_jobs
from src.transform import clean_and_transform
from src.load import load_to_sqlite

# Définition des paramètres par défaut du DAG
default_args = {
    'owner': 'azdine',
    'depends_on_past': False,
    'email': ['laaouissi.azdine@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Création du DAG
with DAG(
    dag_id='etl_dag',
    default_args=default_args,
    description='Un ETL simple',
    schedule_interval='@daily',
    catchup=False,  # Désactive l'exécution rétroactive
) as dag:

    # Tâche d'extraction
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_jobs,
        op_kwargs={
            'csv_path': 'source/jobs.csv',  # Chemin du fichier source
            'output_dir': 'staging/extracted',  # Répertoire de sortie pour les fichiers extraits
        },
    )

    # Tâche de transformation
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=clean_and_transform,
        op_kwargs={
            'input_dir': 'staging/extracted',  # Répertoire contenant les fichiers extraits
            'output_dir': 'staging/transformed',  # Répertoire pour les fichiers transformés
        },
    )

    # Tâche de chargement
    load_task = PythonOperator(
        task_id='load',
        python_callable=load_to_sqlite,
        op_kwargs={
            'input_dir': 'staging/transformed',  # Répertoire contenant les fichiers transformés
            'db_path': 'staging/jobs.db',  # Chemin de la base de données SQLite
        },
    )

    # Définition des dépendances entre les tâches
    extract_task >> transform_task >> load_task
