import os
from src.extract import extract_jobs
from src.transform import clean_and_transform
from src.load import load_to_sqlite

def test_extract():
    extract_jobs('source/jobs.csv', 'staging/extracted')
    assert os.path.exists('staging/extracted/job_0.txt')

def test_transform():
    clean_and_transform('staging/extracted', 'staging/transformed')
    assert os.path.exists('staging/transformed/job_0.json')

def test_load():
    load_to_sqlite('staging/transformed', 'staging/jobs.db')
    assert os.path.exists('staging/jobs.db')
