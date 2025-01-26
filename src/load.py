import os
import json
import sqlite3

def load():
    db_file = "sqlite_database.db"
    input_dir = "staging/transformed"

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    for file_name in os.listdir(input_dir):
        with open(f"{input_dir}/{file_name}", "r") as f:
            data = json.load(f)

        cursor.execute("""
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?)
        """, (
            data['job']['title'],
            data['job']['industry'],
            data['job']['description'],
            data['job']['employment_type'],
            data['job']['date_posted'],
        ))
        job_id = cursor.lastrowid

        cursor.execute("""
            INSERT INTO company (job_id, name, link)
            VALUES (?, ?, ?)
        """, (job_id, data['company']['name'], data['company']['link']))

    conn.commit()
    conn.close()
