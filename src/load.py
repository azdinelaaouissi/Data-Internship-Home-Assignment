import os
import json
import sqlite3

def load_to_sqlite(input_dir, db_path):
    """Charge les données transformées dans SQLite."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title VARCHAR(225),
            industry VARCHAR(225),
            description TEXT,
            employment_type VARCHAR(125),
            date_posted DATE
        );
    """)

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

    conn.commit()
    conn.close()
