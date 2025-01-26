import pandas as pd
import os

def extract():
    input_file = "source/jobs.csv"
    output_dir = "staging/extracted"
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(input_file)
    for index, row in df.iterrows():
        with open(f"{output_dir}/job_{index}.txt", "w") as f:
            f.write(row['context'])
