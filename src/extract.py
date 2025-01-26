import pandas as pd
import os


def extract_jobs(csv_path, output_dir):
    """Extrait les données du fichier CSV et les enregistre sous forme de fichiers texte."""
    # Crée le répertoire de sortie s'il n'existe pas
    os.makedirs(output_dir, exist_ok=True)

    # Charge le fichier CSV en DataFrame
    df = pd.read_csv(csv_path)

    # Boucle sur chaque ligne du DataFrame
    for index, row in df.iterrows():
        # Récupère la valeur de la colonne 'context'
        context = row.get('context', None)

        # Gère les cas où 'context' est NaN ou non valide
        if pd.isna(context) or not isinstance(context, str):
            context = "No context available"

        # Écrit le contenu dans un fichier texte
        with open(f"{output_dir}/job_{index}.txt", "w") as f:
            f.write(context)
