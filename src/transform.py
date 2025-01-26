import os
import json

def clean_and_transform(input_dir, output_dir):
    """
    Reads extracted text files, cleans and transforms JSON data, and saves as JSON files.
    """
    # Crée le répertoire de sortie s'il n'existe pas
    os.makedirs(output_dir, exist_ok=True)

    for file in os.listdir(input_dir):
        input_path = os.path.join(input_dir, file)
        output_path = os.path.join(output_dir, file.replace(".txt", ".json"))

        # Vérifie si le fichier est vide
        if os.path.getsize(input_path) == 0:
            print(f"Fichier vide ignoré : {input_path}")
            continue

        try:
            # Lit le contenu du fichier extrait
            with open(input_path, 'r') as f:
                raw_data = f.read()

            # Convertit le contenu en JSON
            job_data = json.loads(raw_data)

            # Transformation des données
            transformed_data = {
                "job": {
                    "title": job_data.get("job_title", ""),
                    "industry": job_data.get("job_industry", ""),
                    "description": job_data.get("job_description", ""),
                    "employment_type": job_data.get("job_employment_type", ""),
                    "date_posted": job_data.get("job_date_posted", ""),
                },
                "company": {
                    "name": job_data.get("company_name", ""),
                    "link": job_data.get("company_linkedin_link", ""),
                },
                "education": {
                    "required_credential": job_data.get("job_required_credential", ""),
                },
                "experience": {
                    "months_of_experience": job_data.get("job_months_of_experience", 0),
                    "seniority_level": job_data.get("seniority_level", ""),
                },
                "salary": {
                    "currency": job_data.get("salary_currency", ""),
                    "min_value": job_data.get("salary_min_value", 0),
                    "max_value": job_data.get("salary_max_value", 0),
                    "unit": job_data.get("salary_unit", ""),
                },
                "location": {
                    "country": job_data.get("country", ""),
                    "locality": job_data.get("locality", ""),
                    "region": job_data.get("region", ""),
                    "postal_code": job_data.get("postal_code", ""),
                    "street_address": job_data.get("street_address", ""),
                    "latitude": job_data.get("latitude", 0.0),
                    "longitude": job_data.get("longitude", 0.0),
                },
            }

            # Écrit les données transformées dans un fichier JSON
            with open(output_path, 'w') as f:
                json.dump(transformed_data, f, indent=4)

        except json.JSONDecodeError:
            print(f"Erreur : Contenu JSON invalide dans le fichier {input_path}. Ignoré.")
        except Exception as e:
            print(f"Erreur inattendue pour le fichier {input_path} : {e}")
