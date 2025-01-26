import os
import json

def transform():
    input_dir = "staging/extracted"
    output_dir = "staging/transformed"
    os.makedirs(output_dir, exist_ok=True)

    for file_name in os.listdir(input_dir):
        with open(f"{input_dir}/{file_name}", "r") as f:
            data = json.loads(f.read())

        transformed_data = {
            "job": {
                "title": data.get("job_title"),
                "industry": data.get("job_industry"),
                "description": data.get("job_description"),
                "employment_type": data.get("job_employment_type"),
                "date_posted": data.get("job_date_posted"),
            },
            "company": {
                "name": data.get("company_name"),
                "link": data.get("company_linkedin_link"),
            },
            "education": {
                "required_credential": data.get("job_required_credential"),
            },
            "experience": {
                "months_of_experience": data.get("job_months_of_experience"),
                "seniority_level": data.get("seniority_level"),
            },
            "salary": {
                "currency": data.get("salary_currency"),
                "min_value": data.get("salary_min_value"),
                "max_value": data.get("salary_max_value"),
                "unit": data.get("salary_unit"),
            },
            "location": {
                "country": data.get("country"),
                "locality": data.get("locality"),
                "region": data.get("region"),
                "postal_code": data.get("postal_code"),
                "street_address": data.get("street_address"),
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
            },
        }

        with open(f"{output_dir}/{file_name.replace('.txt', '.json')}", "w") as f:
            json.dump(transformed_data, f)
