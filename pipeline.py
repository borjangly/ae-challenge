import json
import pandas as pd
import duckdb

# Connect to DuckDB
conn = duckdb.connect(database=':memory:')  # In-memory database for simplicity in this assignment


# Creates and normalizes the data at the first level (professionals) and converts it into a dataframe
def load_data(path):
    with open(path, "r") as f:
        json_data = json.load(f)

    return json_data['professionals']


# Processes the professionals data entity and creates relevant tables in duckdb
def process_professionals(input_data):
    df = pd.json_normalize(input_data)

    # Data quality checks
    # Checking for outliers in data. I considered 50 years the break point for an outlier in job experience
    outlier_experience = df[df['years_experience'] > 50]

    if not outlier_experience.empty:
        print("The following ids have more than 50 years of experience:")
        print(outlier_experience['professional_id'].to_list())

    # Professionals dimension and fact tables
    conn.execute("""CREATE TABLE dim_professionals AS 
                        SELECT 
                            professional_id,
                            years_experience,
                            current_industry,
                            current_role,
                            education_level
                        FROM df                    
                """)


# Processes the skills data entity and creates relevant tables in duckdb
def process_skills(input_data):
    df = pd.json_normalize(input_data, 'skills', meta=['professional_id', 'years_experience'], record_prefix='skill_')

    # Data cleaning/transformation
    # N/A

    # Data quality checks
    # Checking if any skills have more experience listed than total experience.
    # Assumption: only years of professional experience (skills learned on the job) are counted
    invalid_experience = df[df['skill_years_experience'] > df['years_experience']]

    if not invalid_experience.empty:
        print("The following ids have more years of skills experience than total experience:")
        print(invalid_experience['professional_id'].unique())

    # Skills dimension and fact tables
    conn.execute("""CREATE TABLE dim_skills AS
                        SELECT
                            skill_skill_id          AS skill_id,
                            skill_skill_name        AS skill_name,
                            skill_proficiency_level AS proficiency_level
                        FROM df
                """)

    conn.execute("""CREATE TABLE fact_skills AS
                        SELECT
                            skill_skill_id          AS skill_id,
                            professional_id         AS professional_id,
                            skill_years_experience  AS years_experience
                        FROM df
                """)


# Processes the certifications data entity and creates relevant tables in duckdb
def process_certifications(input_data):
    df = pd.json_normalize(input_data, 'certifications', meta='professional_id', record_prefix='cert_')

    # Data cleaning/transformation
    # Convert dates from string to datetime and change invalid dates to NULL
    # For this assignment default to NULL is used. In a real pipeline an error would be raised and
    # the dev team would be notified
    df["cert_date_earned"] = pd.to_datetime(df['cert_date_earned'], format="%Y-%m-%d", errors='coerce')
    df["cert_expiration_date"] = pd.to_datetime(df['cert_expiration_date'], format="%Y-%m-%d", errors='coerce')

    # Data quality checks
    invalid_cert_dates = df[df['cert_date_earned'] > df['cert_expiration_date']]

    if not invalid_cert_dates.empty:
        print("Certification date_earned is later than cert_expiration_date for the following ids:")
        print(invalid_cert_dates['professional_id'].unique())

    # Certifications dimension and fact tables
    conn.execute("""CREATE TABLE dim_certifications AS
                        SELECT
                            cert_certification_id       AS certification_id,
                            cert_certification_name     AS certification_name,
                            cert_issuing_organization   AS issuing_organization
                        FROM df
                """)

    conn.execute("""CREATE TABLE fact_certifications AS
                        SELECT
                            cert_certification_id   AS certification_id,
                            professional_id         AS professional_id,
                            cert_date_earned        AS date_earned,
                            cert_expiration_date    AS expiration_date
                        FROM df
                """)


# Processes the jobs data entity and creates relevant tables in duckdb
def process_jobs(input_data):
    df = pd.json_normalize(input_data, 'jobs', meta='professional_id', record_prefix='job_')

    # Data cleaning/transformation
    # Convert dates from string to datetime and change invalid dates to NULL
    # For this assignment default to NULL is used. In a real pipeline an error would be raised and
    # the dev team would be notified
    df["cert_date_earned"] = pd.to_datetime(df['job_start_date'], format="%Y-%m-%d", errors='coerce')
    df["cert_expiration_date"] = pd.to_datetime(df['job_start_date'], format="%Y-%m-%d", errors='coerce')

    # Data quality checks
    invalid_job_dates = df[df['job_start_date'] > df['job_end_date']]

    if not invalid_job_dates.empty:
        print("Job start_date is later than end_date for the following ids:")
        print(invalid_job_dates['professional_id'].unique())

    # Jobs dimension and fact tables
    conn.execute("""CREATE TABLE dim_jobs AS
                        SELECT
                            job_job_id      AS job_id,
                            job_company     AS company,
                            job_industry    AS industry,
                            job_role        AS role
                        FROM df
                """)

    conn.execute("""CREATE TABLE fact_jobs AS
                        SELECT
                            job_job_id      AS job_id,
                            professional_id AS professional_id,
                            job_start_date  AS start_date,
                            job_end_date    AS end_date,
                            job_salary_band AS salary_band
                        FROM df
                """)


# Processes the education data entity and creates relevant tables in duckdb
def process_education(input_data):
    df = pd.json_normalize(input_data, 'education', meta='professional_id', record_prefix='edu_')

    # Data cleaning/transformation
    # Convert dates from string to datetime and change invalid dates to NULL
    # For this assignment default to NULL is used. In a real pipeline an error would be raised and
    # the dev team would be notified
    df["edu_graduation_date"] = pd.to_datetime(df['edu_graduation_date'], errors='coerce')

    # Data quality checks
    # N/A

    # Education dimension and fact tables
    conn.execute("""CREATE TABLE dim_education AS
                        SELECT
                            edu_education_id    AS education_id,
                            edu_degree          AS degree,
                            edu_institution     AS institution,
                            edu_field_of_study  AS field_of_study
                        FROM df
                """)

    conn.execute("""CREATE TABLE fact_education AS
                        SELECT
                            edu_education_id    AS education_id,
                            professional_id     AS professional_id,
                            edu_graduation_date AS graduation_date
                        FROM df
                """)


# Main pipeline function
def professionals_data_pipeline(path):
    # Load the data into a json object
    professionals = load_data(path)

    # Professionals data entities
    process_professionals(professionals)

    # Skills data entities
    process_skills(professionals)

    # Certification data entities
    process_certifications(professionals)

    # Jobs data entities
    process_jobs(professionals)

    # Education data entities
    process_education(professionals)

    # A collection of sample queries for testing
    # conn.sql("SELECT * FROM fact_certifications").show(max_width=1000, max_rows=1000)
    # conn.sql("SELECT * FROM dim_certifications").show(max_width=1000, max_rows=1000)
    # conn.sql("SELECT * FROM fact_jobs").show(max_width=1000, max_rows=1000)
    # conn.sql("SELECT * FROM dim_jobs").show(max_width=1000, max_rows=1000)
    # conn.sql("SELECT * FROM fact_education").show(max_width=1000, max_rows=1000)
    # conn.sql("SELECT * FROM dim_education").show(max_width=1000, max_rows=1000)
    # conn.sql("SELECT * FROM dim_skills").show(max_width=1000, max_rows=1000)
    # conn.sql("SELECT * FROM fact_skills").show(max_width=1000, max_rows=1000)
    # conn.sql("SELECT * FROM dim_professionals").show(max_width=1000, max_rows=1000)


# Run the pipeline with the JSON file path
if __name__ == '__main__':
    # Read config file
    with open('config.json', 'r') as c:
        json_data = json.load(c)

    # Grab file path value
    file_path = json_data['file_path']

    # Run the pipeline
    professionals_data_pipeline(file_path)
