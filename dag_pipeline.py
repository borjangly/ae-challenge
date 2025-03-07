import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import duckdb
import pandas as pd
import gc

# Connect to DuckDB
conn = duckdb.connect(database=':memory:')  # In-memory database for simplicity in this assignment

# Paths and Config
# For actual production Airflow, this would either be an environment variable or be located in airflow.cfg
CONFIG_PATH = 'config.json'

# Extract the json data from file
with open(CONFIG_PATH, "r") as f:
    json_data = json.load(f)

professionals_data = json_data['professionals']


# Processes the professionals data entity and creates relevant tables in duckdb
def process_professionals(input_data):
    df = pd.json_normalize(input_data)
    df['last_updated_time'] = datetime.now()

    # Data quality checks
    # Checking for outliers in data. I considered 50 years the break point for an outlier in job experience
    outlier_experience = df[df['years_experience'] > 50]

    if not outlier_experience.empty:
        print("The following ids have more than 50 years of experience:")
        print(outlier_experience['professional_id'].to_list())

    # Professionals dimension and fact tables
    conn.execute("""INSERT OR REPLACE INTO dim_professionals (
                            professional_id, 
                            years_experience, 
                            current_industry, 
                            current_role, 
                            education_level, 
                            last_updated_time
                        )
                        SELECT 
                            professional_id,
                            years_experience,
                            current_industry,
                            current_role,
                            education_level,
                            last_updated_time
                        FROM df              
                        WHERE last_updated_time > {{ prev_start_date_success }}       
                """)

    gc.collect()


# Processes the skills data entity and creates relevant tables in duckdb
def process_skills(input_data):
    df = pd.json_normalize(input_data, 'skills', meta=['professional_id', 'years_experience'], record_prefix='skill_')
    df['last_updated_time'] = datetime.now()

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
    conn.execute("""INSERT OR REPLACE INTO dim_skills (
                            skill_id, 
                            skill_name, 
                            proficiency_level,
                            last_updated_time
                        )
                        SELECT
                            skill_skill_id          AS skill_id,
                            skill_skill_name        AS skill_name,
                            skill_proficiency_level AS proficiency_level,
                            last_updated_time
                        FROM df              
                        WHERE last_updated_time > {{ prev_start_date_success }}    
                """)

    conn.execute("""INSERT OR REPLACE INTO fact_skills (
                            skill_id, 
                            professional_id, 
                            years_experience,
                            last_updated_time
                        )
                        SELECT
                            skill_skill_id          AS skill_id,
                            professional_id         AS professional_id,
                            skill_years_experience  AS years_experience,
                            last_updated_time
                        FROM df              
                        WHERE last_updated_time > {{ prev_start_date_success }}    
                """)

    gc.collect()


# Processes the certifications data entity and creates relevant tables in duckdb
def process_certifications(input_data):
    df = pd.json_normalize(input_data, 'certifications', meta='professional_id', record_prefix='cert_')
    df['last_updated_time'] = datetime.now()

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
    conn.execute("""INSERT OR REPLACE INTO dim_certifications (
                            certification_id, 
                            certification_name, 
                            issuing_organization,
                            last_updated_time
                        )
                        SELECT
                            cert_certification_id       AS certification_id,
                            cert_certification_name     AS certification_name,
                            cert_issuing_organization   AS issuing_organization,
                            last_updated_time
                        FROM df
                        WHERE last_updated_time > {{ prev_start_date_success }}  
                """)

    conn.execute("""INSERT OR REPLACE INTO fact_certifications (
                            certification_id, 
                            professional_id, 
                            date_earned,
                            expiration_date,
                            last_updated_time
                        )
                        SELECT
                            cert_certification_id   AS certification_id,
                            professional_id         AS professional_id,
                            cert_date_earned        AS date_earned,
                            cert_expiration_date    AS expiration_date,
                            last_updated_time
                        FROM df
                        WHERE last_updated_time > {{ prev_start_date_success }}  
                """)

    gc.collect()


# Processes the jobs data entity and creates relevant tables in duckdb
def process_jobs(input_data):
    df = pd.json_normalize(input_data, 'jobs', meta='professional_id', record_prefix='job_')
    df['last_updated_time'] = datetime.now()

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
    conn.execute("""INSERT OR REPLACE INTO dim_jobs (
                            job_id, 
                            company, 
                            industry,
                            role,
                            last_updated_time
                        )
                        SELECT
                            job_job_id      AS job_id,
                            job_company     AS company,
                            job_industry    AS industry,
                            job_role        AS role,
                            last_updated_time
                        FROM df
                        WHERE last_updated_time > {{ prev_start_date_success }}  
                """)

    conn.execute("""INSERT OR REPLACE INTO fact_jobs (
                            job_id, 
                            professional_id, 
                            start_date,
                            end_date,
                            salary_band,
                            last_updated_time
                        )
                        SELECT
                            job_job_id      AS job_id,
                            professional_id AS professional_id,
                            job_start_date  AS start_date,
                            job_end_date    AS end_date,
                            job_salary_band AS salary_band,
                            last_updated_time
                        FROM df
                        WHERE last_updated_time > {{ prev_start_date_success }}  
                """)

    gc.collect()


# Processes the education data entity and creates relevant tables in duckdb
def process_education(input_data):
    df = pd.json_normalize(input_data, 'education', meta='professional_id', record_prefix='edu_')
    df['last_updated_time'] = datetime.now()

    # Data cleaning/transformation
    # Convert dates from string to datetime and change invalid dates to NULL
    # For this assignment default to NULL is used. In a real pipeline an error would be raised and
    # the dev team would be notified
    df["edu_graduation_date"] = pd.to_datetime(df['edu_graduation_date'], errors='coerce')

    # Data quality checks
    # N/A

    # Education dimension and fact tables
    conn.execute("""INSERT OR REPLACE INTO dim_education (
                            education_id, 
                            degree, 
                            institution,
                            field_of_study,
                            last_updated_time
                        )
                        SELECT
                            edu_education_id    AS education_id,
                            edu_degree          AS degree,
                            edu_institution     AS institution,
                            edu_field_of_study  AS field_of_study,
                            last_updated_time
                        FROM df
                        WHERE last_updated_time > {{ prev_start_date_success }}  
                """)

    conn.execute("""INSERT OR REPLACE INTO fact_education (
                            education_id, 
                            professional_id, 
                            graduation_date,
                            last_updated_time
                        )
                        SELECT
                            edu_education_id    AS education_id,
                            professional_id     AS professional_id,
                            edu_graduation_date AS graduation_date,
                            last_updated_time
                        FROM df
                        WHERE last_updated_time > {{ prev_start_date_success }}  
                """)

    gc.collect()


# Define a DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,  # 3 retries in case of network issues
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2015, 6, 1),
    'email_on_failure': True,
    'email': 'wxtan@example.com'
}

dag = DAG(
    'career_analytics_data_pipeline',
    default_args=default_args,
    description='Sample data pipeline for career analytics',
    schedule_interval=timedelta(minutes=60),  # Run every hour for this sample
    catchup=False
)

# Task for processing professionals
process_professionals_task = PythonOperator(
    task_id='process_professionals',
    python_callable=process_professionals,
    op_args=[professionals_data],
    dag=dag
)

# Task for processing skills
process_skills_task = PythonOperator(
    task_id='process_skills',
    python_callable=process_skills,
    op_args=[professionals_data],
    dag=dag
)

# Task for processing certifications
process_certifications_task = PythonOperator(
    task_id='process_certifications',
    python_callable=process_certifications,
    op_args=[professionals_data],
    dag=dag
)

# Task for processing jobs
process_jobs_task = PythonOperator(
    task_id='process_jobs',
    python_callable=process_jobs,
    op_args=[professionals_data],
    dag=dag
)

# Task for processing education
process_education_task = PythonOperator(
    task_id='process_education',
    python_callable=process_education,
    op_args=[professionals_data],
    dag=dag
)

# Define task steps and dependencies
# As loading the data into a json object is done as part of the base file, no dependent steps are required
# and all tasks can run concurrently
[process_professionals_task, process_skills_task, process_certifications_task, process_jobs_task,
 process_education_task]
