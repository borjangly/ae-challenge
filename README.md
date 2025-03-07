# Part 1 - Data Pipeline `pipeline.py`

## Instructions

1. Update the file_path value in config.json to the location of your choosing
2. Either run pipeline.py from an IDE of your choice or run `python pipeline.py` in terminal/command line

## Design approach

### Pipeline

* I decided to group each entity into its own processing function for ease of readbility in this exercise

### Data models
![Data model](/datamodel.png "Data model")
* Traditional fact-dimension setup
* Decided to create smaller fact tables for each data entity (skills, certs, jobs, education) for more granular detail and for better query performance when looking up individual entities
  * This will increase query complexity at the analytics layer (more joins) but I decided to use this structure over a larger fact table to ensure data quality
  * Lower risk when modifying schemas for versioning and evolution
* I assumed years_experience to be a dimension in this exercise but in a real pipeline this value would probably be derived as a calculation of `current_date - earliest_job_start_date` or summing all job date periods

## Notes

* For simplicity, no errors are raised and instead default values or print statements are used in this sample
* I assumed `current_role` to be a non-calculated dimension that does not require validation
  * You can see P001 has `current_role = Data Scientist` but he left his most recent Data Scientist role in `2023-02-28` (J004)
* Explicit garbage collection (`import gc` and `gc.collect()`) was not considered for this exercise

## Productionization 
* Database schemas would be created and managed independently of any pipeline.
  * The insertion portions of the pipeline would then be `INSERT INTO {table}...` instead of `CREATE TABLE {table}...`
  * Id columns would act as Primary or Joint/Foreign keys
* An errors table should be created in the pipeline
  * Any record that fails validation should be inserted into this table and the pipeline can continue uninterrupted
  * Error notifications would still be sent to the relevant parties
* `config.json` (or some config file system) should be expanded to include any changing scalar values
* Pipeline functions would be generalized and partitioned into different files
  * Following the pipeline created for this exercise, each data entity function e.g. `process_skills` would be a separate python file
  * This makes maintenance and enhancing functionality easier and less risky (no chance of messing up the jobs entities if you're only working on a skills entity file)
* Validation functions or generic functions like date converters should be encapsulated in distinct classes that can be invoked per file
  * Again for maintenance purposes and ease of development

# Part 2 - Requirements Gathering & Problem Space Analysis

## 1 - Clarifying questions

### What defines career growth?
Career growth is a vague term by itself. It would be beneficial to define career growth more concretely function of specific metrics or terms

### Is there any sort of dataset partitioning required?
For example, should the factors be grouped by certain demographic markers or education levels or location

### How are industries defined in this problem?
i.e. a software engineer at Nike is technically the tech industry, however company operates in the retail sector and they may prefer to stay in retail

## 2 - Key Metrics

### Salary
Tracking salary (band) growth over time is a generally accepted indicator of career growth

### Certifications
Skills are also valuable to growing a career however they are more subjectively categorized; your idea of "advanced python skills" may not be the same as mine.

Certifications are an objective measurement point and certifications can also be tracked back to issuing organizations for additional analysis/benchmarking on the value of a certification from a specific org.

### Job titles
For each industry, sector or even company, a rough job progression path can be created and used as a benchmark to measure individual career growth against.

### Education level
For some industries education level can present either a growth opportunity or a barrier-to-entry i.e. in academia to be a full-fledged professor you need to have completed a PhD.

## 3 - How the data model ties in
Using the data model designed in part 1, we can easily create analytics that observe key metrics like salary and job titles over time. Additionally, we can factor in more subjective factors like skills, certifications and education level.

## 4 - Additional data sources

### Job satisfaction data
This can help explain trends in companies or industries where individuals change jobs frequently. There's also a correlation between job satisfaction and job title changes.

### External education data
For some industries, graduating from reputable universities can help in jumpstarting a career. Understanding the role played by this can improve analysis of education as a factor in career growth.

### Demographic data
Demographic markers like age, gender, race and location can help increase accuracy of any modeling i.e. assuming two fresh grads from the same school, a 25-year-old woman may have a different career trajectory compared to a 50-year-old man (note: discrimination based on demographics by employers is illegal but these trends may still appear).

### Economic models or benchmarks
Metrics like the number of available jobs or salaries may fluctuate based on the overall performance of the economy. Adding benchmarks like the S&P 500 or the NASDAQ Composite may further help to explain trends in career trajectories and predict future growth.

# Part 3 - Airflow `dag_pipeline.py`

## Design approach

### Conversion from raw python script to Airflow
* Instead of wrapping the load portion into a function like in the data pipeline, I opted to script it just for convenience not having to use XCOM triggers to move data between tasks
* Added garbage collection `gc.collect()` as the computational resourcing in this Airflow instance is not that high
* Some configurations were estimated. All of these configurations would depend on the overall design of the pipeline.
  * The number of retries and retry delays
  * Schedule interval

### Incremental data/refreshes
Using DuckDB, I opted for the primitive method of `INSERT OR REPLACE INTO` which would update fields when matched on joint key columns (the `___id` columns). 

The `last_updated_time` columns is a new addition to the dataframes which will be used as a marker to compare against the last successful run in Airflow ``{{ prev_start_date_success }}``

In a production setting, it may be more computationally efficient to have an in-between staging area to deposit all incoming data. This data can then be cleaned and validated for incremental refreshes before being pushed to the final analytic database.

## Productionization of Airflow

### Resource allocation
With the allocation described in the challenge instructions, it would be a good idea to ensure limits are placed on the number of concurrent tasks and DAGs to prevent resource usage errors

With 1 vCPU each for workers and schedulers, we would want to cap the number of concurrent tasks in the `airflow.cfg` config.

Some examples:
```
worker_concurrency = 3
parallelism = 15
max_active_tasks_per_dag = 15
max_active_runs_per_dag = 15
dag_file_processor_timeout = 60
dagbag_import_timeout = 60
```
These are just estimated values and the most accurate value would depend on the workload

### Breaking down tasks
Tasks can be encapsulated in separate files. Similar to the logic in the original pipeline, this allows for easier development work and maintenance. Individual DAGs can be triggered by the `TriggerDagRunOperator`

Example
```
trigger_skills_dag = TriggerDagRunOperator(
        task_id="process_skills_dag",
        trigger_dag_id="process_skills"
    )

...

trigger_load_data_dag >> trigger_professionals_dag >> trigger_skills_dag trigger_certifications_dag >> trigger_jobs_dag >> trigger_education_dag
```

### Scaling considerations/Migration of calculation work to external applications
If the size of the data scales too large, I would prefer to move all computational work (dataframes/SQL statements) into external applications. Airflow would be used purely as orchestration in this situation and we could use `BashOperator` to execute a Python script or an operator to run a job on a database e.g. `BigQueryInsertJobOperator` or `PosgresOperator`

This will reduce the amount of resourcing allocated to computational work, freeing up resources for Airflow to be able to orchestrate more tasks and any given time.
