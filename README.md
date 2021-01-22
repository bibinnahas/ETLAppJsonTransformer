# Overview on Module
![alt text](https://github.com/bibinnahas/ETLAppJsonTransformer/blob/master/src/main/resources/WF_Hipages.png)

Users interact with web pages that creates certain events (interactions being actions like page view, button click, list directory, claim etc). Each such event is represented by a json line in the input file. All these json lines (including malformed ones that can be opted to be written to a separate path) are captured by this application, which then applies certain transformations to make it readable for better understanding and decision making. This structured data is saved in a target path in the same environment.   
# Problem Statement
- Read input json lines from the events json
- Apply necessary validations (Datatype, max-min values permissible etc) from the schema.json
- Apply transformations
   * Take substring of full url and segregate into different levels
   * Rename columns as required in expected output
   * Aggregate user count and activity count on a time bucket level (on hourly buckets)
- Save the transformed data in a structured table format (csv)
- Optionally write malformed and invalid json line to a separate path
# Assumptions
The Application in its current state assumes the below
- The input events json and schema json are available locally where the application runs
- Output strcutured data is written in to the same system
# ETL Functions in Module
- **Extract**
  * Read json from Input path
  * Cleanses data using supplied schema.json
  * And creates dataframes by applying schema validation
- **Transformations**
  * Extract required columns
  * Rename them from original json keys
  * Takes substring of full url for level 1 url values
  * Applies aggregation over user and activities
- **Load**
  * Save the output as a CSV locally
  * Optionally save malformed/invalid output as csv or text
## Arguments
- **Input File Path** - Source_event_data.json (events json created from User Action Events)
- **Output File Path** - Target path for saving the structured output user activity tables
- **Schema File Path** - Source_data_schema.json (Json schema file path)
- **validate function** - Supply a fourth argument as "validate" or "no". Fourth argument keyed in as  "validate", will give a detailed picture on any errors in the input json. Errors include, but is not limited to
   * Malformed Json
   * Datatyping errors
   * Incomplete/Corrupt json etc
# Instructions on Execution
Can be executed via below 3 means
- Using run_job.sh - The run_job is present in the scripts folder.
  * Usage: **./run_job.sh <path_to_jar> <path_to_input_json> <path_to_target_folder> <path_to_schema_json> <validate/no>**
- Using Spark submit - Directly calling the spark job. (Assumes SPARK_HOME is set in your environment)
  * Usage: **./bin/spark-submit --class com.hipages.jsontransformer.entry.ETLMainApp --deploy-mode client --master local[*] <path_to_HipagesJsonTransformerETL-assembly-1.0.jar> <path_to_input_json> <path_to_target_folder> <path_to_schema_json>**
- 
# Possible Future Enhancements
Given more time, we can think of adding the below functionalities
- Support for more input file formats like csv, xml etc. The ingestion method should we enhanced to accept file types with or without a schema.
- Deploy and Dockerize the spark job using Kubernetes. Implement a job deployment in K8s or schedule the deployment as a cron job
- Ability to read and write files from/to S3
- Ability to write structured data to a relational DB from K8s
- Additional configs for dev, test, preprod and prod env set ups
- A dedicated scheduler to run the job like Airflow or Autosys
- Supplying Dataframe queries as separate parameterised configurable sql files so that any person having sql skills can work on Big Data 
# Portability and Scaling
The Application is running on Spark which makes itself a highly scalable app with the correct ideal set up. In addition, implementing the same app on k8s will introduce additional benefits of availability, scalability and ease of deployment
