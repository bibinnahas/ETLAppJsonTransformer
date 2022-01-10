# Overview on Module
![alt text](https://github.com/bibinnahas/ETLAppJsonTransformer/blob/master/src/main/resources/WF_Hipages.png)

Users interact with web pages that creates certain events (interactions being actions like page view, button click, list directory, claim etc). Each such event is represented by a json line in the input file. All these json lines are captured by this application, which then transforms it for better readability, understanding and decision making. Finally the transformed data is loaded in a structured table format (including malformed ones that can be opted to be written to a separate path).   
# Problem Statement
- Read input json lines from the events.json
- Apply necessary validations (Type check, max-min permissible values etc) from the schema.json
- Apply transformations
   * Take substring of full url and segregate into different url levels
   * Rename columns as required in expected output
   * Aggregate user count and activity count on a time bucket level (on hourly buckets)
- Save the transformed data in a structured table format (csv)
- Optionally write malformed and invalid json line to a separate path
# ETL Functions in Module
- **Extract**
  * Read json from Input path
  * Cleanse and validate the json
  * Create dataframes by applying schema validation
- **Transformations**
  * Extract required columns
  * Rename columns as required
  * Takes substring of 'full url' for level 1 url values
  * Applies aggregation over user and activities
- **Load**
  * Saves the activity report and activity-user counts report as a CSV's
  * Optionally save malformed/invalid output as csv or text
# Prerequisites
The Application in its current state assumes the below
- The input events json and schema json are available locally where the application runs
- Output structured data is written to the same environment where the execution was triggered
- Docker, Spark, Git and jdk are setup and configured in environment
- The various versions used while development are as below
    ```
    scala  - v2.12.11
    spark  - v3.0.0
    sbt    - v1.3.8
    docker - v19.03.8
    ```
# How to Run
### Build/Setup Project
Use the below commands to build the project
- Go to the folder where the project is to be set up
   ```
   git clone https://github.com/bibinnahas/ETLAppJsonTransformer.git
   ```
- Go to the project folder
   ```
   cd ETLAppJsonTransformer
   ```
- Run sbt clean compile and assembly to package the jar
   ```
   sbt clean compile
   sbt assembly
   ```
- If successful, the jar should be in the below path
   ```
   ls target/scala-2.12/JsonTransformerETL-assembly-1.0.jar
   ```
- Copy jar to "jars" folder in project structure
   ```
   mkdir -p jars && cp target/scala-2.12/JsonTransformerETL-assembly-1.0.jar jars/ && chmod -R 777 jars
   ```
### Running Application (with default parameters)
- Post setting up and successfully packaging the application, use either of the 2 commands to execute
    ```
    ./deploy_app_in_docker.sh
    ```
- **OR** alternatively, use nohup command to make it run in background
    ```
    nohup ./deploy_app_in_docker.sh >> delpoy_app.log &
    ```
- **Check output in ./data/output/**
- To change parameters, update docker-compose.yml as explained below
### All ways to run the application
#### Method 1 - Using Docker Containers (Preferred option and explained above)
1. To change default arguments to docker, update "command" in docker-compose.yml with input path, target path, schema path and validate/no arguments. 
    ```
    command:
          [
            "$SPARK_HOME/bin/spark-submit",
            "--class", "com.hipages.jsontransformer.entry.ETLMainApp",
            "/opt/jars/JsonTransformerETL-assembly-1.0.jar",
            "/opt/input/source_event_data.json", #change input here
            "/usr/data/output/", #change output folder here
            "/opt/schema/source_data_schema.json", #change schema path here
            "validate" #change to no to skip file validation
          ]
    ```
2. Run the below script to start docker application
    ```
    ./deploy_app_in_docker.sh
    ```
3. This triggers the spark job in a container, writes the output to disk and shuts down gracefully. Validate output in ./data/output/
#### Method 2 - Using Shell Script
1. Build the project as explained in the section [Build Project](https://github.com/bibinnahas/ETLAppJsonTransformer#buildsetup-project) 
2. Execute run_job.sh. This shell script internally calls a spark-submit job.
    ```
   chmod 777 ./scripts/run_job.sh
   sudo ./scripts/run_job.sh jars/JsonTransformerETL-assembly-1.0.jar data/input/source_event_data.json data/output schema/source_data_schema.json no
   ```
3. Check for output in ./data/output/ folder 
#### Method 3 - Using Spark Submit
1. Build the project as explained in the section [Build Project](https://github.com/bibinnahas/ETLAppJsonTransformer#buildsetup-project)
2. Using spark-submit command on a spark cluster (Assumes $SPARK_HOME is set)
   ```
   sudo $SPARK_HOME/bin/spark-submit --class com.hipages.jsontransformer.entry.ETLMainApp --deploy-mode client --master local[*] jars/HipagesJsonTransformerETL-assembly-1.0.jar data/input/source_event_data.json data/output schema/source_data_schema.json no
   ```
3. Check for output in ./data/output folder
### Details about rguments to docker-compose/spark-submit
- **Input File Path** - Source_event_data.json (events json created from User Action Events)
- **Output File Path** - Target path for saving the structured output tables
- **Schema File Path** - Source_data_schema.json (Json schema file path)
- **Validate function** - Supply a fourth argument as "validate" or "no". Fourth argument keyed in as  "validate", will give a detailed picture on any errors in the input json. Errors include, but is not limited to
   * Malformed Json
   * Datatyping errors
   * Incomplete/Corrupt json etc
### Data Samples
- Input
    ```
    { "event_id" : "893479324983546", "user" : { "session_id" : "564561", "id" : 56456 , "ip" : "111.222.333.4" }, "action" : "page_view", "url" : "https://www.hipages.com.au/articles", "timestamp" : "02/02/2017 20:22:00"}
    { "event_id" : "349824093287032", "user" : { "session_id" : "564562", "id" : 56456 , "ip" : "111.222.333.5" }, "action" : "page_view", "url" : "https://www.hipages.com.au/connect/sfelectrics/service/190625", "timestamp" : "02/02/2017 20:23:00"}
    { "event_id" : "324872349721534", "user" : { "session_id" : "564563", "id" : 56456 , "ip" : "111.222.33.66" }, "action" : "page_view", "url" : "https://www.hipages.com.au/get_quotes_simple?search_str=sfdg", "timestamp" : "02/02/2017 20:26:00"}
    { "event_id" : "213403460836344", "user" : { "session_id" : "564564", "id" : 56456 , "ip" : "111.222.3.77" }, "action" : "button_click", "url" : "https://www.hipages.com.au/advertise", "timestamp" : "01/03/2017 20:21:00"}
    { "event_id" : "235487204723104", "user" : { "session_id" : "564565", "id" : 56456 , "ip" : "111.22.22.4" }, "action" : "page_view", "url" : "https://www.hipages.com.au/photos/bathrooms", "timestamp" : "02/02/2017 20:12:34"}
    { "event_id" : "348696806978456", "user" : { "session_id" : "564566", "id" : 56456 , "ip" : "111.222.33.4" }, "action" : "list_directory", "url" : "https://www.hipages.com.au/find/electricians", "timestamp" : "01/01/2017 20:22:00"}
    ```
- Output
  - User - Activity table
    ```
    user_id,time_stamp,url_level1,url_level2,url_level3,activity
    56456,02/02/2017 20:26:00,hipages.com,get_quotes_simple?search_str=sfdg,"",page_view
    56456,01/03/2017 20:21:00,hipages.com,advertise,"",button_click
    ```
  - User - Activity Counts aggregated table
    ```
    time_bucket,url_level1,url_level2,activity,activity_count,user_count
    2017030100,hipages.com,advertise,button_click,1,1
    2017020200,hipages.com,get_quotes_simple?search_str=sfdg,page_view,1,1
    ```
# Possible Future Enhancements
Given more time, we can think of adding the below functionalities
- Support for more input file formats like csv, xml etc. The ingestion method should be enhanced to accept file types with or without a schema.
- Ability to read and write files from/to S3
- Ability to write structured data to a relational DB from docker
- Additional configs for dev, test, preprod and prod env set ups
- A dedicated scheduler to run the job like Airflow or Autosys
- Supplying Dataframe queries as separate parameterised configurable sql files so that any person with sql skills may modify sql
- Create an additional conf.yml file which can be used to supply the parameters, create a docker-compose.yml on the fly, run docker and fetch results. This will make support and execution easier.
- Set up multi executioner using k8s 
# Portability and Scaling
- The v1.0 version is deployed using spark running on docker. The same application is also designed to run on a multi node cluster
- Dockerizing the container enables easy deployment and makes the application highly portable
- Spark environments can be configured to ensure great scalability and availability.

##### PS: Final commit done on 16 March 2021
- renamed project
- hence renamed jar in deployment instructions and docker-compose
