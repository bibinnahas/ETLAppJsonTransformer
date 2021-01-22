#!/bin/sh

# Arguments to the spark submit job
jarPath=$1
inpPath=$2
targetPath=$3
schemaPath=$4
validate=$5

chmod -R 777 $inpPath
chmod -R 777 $targetPath
chmod -R 777 $schemaPath
chmod -R 777 $jarPath

# If all arguments supplied, execute the spark sumit command
# UPDATE SPARK_HOME if in a different path
if [ $# -ne 5 ]
  then
    echo "Usage: <input_path> <target_folder> <schema_file_path> <validate or no>"
    exit 1
  else
    /opt/spark/bin/spark-submit --class com.hipages.jsontransformer.entry.ETLMainApp --deploy-mode client --master local[*] $jarPath $inpPath $targetPath $schemaPath $validate
fi

