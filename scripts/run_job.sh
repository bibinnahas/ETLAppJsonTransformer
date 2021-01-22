#!/bin/sh

jarPath=$1
inpPath=$2
targetPath=$3
schemaPath=$4
validate=$5


if [ $# -ne 5 ]
  then
    echo "Usage: <input_path> <target_folder> <schema_file_path> <validate or no>"
    exit 1
  else
    /opt/spark/bin/spark-submit --class com.hipages.jsontransformer.entry.ETLMainApp --deploy-mode client --master local[*] $jarPath $inpPath $targetPath $schemaPath $validate
fi

