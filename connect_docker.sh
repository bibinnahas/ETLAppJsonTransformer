#!/bin/sh

# Arguments to the spark submit job
dockerName=$1

docker build . -f Dockerfile -t $dockerName
docker run --network host -it $dockerName /bin/bash
