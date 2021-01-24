#!/bin/sh

# The below command removes all images to save disk space
for i in `docker images --format "{{.ID}}" --all`; do docker rmi $i --force; done
