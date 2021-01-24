FROM mesosphere/spark:2.12.0-3.0.1-scala-2.12-hadoop-3.2

COPY jars /opt/
COPY input /opt/
COPY schema /opt/
RUN mkdir -p /usr/data/output
RUN chmod -R 777 /usr/data
RUN chmod -R 777 /usr/data/output

