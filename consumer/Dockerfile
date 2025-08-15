#Base image
FROM bitnami/spark:3.5.0

#Set the working directory in the container
WORKDIR /app

#Copy the python code in the container.
COPY streaming.py /app/

#Install required dependencies.
RUN pip install cassandra-driver 

#Default command when running the spark-app container
# Download dependencies, start spark application, enable spark to read from and write to kafka and cassandra, 
CMD ["spark-submit","--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0","/app/streaming.py"]



