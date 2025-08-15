#Base image.
FROM python:3.11-slim

#Set the working directory in the container.
WORKDIR /app

#Install  required dependencies.
RUN pip install --no-cache-dir Kafka-python requests datetime uuid numpy

#Copy the python code in the container.
COPY producer.py .

#Default command when running the producer container.
CMD ["python","producer.py"]