# Design and implement a data intensive system to visualize berlin weather 

## Services
This application is designed to ingest streaming Berlin-weather data, preprocess them, transform them and eventually visualize them.
all using containerized microservices to ensure reliability, scalability, maintainability and above that, security and governance.
The system is built using 6 services:
   NAME            Docker-image                Role
1. Zookeeper       wurstmeister/zookeeper      kafka management 
2. Kafka           wurstmeister/kafka          Real-time data streaming
3. Producer        manually built              Data producing
4. Spark-app       manually built              Data Preprocessing and sending
5. cassandra       cassandra                   Database
6. grafana         grafana/grafana-oss         Visualization

All images are pulled from the Docker-Hub except for the producer and spark-app. The producer is responsible for fetching data streams 
from https://open-meteo.com/, and sending them to the weather-topic topic in kafka. The spark-app on the other side, is the only consumer,
and its job is first to read these streams and transform them using spark streming, store them in cassandra to be ultimately visualized using
grfana.



## Using Grafana
Once you lunch the app using docker-compose up, you can access the intended visualizations using grafana.
1. To do this, go to http://localhost:3000/ to access the GUI on your system.
2. you will be asked for the user and password: Enter user:Admin, password: Admin
3. When the home page open, click on "add your first data scource" to add the required plugin.
4. search for and add hadesarchitect-cassandra-datasource.
5. To connect,