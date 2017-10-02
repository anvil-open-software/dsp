dlab
====

Dematic Labs sandbox project for creating a generic Data Streaming Pipeline. 
Technologies include Kafka for data ingestion, Spark and Spark Streaming for 
data processing, and Cassandra for data storage.

Project will generate two executable jars: 

drivers-${project.version-dsp.jar -- a self contained jar that includes all the drivers. Drivers are deployed within a Spark environment.
Spark drivers are configured using a driver specific configuration file.

simulators-${project.version}-dsp.jar -- a self contained jar that includes all the simulators. Simulators are 
configured using a configuration file. See throughput.conf for an example. To run a simulator
run the following command.

java -cp simulators-${project.version}-dsp.jar -Dconfig.file=/pathToFile/throughput.conf com.dematic.labs.dsp.simulators.Throughput

