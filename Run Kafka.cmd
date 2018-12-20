
REM Start zookeeper
C:\zookeeper-3.4.12\bin\zkServer.cmd

REM Start Kafka
C:\kafka_2.11-2.10\bin\windows\kafka-server-start-bat "C:\kafka_2.11-2.10\config\server.properties"

REM Start consumer Kafka
C:\kafka_2.11-2.10\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test

