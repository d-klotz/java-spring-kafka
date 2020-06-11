<h1 align="center">Kafka producer/consumer application</h1>

## :electric_plug: Requirements

- Java 8
- Maven
- Kafka

# :closed_lock_with_key: Initital Instructions
### Installing Kafka
In order to run the project, you will need to have Kafka and Kafka Zookeeper running.

```shell
# In Kafka root directory run the command below to start kafka
$ kafka-server-start.bat config\server.properties
```

```shell
# In Kafka root directory run the command below to start Zookeeper
$ zookeeper-server-start.bat config\zookeeper.properties
```

```shell
# In Kafka root directory run the command below to create a topic with partitions and replication
$ kafka-topics.bat --zookeeper 127.0.0.1:2181 --topic <name_of_your_topic> --create --partitions <you_number_of_partitions> --replication-factor 1
```

Clone this repository and install all dependencies.

```shell
# Install all dependencies using Maven
$ mvn install
```

<hr />

### <a href="http://linkedin.com/in/danielfelipeklotz">Contact me on LinkedIn</a>
