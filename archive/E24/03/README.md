# Lecture 03 - Distributed Transport and Streaming

## Exercises

The exercises for this week's lecture is be about distributed transport and streaming. You will be creating a Kafka
cluster and various publishing programs and subscribing programs to stream records.
The focus of today's lecture is to interact with Kafka, create Python producer and consumer programs, and configure
Kafka registry and Kafka connect modules. Furthermore, we introduce exercises to Sqoop and Flume.

Please open issues [here](https://github.com/jakobhviid/BigDataCourseExercises/issues) if you encounter unclear
information or experience bugs in our examples!

### Exercise 1 - Deploy a Kafka cluster

The objective of this exercise is to deploy a Kafka cluster. This year will be using a helm chart
from [Bitnami](https://artifacthub.io/packages/helm/bitnami/kafka#bitnami-package-for-apache-kafka) to deploy a Kafka
cluster.

**Task**: Deploy Kafka using the `helm` and the following [kafka-values.yaml](kafka-values.yaml) file.

```bash
helm install --values kafka-values.yaml kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 30.0.4
```

**Notice**: When you need to delete the Kafka cluster, you can use the following command: `helm delete kafka`.

#### Validate the deployment of Kafka using a simple producer and consumer

**Tasks**: Producing and consuming topic messages

1. Create a Kafka client pod (`docker.io/bitnami/kafka:3.8.0-debian-12-r3`) using `kubectl run`.

```bash
kubectl run kafka-client --restart='Never' --image docker.io/bitnami/kafka:3.8.0-debian-12-r3  --command -- sleep infinity
```

2. Open two terminals and attach to the Kafka client pod using `kubectl exec` command.

```bash
kubectl exec -it kafka-client -- bash
```

3. Run the following command in the first terminal to produce messages to the Kafka topic `test`:

```bash
kafka-console-producer.sh --bootstrap-server kafka:9092 --topic test
```

4. Run the following command in the second terminal to consume messages from the Kafka topic `test`:

```bash
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test --from-beginning
```

5. Try typing text in the first terminal and hit enter. What happens in the second terminal?

**Task**: Delete the pod you just created (`kafka-client`).

<details>
  <summary><strong>Hint</strong>: Delete the pod</summary>

CTRL + C should exit the terminal and delete the pod. If not then just use the commands below.

  ```bash
  kubectl delete pod/kafka-client
  ```

</details>

### Exercise 2 - Additional deployments of Kafka Connect, Kafka Schema Registry, and Kafka KSQL

We will also use Kafka Connect, Kafka Schema Registry, and Kafka KSQL to facilitate a distributed transport and
streaming of records in Kafka. These services are included in the given in the manifest files:

- [kafka-schema-registry.yaml](./kafka-schema-registry.yaml)
- [kafka-connect.yaml](./kafka-connect.yaml)
- [kafka-ksqldb.yaml](./kafka-ksqldb.yaml)

**Task**: Familiarize yourself with the three mentioned files.
**Task**: Create additional deployments of Kafka Connect, Kafka Schema Registry, and Kafka KSQL using the following
commands:

1. Apply the Kafka Schema Registry manifest file to your namespace. `kubectl apply -f kafka-schema-registry.yaml`
1. Apply the Kafka Connect module to your namespace. `kubectl apply -f kafka-connect.yaml`
1. Apply the Kafka Ksqldb server to your namespace. `kubectl apply -f kafka-ksqldb.yaml`
1. Toggle the following values in the redpanda config map ([redpanda.yaml](./redpanda.yaml)) to enable Kafka modules.
    - `KAFKA_SCHEMAREGISTRY_ENABLED`=`true`
    - `CONNECT_ENABLED`=`true`

The list below summarises the extra services and briefly demonstrate how to interact with them:

- Registry (kafka-schema-registry)
    - `kubectl port-forward svc/kafka-schema-registry 8081`. Make a `curl` cmd in a terminal using the
      URL [http://127.0.0.1:8081](http://127.0.0.1:8081) and get this output:

      ```bash
      curl http://127.0.0.1:8081
      {}%                                  
      ```

- Connect (kafka-connect)
    - `kubectl port-forward svc/kafka-connect 8083`. Make a `curl` cmd in a terminal using the
      URL [http://127.0.0.1:8083](http://127.0.0.1:8083) and get this output:

      ```bash
      curl http://127.0.0.1:8083
      {"version":"7.3.1-ce","commit":"a453cbd27246f7bb","kafka_cluster_id":"<kafka_cluster_id>"}%                                    
      ```

- KsqlDB (kafka-ksqldb-server) and KsqlDB CLI (kafka-ksqldb-cli)
    - `kubectl exec -it deployment/kafka-ksqldb-cli -- ksql http://kafka-ksqldb-server:8088` make sure you
      will reach a console similar to this:

      ```bash
                        
                        ===========================================
                        =       _              _ ____  ____       =
                        =      | | _____  __ _| |  _ \| __ )      =
                        =      | |/ / __|/ _` | | | | |  _ \      =
                        =      |   <\__ \ (_| | | |_| | |_) |     =
                        =      |_|\_\___/\__, |_|____/|____/      =
                        =                   |_|                   =
                        =        The Database purpose-built       =
                        =        for stream processing apps       =
                        ===========================================
  
      Copyright 2017-2022 Confluent Inc.
  
      CLI v7.3.1, Server v7.3.1 located at http://kafka-ksqldb-server:8088
      Server Status: RUNNING
  
      Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!
  
      ksql> 
      ```

### Exercise 3 - The Redpanda console

We will use the [Redpanda Console](https://redpanda.com/redpanda-console-kafka-ui) to interact with Kafka. The console
is a web-based user interface that allows you to interact with Kafka topics, schema registry, and Kafka connectors.

**Task**: Deploy Redpanda using manifest file [redpanda.yaml](./redpanda.yaml) with the following command:

```bash
kubectl apply -f redpanda.yaml
```

**Task**: Access Redpanda using the following command: `kubectl port-forward svc/redpanda 8080` to
open [Redpanda](http://127.0.0.1:8080) in your browser!

**Task**: Explore the following tabs:

- [Overview](http://127.0.0.1:8080/overview)
- [Topics](http://127.0.0.1:8080/topics)
- [Schema Registry](http://127.0.0.1:8080/schema-registry)
- [Kafka Connectors](http://127.0.0.1:8080/connect-clusters/Connectors)

**Task**: Question: What does the [Topics](http://127.0.0.1:8080/topics) view show you?

**Task**: Manual interaction with Kafka using the Redpanda UI:

1. Use Redpanda to create a topic called `test-redpanda`.
1. Use Redpanda to produce record with `key=1` and `value={"id":1,"status":"it works"}` to the created topic
   `test-redpanda`.
1. Use Redpanda to delete all the messages in topic `test-redpanda`.

<details>
  <summary><strong>Hint</strong>: Step by step.</summary>

1. A new topic can be generated by clicking on the "Create topic" button inside
   the [Topics](http://127.0.0.1:8080/topics) view.
1. Select the topic `test-redpanda` and find the button "Produce Record" inside the newly generated topic
   `test-redpanda`: [topics/test-redpanda](http://127.0.0.1:8080/topics/test-redpanda). Add the key value in the data
   field under "key" and add the json message into the data field under "value". Press "Produce" to complete.
1. Find the button "Delete Records" inside the newly generated topic
   `test-redpanda`: [topics/test-redpanda](http://127.0.0.1:8080/topics/test-redpanda). Select "All Partitions" ->
   press "Choose End Offset" -> select "High Watermark" -> press "Delete Records" -> done.

</details>

### Exercise 4 - Produce messages to Kafka using Python

The objective of this exercise is to create a program which publishes records to a Kafka topic. The exercise builds on
top of [exercise 8 from lecture 2](../02/README.md#exercise-10---create-six-fictive-data-sources), but instead of saving
the data to HDFS we will publish it to a Kafka topic and use a Kafka Connector to save the records to HDFS.
This exercise focuses on creating the topic and creating the producer. If you have not solved the exercise from last
week then you can use the files provided [here](./hints/simple-producer.py).

**Tasks**: Think about what properties you want for the `INGESTION` topic:

1. How many partitions will you have for the `INGESTION` topic?
2. Which replication factor will you use for the `INGESTION` topic?
3. Which min in-sync replicas will you use for the `INGESTION` topic?
4. What would be an appropriate retention time for the `INGESTION` topic?
5. What would be an appropriate retention size for the `INGESTION` topic?


#### 1. How many partitions will you have for the `INGESTION` topic?

**Answer:**  
`6`

**Explanation:**  
The number of partitions determines how many parallel consumers can process the topic and the overall throughput. Since you have six fictive data sources (from previous exercises), using six partitions allows each source to write to its own partition, maximizing parallelism and throughput. You can increase this number if you expect higher throughput or want to scale out consumers further.

#### 2. Which replication factor will you use for the `INGESTION` topic?

**Answer:**  
`3`

**Explanation:**  
The replication factor defines how many copies of each partition are stored across different Kafka brokers. A replication factor of 3 is a common best practice for production, as it provides fault tolerance: the topic can survive the loss of up to two brokers without data loss.

#### 3. Which min in-sync replicas will you use for the `INGESTION` topic?

**Answer:**  
`2`

**Explanation:**  
`min.insync.replicas` specifies the minimum number of replicas that must acknowledge a write for it to be considered successful. Setting this to 2 (with a replication factor of 3) ensures that at least two brokers have the data before acknowledging the write, improving durability and reducing the risk of data loss if a broker fails.


`min.insync.replicas` controls how many replicas must confirm the write.

**Example:**  
 - Replication factor = 3 (so 3 brokers have each partition)
 - `min.insync.replicas = 2`
 - When a producer writes a message, at least 2 of the 3 brokers must confirm they have stored it.
 - If fewer than 2 are available (e.g., 2 brokers are down), Kafka will **reject writes** to avoid data loss.

**Why is this important?**
- It **protects against data loss**: If too few replicas are available, Kafka won’t accept new data, so you don’t lose messages if a broker fails.
- It’s a trade-off: Higher values mean more safety, but less availability if brokers go down.


#### 4. What would be an appropriate retention time for the `INGESTION` topic?

**Answer:**  
`7 days` (604800000 ms)

**Explanation:**  
Retention time controls how long messages are kept in the topic before being deleted. Seven days is a common default, giving downstream systems enough time to process the data and allowing for reprocessing if needed. Adjust this based on your storage capacity and business requirements.

#### 5. What would be an appropriate retention size for the `INGESTION` topic?

**Answer:**  
`10 GB` (10737418240 bytes)

**Explanation:**  
Retention size limits the total amount of data stored in the topic. If the topic exceeds this size, older messages are deleted, even if the retention time has not expired. This helps prevent the cluster from running out of disk space. Choose a value that fits your expected data volume and available storage.

#### 6. Example Topic Creation Command

```bash
kubectl run kafka-client --restart='Never' --image docker.io/bitnami/kafka:3.8.0-debian-12-r3  --command -- sleep infinity
```
```bash
kubectl exec -it kafka-client -- bash
```
```bash
kafka-topics.sh --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 3 \
  --partitions 6 \
  --topic INGESTION \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 \
  --config retention.bytes=10737418240
```

---

Adjust these values as needed for your workload and cluster size.

**Task**: Create the `INGESTION` topic with your chosen properties.

**Hint**: Use Redpanda (ref. [exercise 02](README.md#exercise-3---the-redpanda-console)).

**Task**: Question: Which property will be possible if you add a key, which defines the sensor id, to each record?

- If you add a **key** (such as the sensor ID) to each record produced to Kafka, you enable **message partitioning by key**.

- **Property Enabled by Adding a Key (Sensor ID):**

  - Kafka will ensure that all records with the same key (sensor ID) are sent to the same partition.


- **Explanation:**
  - Kafka uses the key to determine the partition for each message (using a hash function).
  - This guarantees **message ordering per key**: all messages from the same sensor will be in order within a single partition.
  - It also enables efficient processing and aggregation per sensor, since all data for a sensor is in one partition.

- **Summary:**  
  - Adding a key (sensor ID) ensures that all records from the same sensor are written to the same partition, preserving their order and enabling partition-based processing.

**Task**: Update your program from [lecture 2 exercise 8](../02/README.md#exercise-10---create-six-fictive-data-sources)
to produce sensor samples directly to Kafka.

**Hint**: If you did not create a program then use the scripts [./hints/*.py](./hints).

**Task**: Now that you have created the producer program it is time to run it.

**Notice**: We recommend to use an interactive container and attach to it
using [vscode](../../../services/interactive/README.md#attach-visual-studio-code-to-a-running-container) as we did last
time in lecture 2.

**Verification**: To verify that the program is producing messages to the `INGESTION` topic. Open Redpanda
console: [localhost:8080/topics/INGESTION](http://127.0.0.1:8080/topics/INGESTION?p=-1&s=50&o=-1#messages).

<details>
  <summary><strong>Hints</strong>: The creation of the Python producer</summary>

1. Install Python inside the interactive container using `sudo apt update && sudo apt install python -y`
2. Copy the provided solution to the interactive container
    - [client.py](./hints/client.py)
    - [data_model.py](./hints/data_model.py)
    - [simple-producer.py](./hints/simple-producer.py)
3. Run the program using `python simple-producer.py`

Open [localhost:8080/topics/INGESTION](http://127.0.0.1:8080/topics/INGESTION#messages) in your browser and look for the
number of messages.

</details>

#### Solution

To run your Python script interactively in your Kubernetes cluster, you should create a pod based on a Python image with an interactive shell. Here’s how you can do it:


##### 1. **Start an Interactive Python Pod**

```bash
kubectl run python-client --rm -it --image=python:3.10-slim -- bash
```

- `--rm`: Remove the pod when you exit.
- `-it`: Interactive terminal.
- `--image=python:3.10-slim`: Uses an official Python image.
- `-- bash`: Starts a bash shell.

##### 2. **Install Kafka Python Client (inside the pod)**

Once inside the pod, install the Kafka client library:

```bash
pip install kafka-python
```

##### 3. **Copy Your Script and Dependencies**

In a new terminal, use `kubectl cp` to copy your script and dependencies into the pod:

```bash
kubectl cp /path/to/simple-producer.py python-client:/simple-producer.py
kubectl cp /path/to/client.py python-client:/client.py
kubectl cp /path/to/data_model.py python-client:/data_model.py
```

##### 4. **Run Your Script**

Inside the pod, run:

```bash
python simple-producer.py
```

**Note:**  
- Make sure your Python script’s Kafka broker address matches the service name and port in your cluster (e.g., `kafka:9092`).
- If you need additional dependencies, install them with `pip` inside the pod.

---

### Exercise 5 - Consume messages from Kafka using Python with single and multiple consumers

The objective of this exercise is to write a Python consumer and to output the messages from the `INGESTION` topic.

A requirement for the program is that it must take a consumer `group_id` as an argument. It can either be an environment
variable, a config file, or an argument to the program when you run it.

**Task**: Create a consumer program and run it in the terminal in your interactive container.

<details>
<summary><strong>Hint</strong>: A consumer program.</summary>

The below-mentioned files provide one solution for exercise.

- [hints/client.py](./hints/client.py)
- [hints/data_model.py](./hints/data_model.py) (not used but is still required because it is imported by
  `hints/client.py`)
- [hints/simple-consumer.py](./hints/simple-consumer.py)

</details>

<details>
<summary><strong>Verify</strong>: A consumer program</summary>

Run the program using `python simple-consumer.py`

The following should be printed to the console:

```bash
group_id=DEFAULT_CONSUMER
PackageObj(payload=SensorObj(sensor_id=3, modality=421, unit='MW', temporal_aspect='real_time'), correlation_id='906f764e-0f3b-4517-aed7-3b646081f6fb', created_at=1694902173.122458, schema_version=1)
...
...
...
```

</details>

**Task**: Start another consumer but with a different group id in your interactive container. What happens when you run
the program?

<details>
<summary><strong>Hint</strong>: Another consumer</summary>

Run the program using `python simple-consumer.py <GROUP_ID>`

The following should be printed to the console:

```bash
group_id=<GROUP_ID>
PackageObj(payload=SensorObj(sensor_id=3, modality=421, unit='MW', temporal_aspect='real_time'), correlation_id='906f764e-0f3b-4517-aed7-3b646081f6fb', created_at=1694902173.122458, schema_version=1)
...
...
...
```

</details>

**Task**: Open [localhost:8080/topics/INGESTION](http://127.0.0.1:8080/topics/INGESTION#consumers). You should now see a
table similar to the one below. What does the Lag column mean?

```bash
Group               Lag
<GROUP_ID>          3182
DEFAULT_CONSUMER    3186
```

**Task**: Questions:

1. How can we get two consumers to receive identical records?
2. How can we get two consumers to receive unique records?
3. What defines the maximum number of active parallel consumers within one consumer group?

#### Solution

##### 1. **Open the interactive pod from before**
```bash
kubectl exec -it python-client -- bash
```


##### 2. **Copy Your Script and Dependencies**

In a new terminal, use `kubectl cp` to copy your script and dependencies into the pod:

```bash
kubectl cp scripts/simple-consumer.py python-client:/scripts/simple-consumer.py
````

##### 3. **Start Consumer 1**

```bash
python simple-consumer.py
```

##### 4. **Start Consumer 2 in a new terminal**

```bash
python simple-consumer.py <GROUP_ID>
```


#### 1. **How can we get two consumers to receive identical records?**

**Answer:**  
To have two consumers receive identical records from a Kafka topic, you must assign them to **different consumer groups**.  
Kafka delivers each message in a topic to every consumer group independently. So, if you start two consumer processes with different `group.id` values, both will receive all messages from the topic.

**Explanation:**  
- Each consumer group maintains its own offset for each partition.
- Messages are broadcast to all groups, but within a group, messages are load-balanced among members.
- Use case: Multiple independent applications/processes need to process the same data stream.

#### 2. **How can we get two consumers to receive unique records?**

**Answer:**  
To have two consumers receive unique (non-overlapping) records, assign them to the **same consumer group**.  
Kafka will distribute partitions among all consumers in the same group, ensuring each message is delivered to only one consumer in that group.

**Explanation:**  
- Kafka uses the consumer group protocol to assign partitions to consumers.
- Each partition is consumed by only one consumer in a group at a time.
- This enables parallel processing and horizontal scaling, but each message is processed only once per group.

#### 3. **What defines the maximum number of active parallel consumers within one consumer group?**

**Answer:**  
The **maximum number of active parallel consumers in a consumer group is equal to the number of partitions in the topic**.

**Explanation:**  
- Each partition can be assigned to only one consumer in a group at a time.
- If there are more consumers than partitions, the extra consumers will be idle.
- If there are fewer consumers than partitions, some consumers will handle multiple partitions.
- **Formula:**  
  `max_active_consumers = number_of_partitions`



### Exercise 6 - Kafka Ksql

The objective of this exercise is use ksqlDB to split the records in the `INGESTION` topic into six separate streams
`SENSOR_ID_{1, 2, ..., 6}` based on the sensor id in the `payload` field of the JSON file.

#### Useful ksqlDB commands

- `SHOW TOPICS;`
- `PRINT <topic> FROM BEGINNING;`
- `SHOW STREAMS;`
- `CREATE STREAM ...`

**Task**: Get interactive shell with ksqlDB.

<details>
  <summary><strong>Hint</strong>: kubectl exec</summary>

  ```
  kubectl exec -it deployment/kafka-ksqldb-cli -- ksql http://kafka-ksqldb-server:8088
  ```

</details>


**Task**: Create a stream over the existing `INGESTION` topic with the following name `STREAM_INGESTION`.


<details>
<summary><strong>Hint</strong>:ksqlDB CREATE STREAM on topic</summary>

```sql
CREATE
STREAM STREAM_INGESTION (
  payload STRING,
  correlation_id STRING,
  created_at DOUBLE,
  schema_version INTEGER
) WITH (KAFKA_TOPIC = 'INGESTION', VALUE_FORMAT = 'JSON');
```

</details>

**Task**: Create a new stream based on previously created stream `STREAM_INGESTION`. Start by writing a SQL statement
which filters the records with the sensor of interest. Then populate the records into a new stream
`SENSOR_ID_<sensor_id>`.

<details>
<summary><strong>Hint</strong>: ksqlDB CREATE STREAM from SELECT</summary>

```sql
CREATE
STREAM SENSOR_ID_<sensor_id> AS
SELECT *
FROM STREAM_INGESTION
WHERE EXTRACTJSONFIELD(PAYLOAD, '$.sensor_id') = '<sensor_id>';
```

</details>

**Task**: Validate your newly created streams using ksql commands in its CLI.

<details>
<summary><strong>Hint</strong>: ksqlDB SELECT STREAM with EMIT CHANGES</summary>

```sql
SELECT *
FROM SENSOR_ID_<sensor_id> EMIT CHANGES;
```

The `EMIT CHANGES` clause makes the query a continuous query, meaning it will keep listening for new events as they
arrive

</details>

#### **Solution**

##### **Step 1: Get an Interactive Shell with ksqlDB**

**Command:**
```
kubectl exec -it deployment/kafka-ksqldb-cli -- ksql http://kafka-ksqldb-server:8088
```
**Explanation:**  
This command opens an interactive ksqlDB CLI connected to your ksqlDB server running in Kubernetes. You’ll use this shell to run SQL-like commands for stream processing.

##### **Step 2: Explore Existing Topics and Streams**

**Commands:**
```sql
SHOW TOPICS;
SHOW STREAMS;
```
**Explanation:**  
- `SHOW TOPICS;` lists all Kafka topics available in your cluster.  
- `SHOW STREAMS;` lists all ksqlDB streams you’ve already defined.  
This helps you verify that the `INGESTION` topic exists and see what streams are already set up.

##### **Step 3: Create a Stream Over the `INGESTION` Topic**

**Command:**
```sql
CREATE STREAM STREAM_INGESTION (
  payload STRING,
  correlation_id STRING,
  created_at DOUBLE,
  schema_version INTEGER
) WITH (KAFKA_TOPIC = 'INGESTION', VALUE_FORMAT = 'JSON');
```
**Explanation:**  
This defines a new ksqlDB stream called `STREAM_INGESTION` that reads from the Kafka topic `INGESTION`.  
- The schema matches the JSON structure of your messages.
- `VALUE_FORMAT = 'JSON'` tells ksqlDB to parse the topic data as JSON.

##### **Step 4: Create Streams for Each Sensor ID**

**Command (repeat for each sensor id 1–6):**
```sql
CREATE STREAM SENSOR_ID_1 AS
SELECT *
FROM STREAM_INGESTION
WHERE EXTRACTJSONFIELD(PAYLOAD, '$.sensor_id') = '1';
```
**Explanation:**  
- This creates a new stream (e.g., `SENSOR_ID_1`) that contains only records where the `sensor_id` in the JSON payload is `1`.
- `EXTRACTJSONFIELD(PAYLOAD, '$.sensor_id')` extracts the `sensor_id` from the JSON string in the `payload` field.
- Repeat this for each sensor id (replace `1` with `2`, `3`, etc.).

##### **Step 5: Validate the Streams**

**Command (for each stream):**
```sql
SELECT * FROM SENSOR_ID_1 EMIT CHANGES;
```
**Explanation:**  
- This command continuously outputs new records from the stream as they arrive.
- `EMIT CHANGES` makes the query “push” new results as soon as they are available, so you can see the stream working in real time.


##### **Summary Table**

| Step | Command | Purpose |
|------|---------|---------|
| 1 | `kubectl exec ...` | Open ksqlDB CLI |
| 2 | `SHOW TOPICS;`<br>`SHOW STREAMS;` | Explore topics/streams |
| 3 | `CREATE STREAM STREAM_INGESTION ...` | Define stream over Kafka topic |
| 4 | `CREATE STREAM SENSOR_ID_X AS ...` | Split into per-sensor streams |
| 5 | `SELECT * FROM SENSOR_ID_X EMIT CHANGES;` | Validate stream output |

### Exercise 7 - Kafka Connect and HDFS

The objective of this exercise is apply and configure a Kafka Connect module to write the records from the `INGESTION`
topic into HDFS as mentioned
in [exercise 03](README.md#exercise-2---additional-deployments-of-kafka-connect-kafka-schema-registry-and-kafka-ksql).

The module of interest is
the [HDFS 2 Sink Connector](https://docs.confluent.io/kafka-connectors/hdfs/current/overview.html) created by a company
called Confluent. The module will be accessible through the ready-running `kafka-connect` service. The creation of the
underlying image of our `kafka-connect` service can be further explored
here: [README.md](../../../services/kafka-connect/README.md).

***NB***: This connector module will work with our installation of HDFS despite the various version numbers. This module
is under the [Confluent Community License v1.0](https://www.confluent.io/confluent-community-license/) which enables
free use.

**Task**: Ensure you have HDFS running as in lecture 2.

**Task**: Setup HDFS 2 Sink Connector in our `kafka-connect` service.
**Notice**: The Redpanda UI does not include all the necessary properties for the configuration of the HDFS 2 Sink
Connector. Therefore, you need to investigate how to interact with
the [Connect REST Interface](https://docs.confluent.io/platform/current/connect/references/restapi.html). Then you need
to post the configuration using `curl` in a terminal with port-forwarding enabled or using an interactive container in
Kubernetes.

<details>
<summary><strong>Hint</strong>:Post the configuration using `curl`</summary>

This example will be using port-forwarding. Therefore, ensure `kubectl port-forward svc/kafka-connect 8083` has been
enabled.

Look into the configuration in the chunk below and the command in your terminal:

```bash
curl -X POST \
http://127.0.0.1:8083/connectors \
-H 'Content-Type: application/json' \
-d '{
    "name": "hdfs-sink",
    "config": {
        "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
        "tasks.max": "3",
        "topics": "INGESTION",
        "hdfs.url": "hdfs://namenode:9000",
        "flush.size": "3",
        "format.class": "io.confluent.connect.hdfs.json.JsonFormat",
        "key.converter.schemas.enable":"false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "key.converter.schema.registry.url": "http://kafka-schema-registry:8081", 
        "value.converter.schemas.enable":"false",
        "value.converter.schema.registry.url": "http://kafka-schema-registry:8081", 
        "value.converter": "org.apache.kafka.connect.json.JsonConverter"
    }
}'
```

**Note:** If this does not work for you (Windows), then you can use this command (using Command Prompt NOT Powershell):

````bash
curl -X POST -H "Content-Type: application/json" -d @hints/configuration.json http://127.0.0.1:8083/connectors
````

</details>


**Task**: Validate the HDFS 2 Sink Connector is working as expected.

- Make sure the following folders in HDFS have been created: `/topics` and `/logs`.

  <details>
    <summary><strong>Hint</strong>:Post the configuration using curl</summary>

    - Open and interactive terminal like `kubectl run hdfs-cli -it --image apache/hadoop:3 -- bash`.
    - `hdfs dfs -fs hdfs://namenode:8020 -ls /topics/` -> /topics/INGESTION
  </details>

- Use Redpanda UI to find the total lag (difference between log ned offset and group offset) of the consumer group
  `connect-hdfs-sink` [localhost:8080/groups/connect-hdfs-sink](http://127.0.0.1:8080/groups/connect-hdfs-sink).
    - How does HDFS 2 Sink Connector keep up with the six fictive data sources?

#### **Solution**

**This exercise teaches you how to bridge Kafka and HDFS using Kafka Connect, automating the movement of streaming data into distributed storage for further processing or analytics.**


##### **Goal**
You want to automatically write all records from the Kafka topic `INGESTION` into HDFS using Kafka Connect’s HDFS 2 Sink Connector.

##### **Step 1: Ensure HDFS is Running**

Before you start, make sure your HDFS cluster is up and running (see Lecture 2).  
You can check with:
```bash
kubectl get pods
```
Look for pods like `namenode-0`, `datanode-0`, etc.

##### **Step 2: Port-Forward Kafka Connect**

Kafka Connect exposes a REST API for configuration.  
You need to make it accessible on your local machine:
```bash
kubectl port-forward svc/kafka-connect 8083
```
Now, the Kafka Connect REST API is available at `http://127.0.0.1:8083`.

##### **Step 3: Prepare the HDFS Sink Connector Configuration**

You need to tell Kafka Connect how to write to HDFS.  
Here’s a sample configuration (explained below):

````bash
curl -X POST \
http://127.0.0.1:8083/connectors \
-H 'Content-Type: application/json' \
-d '{
    "name": "hdfs-sink",
    "config": {
        "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
        "tasks.max": "3",
        "topics": "INGESTION",
        "hdfs.url": "hdfs://namenode:9000",
        "flush.size": "3",
        "format.class": "io.confluent.connect.hdfs.json.JsonFormat",
        "key.converter.schemas.enable":"false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "key.converter.schema.registry.url": "http://kafka-schema-registry:8081", 
        "value.converter.schemas.enable":"false",
        "value.converter.schema.registry.url": "http://kafka-schema-registry:8081", 
        "value.converter": "org.apache.kafka.connect.json.JsonConverter"
    }
}'
````

**Explanation of important fields:**
- `"connector.class"`: Specifies the HDFS Sink Connector.
- `"topics"`: The Kafka topic to read from (`INGESTION`).
- `"hdfs.url"`: The HDFS namenode address.
- `"flush.size"`: Number of records to buffer before writing a file to HDFS (small for demo, larger in production).
- `"format.class"`: Write data as JSON files.
- `"key.converter"` and `"value.converter"`: How to interpret Kafka messages (JSON in this case).

##### **Step 4: Post the Configuration**

Run the above `curl` command in your terminal (with port-forwarding running).  
This tells Kafka Connect to start a new connector named `hdfs-sink` with your settings.

##### **Step 5: Validate the Connector**

1. **Check HDFS for Output:**
   - Open a Hadoop CLI pod:
     ```bash
     kubectl run hdfs-cli -it --image apache/hadoop:3 -- bash
     ```
   - List the topics directory:
     ```bash
     hdfs dfs -fs hdfs://namenode:9000 -ls /topics/
     ```
   - You should see `/topics/INGESTION` and `/logs` created by the connector.

2. **Check Connector Status:**
   - Visit [http://localhost:8080/connect-clusters/Connectors](http://localhost:8080/connect-clusters/Connectors) in Redpanda UI.
   - Or use the REST API:
     ```bash
     curl http://127.0.0.1:8083/connectors/hdfs-sink/status
     ```

3. **Check Consumer Lag:**
   - Go to [http://localhost:8080/groups/connect-hdfs-sink](http://localhost:8080/groups/connect-hdfs-sink).
   - The **lag** shows how many messages are waiting to be written to HDFS. If lag is low or zero, the connector is keeping up.

##### **How does the HDFS 2 Sink Connector keep up with six data sources?**

**In short**
- The connector can run multiple tasks (`tasks.max`), allowing parallel consumption from Kafka partitions.
- Since your `INGESTION` topic has 6 partitions (one per data source), the connector can process all partitions in parallel, efficiently writing data from all sources to HDFS.
- If lag increases, you may need to increase `tasks.max` or tune HDFS/Kafka performance.


**Detailed Explanation**

The HDFS 2 Sink Connector is designed to efficiently handle data from multiple sources by leveraging Kafka’s partitioning and its own ability to run multiple parallel tasks.

- **Parallelism with Tasks:**  
  The connector’s `tasks.max` setting controls how many parallel tasks (workers) it can run. Each task can independently read from one or more Kafka partitions and write data to HDFS.

- **Partition-to-Task Mapping:**  
  Your `INGESTION` topic has 6 partitions, typically one for each data source. Kafka Connect will assign these partitions across the available tasks. If `tasks.max` is set to 6, each task can handle one partition, allowing all data sources to be processed in parallel.

- **Efficient Data Movement:**  
  This parallelism ensures that records from all six data sources are consumed and written to HDFS simultaneously, maximizing throughput and minimizing lag.

- **Scaling Up:**  
  If you notice that the connector is falling behind (lag is increasing), you can increase the `tasks.max` value. This allows more tasks to run in parallel, which can help the connector keep up with higher data rates—provided there are enough partitions and system resources.

##### **Why not just give a high number for tasks.max?**

Setting a very high value for `tasks.max` does **not** always improve performance, and here’s why:

- **Partition Limit:**  
  The number of active tasks cannot exceed the number of partitions in the topic. Extra tasks will remain idle. For example, if your topic has 6 partitions and you set `tasks.max` to 20, only 6 tasks will be used.

- **Resource Usage:**  
  Each task consumes system resources (CPU, memory). Setting `tasks.max` too high can waste resources and may even degrade performance if your system is overloaded.

- **Management Overhead:**  
  More tasks mean more overhead for coordination and management within Kafka Connect, which can reduce efficiency.

**Best Practice:**  
Set `tasks.max` to match the number of partitions in your topic or the number of parallel consumers you actually need, based on your workload and available resources.

##### **How to Update/Increase `tasks.max`**

1. **Edit the Connector Configuration:**  
    - You can update the connector’s configuration by sending a `PUT` instead of `POST`request to the Kafka Connect REST API. For example, to increase `tasks.max` to 8
    - POST to /connectors is for creating a new connector. PUT to /connectors/<connector-name>/config is for updating an existing connector.

```bash
curl -X PUT \
http://127.0.0.1:8083/connectors/hdfs-sink/config \
-H 'Content-Type: application/json' \
-d '{
        "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
        "tasks.max": "8",
        "topics": "INGESTION",
        "hdfs.url": "hdfs://namenode:9000",
        "flush.size": "3",
        "format.class": "io.confluent.connect.hdfs.json.JsonFormat",
        "key.converter.schemas.enable":"false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "key.converter.schema.registry.url": "http://kafka-schema-registry:8081", 
        "value.converter.schemas.enable":"false",
        "value.converter.schema.registry.url": "http://kafka-schema-registry:8081", 
        "value.converter": "org.apache.kafka.connect.json.JsonConverter"
}'
```

2. **Or Delete and Recreate the Connector:**  
   If you prefer, you can delete the connector and create it again with a higher `tasks.max` value.



##### **Summary Table**

| Step | What you do | Why |
|------|-------------|-----|
| 1 | Ensure HDFS is running | So data can be written to HDFS |
| 2 | Port-forward Kafka Connect | To access REST API locally |
| 3 | Prepare connector config | Tell Kafka Connect how to write to HDFS |
| 4 | POST config with curl | Deploy the connector |
| 5 | Validate in HDFS and UI | Ensure data is flowing as expected |


### Exercise 8 - Flume

The objective of this exercise is to ingest data from a command-line program into Kafka using Flume. This exercise will
simulate a scenario where an endpoint continuously provides new data that can be ingested using Flume.

**Task**: Deploy the Flume manifest [flume.yaml](flume.yaml).

**Task**: Open an interactive container with Python 3.12 or use the interactive pod provided.

<details>
<summary><strong>Hint</strong>: kubectl run</summary>

```bash
kubectl run python -it --image python:3.12 -- bash
```

</details>

**Task**: Create a command-line program which simulates new inputs from the command-line. Run the program in your
interactive container.

<details>
<summary><strong>Hint</strong>: A text input program.</summary>

The below-mentioned file provide one solution for exercise.

- [text_input.py](./hints/text_input.py)

  </details>

**Task**: Open [localhost:8080/topics/flume-logs](http://localhost:8080/topics/flume-logs). You should now see the
streamed data from the command-line into a kafka topic.

**Optional task**: Set up a new HDFS 2 Sink Connector to ingest the data into HDFS from the `flume-logs` Kafka topic.

### Exercise 9 - Sqoop

The objective of this exercise is to ingest a database table into HDFS utilizing Sqoop.

#### Useful Sqoop commands

- `sqoop list-databases --connect "jdbc:<DB_DRIVER>://<DB_URL>/<DB_TABLE>"`
- `sqoop import --connect "jdbc:<DB_DRIVER>://<DB_URL>/<DB_TABLE>"`

**Task**: Deploy PostgresSQL database using Helm Chart with the following command:

```bash
helm install postgresql \
  --version=12.1.5 \
  --set auth.username=root \
  --set auth.password=pwd1234 \
  --set auth.database=employees \
  --set primary.extendedConfiguration="password_encryption=md5" \
  --repo https://charts.bitnami.com/bitnami \
  postgresql
```

**Task**: Get interactive shell with PostgresSQL.

<details>
  <summary><strong>Hint</strong>: kubectl exec</summary>

  ```
  kubectl exec -it postgresql-0  -- bash
  ```

</details>

**Task**: Seed the database with employees.

<details>
  <summary><strong>Hint</strong>: Create a new table with employees</summary>

- Login to the database

  ```bash
  PGPASSWORD=pwd1234 psql -U root -d employees
  ```

- Seed the database with employees

  ```
  CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    department VARCHAR(255) NOT NULL,
    salary DECIMAL(10, 2) NOT NULL
  );
  
  INSERT INTO employees (name, department, salary) VALUES
  ('John Doe', 'Engineering', 75000),
  ('Jane Smith', 'Marketing', 65000),
  ('Alice Johnson', 'HR', 60000),
  ('Robert Brown', 'Finance', 80000);
  ```

</details>

<details>
  <summary><strong>Verify</strong>: Check employees were added</summary>

  ```bash
  SELECT * FROM employees;
  ```

Expected output

  ```
    id |     name      | department  |  salary
  ----+---------------+-------------+----------
    1 | John Doe      | Engineering | 75000.00
    2 | Jane Smith    | Marketing   | 65000.00
    3 | Alice Johnson | HR          | 60000.00
    4 | Robert Brown  | Finance     | 80000.00
  (4 rows)
  ```

</details>

Now the database have been seeded with employees and now be ingested with Apache Sqoop

**Task**: Deploy the Sqoop manifest [sqoop.yaml](sqoop.yaml).

**Task**: Get interactive shell with Sqoop.

<details>
  <summary><strong>Hint</strong>: kubectl exec</summary>

  ```
  kubectl exec -it sqoop-<ID> -- bash
  ```

</details>

**Task**: Verify that Sqoop can connect to the PostgresSQL database

<details>
  <summary><strong>Hint</strong>: List databases with Sqoop</summary>

  ```bash
  sqoop list-databases \
    --connect "jdbc:postgresql://postgresql:5432/employees" \
    --username root \
    --password pwd1234 
  ```

</details>

**Task**: Ingest the database into HDFS

<details>
  <summary><strong>Hint</strong>: Use Sqoop to import database</summary>

  ```bash
  sqoop import \
  --connect "jdbc:postgresql://postgresql:5432/employees" \
  --username root \
  --password pwd1234 \
  --table employees \
  --target-dir /employees \
  --direct \
  --m 1
  ```

</details>

The expected output should look like this:

```bash
Warning: /usr/local/sqoop/../hbase does not exist! HBase imports will fail.
Please set $HBASE_HOME to the root of your HBase installation.
Warning: /usr/local/sqoop/../hcatalog does not exist! HCatalog jobs will fail.
Please set $HCAT_HOME to the root of your HCatalog installation.
Warning: /usr/local/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
Warning: /usr/local/sqoop/../zookeeper does not exist! Accumulo imports will fail.
Please set $ZOOKEEPER_HOME to the root of your Zookeeper installation.
2024-08-23 09:55:46,594 INFO sqoop.Sqoop: Running Sqoop version: 1.4.7
2024-08-23 09:55:46,617 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
2024-08-23 09:55:46,688 INFO manager.SqlManager: Using default fetchSize of 1000
2024-08-23 09:55:46,688 INFO tool.CodeGenTool: Beginning code generation
2024-08-23 09:55:46,809 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM "employees" AS t LIMIT 1
2024-08-23 09:55:46,834 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /usr/local/hadoop
Note: /tmp/sqoop-root/compile/499d9f344cd22bb55b69c541d7b178f9/employees.java uses or overrides a deprecated API.
Note: Recompile with -Xlint:deprecation for details.
2024-08-23 09:55:48,211 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-root/compile/499d9f344cd22bb55b69c541d7b178f9/employees.jar
2024-08-23 09:55:48,223 INFO manager.DirectPostgresqlManager: Beginning psql fast path import
2024-08-23 09:55:48,225 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM "employees" AS t LIMIT 1
2024-08-23 09:55:48,230 INFO manager.DirectPostgresqlManager: Copy command is COPY (SELECT "id", "name", "department", "salary" FROM "employees" WHERE 1=1) TO STDOUT WITH DELIMITER E'\54' CSV ;
2024-08-23 09:55:48,234 INFO manager.DirectPostgresqlManager: Performing import of table employees from database employees
2024-08-23 09:55:49,111 INFO manager.DirectPostgresqlManager: Transfer loop complete.
2024-08-23 09:55:49,112 INFO manager.DirectPostgresqlManager: Transferred 124 bytes in 0.0836 seconds (1.4486 KB/sec)
```

**Task**: Verify that the PostgresSQL table `employees` were ingested into HDFS.

**Validate**: Make sure a directory `/employees` were added to HDFS.

#### **Solution**
**This exercise demonstrates how to move data from a relational database (PostgreSQL) into HDFS using Sqoop, which is a common pattern in big data pipelines.**

##### **Step 1: Deploy PostgreSQL Database**

**Command:**
```bash
helm install postgresql \
  --version=12.1.5 \
  --set auth.username=root \
  --set auth.password=pwd1234 \
  --set auth.database=employees \
  --set primary.extendedConfiguration="password_encryption=md5" \
  --repo https://charts.bitnami.com/bitnami \
  postgresql
```
**Explanation:**  
This command deploys a PostgreSQL database in your Kubernetes cluster using Helm.  
- Username: `root`
- Password: `pwd1234`
- Database: `employees`

##### **Step 2: Get an Interactive Shell with PostgreSQL**

**Command:**
```bash
kubectl exec -it postgresql-0 -- bash
```
**Explanation:**  
This opens a shell inside the PostgreSQL pod so you can interact with the database.

##### **Step 3: Seed the Database with Employees**

**a. Login to PostgreSQL:**
```bash
PGPASSWORD=pwd1234 psql -U root -d employees
```
**b. Create and Populate the Table:**
```sql
CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  department VARCHAR(255) NOT NULL,
  salary DECIMAL(10, 2) NOT NULL
);

INSERT INTO employees (name, department, salary) VALUES
('John Doe', 'Engineering', 75000),
('Jane Smith', 'Marketing', 65000),
('Alice Johnson', 'HR', 60000),
('Robert Brown', 'Finance', 80000);
```
**Explanation:**  
You create a table and insert four sample employees.

- `-U root` Connect as user root
- `-d employees` Connect to the database named employees

##### **Step 4: Verify the Data**

**Command:**
```sql
SELECT * FROM employees;
```
**Expected Output:**  
A table listing the four employees you just inserted.

##### **Step 5: Deploy the Sqoop Pod**

**Command:**
```bash
kubectl apply -f sqoop.yaml
```
**Explanation:**  
This deploys a Sqoop pod, which will be used to transfer data from PostgreSQL to HDFS.

##### **Step 6: Get an Interactive Shell with Sqoop**

**Command:**
```bash
kubectl exec -it sqoop-<ID> -- bash
```
Replace `<ID>` with the actual pod name.

##### **Step 7: Verify Sqoop Can Connect to PostgreSQL**

**Command:**
```bash
sqoop list-databases \
  --connect "jdbc:postgresql://postgresql:5432/employees" \
  --username root \
  --password pwd1234
```
**Explanation:**  
This checks that Sqoop can connect to your PostgreSQL database.

##### **Step 8: Import the Employees Table into HDFS**

**Command:**
```bash
sqoop import \
  --connect "jdbc:postgresql://postgresql:5432/employees" \
  --username root \
  --password pwd1234 \
  --table employees \
  --target-dir /employees \
  --direct \
  --m 1
```
**Explanation:**  
This command tells Sqoop to import the `employees` table from PostgreSQL into HDFS at `/employees`.  
- `--direct`: Uses a fast path for import.
- `--m 1`: Uses one mapper (parallel task).

##### **Step 9: Validate the Import**
You need to open an interactive shell in a pod that has the HDFS CLI installed before running the `hdfs dfs -ls /employees` command.

1. **Start an interactive shell in an HDFS CLI pod:**
   ```bash
   kubectl run hdfs-cli -it --image apache/hadoop:3 -- bash
   ```
   or
   ```bash
   kubectl exec -it hdfs-cli -- bash
   ```
   This command creates (or reuses) a pod with Hadoop installed and opens a bash shell.

2. **Run the HDFS command inside the shell:**
   ```bash
   hdfs dfs -ls /employees
   ```

**Explanation:**  
Check that the `/employees` directory exists in HDFS and contains the imported data.


## **Summary Table**

| Step | Command | Purpose |
|------|---------|---------|
| 1 | `helm install ...` | Deploy PostgreSQL |
| 2 | `kubectl exec ...` | Access PostgreSQL pod |
| 3 | `psql ...` + SQL | Create and seed table |
| 4 | `SELECT * ...` | Verify data |
| 5 | `kubectl apply -f sqoop.yaml` | Deploy Sqoop |
| 6 | `kubectl exec ...` | Access Sqoop pod |
| 7 | `sqoop list-databases ...` | Test connection |
| 8 | `sqoop import ...` | Import data to HDFS |
| 9 | `hdfs dfs -ls /employees` | Validate import |


## Step-by-step guide to clean up

You will be using HDFS, Kafka, and the interactive container in next lecture. However, if you will clean up the
resources created in this lecture, you can follow the steps below:

### Automated clean up

If you have Python installed on your machine, you can use the following command to clean up all resources:

**Windows**:

````bash
python cleanup.py
````

**MacOS / Linux**:

````bash
python3 cleanup.py
````

The script will delete all resources created in the exercises.

### Manual clean up

- Today's exercises.
    1. `kubectl delete -f sqoop.yaml`
    1. `helm delete postgresql`
    1. `kubectl delete pvc data-postgresql-0`
    1. `kubectl delete pod python`
    1. `kubectl delete -f flume.yaml`
    1. `kubectl delete pod kafka-client`
    1. `kubectl delete -f redpanda.yaml`
    1. `kubectl delete -f kafka-schema-registry.yaml`
    1. `kubectl delete -f kafka-connect.yaml`
    1. `kubectl delete -f kafka-ksqldb.yaml`
    1. `helm delete kafka`
- cd into the `services/interactive` folder in the repository.
    1. `kubectl delete -f interactive.yaml`
- cd into the `services/hdfs` folder in the repository.
    1. `kubectl delete -f hdfs-cli.yaml` (if used)
    1. `kubectl delete -f datanodes.yaml`
    1. `kubectl delete -f namenode.yaml`
    1. `kubectl delete -f configmap.yaml`

You can get a list of the pods and services to verify that they are deleted.

- `kubectl get all`
