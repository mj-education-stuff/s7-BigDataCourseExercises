# Lecture 04 - Spark

The exercises for this lecture are about Apache Spark. Apache Spark is a unified analytics engine for big data
processing, with built-in modules for streaming, SQL, machine learning, and graph processing. It can be used to process
large amounts of data in parallel on a cluster of computers.
Apache Spark is built to work on top of the Hadoop ecosystem and can be used to process data stored in HDFS, S3, or
other storage systems.

Please open issues [here](https://github.com/jakobhviid/BigDataCourseExercises/issues) if you encounter unclear
information or experience bugs in our examples!

## Exercises

### Exercise 1 - Deploying Apache Spark on Kubernetes

Before you get to play around with Apache Spark you need to deploy your Spark environment on your Kubernetes cluster. We
will be using a helm chart to deploy Spark on Kubernetes.

**Task**: Inspect the [spark-values.yaml](./spark-values.yaml) file to see how the Spark deployment is configured.

**Task**: Install the Spark Helm chart using the following command:

```bash
helm install --values spark-values.yaml spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.10
```

**Task**: Inspect the UI of the Spark deployment and validate that there are two worker nodes alive.

```bash
kubectl port-forward svc/spark-master-svc 8080:80
```

#### **Solution**

**By following these steps, you learn how to deploy and manage a distributed Spark environment on Kubernetes using Helm, and how to verify that your cluster is running as expected.**

##### **Step 1: Inspect the `spark-values.yaml` File**

**What you do:**  
Open and review the `spark-values.yaml` file in your project.

**Why:**  
This file contains configuration values for the Spark Helm chart. It defines how Spark will be deployed on your Kubernetes cluster, such as:
- The number of worker nodes
- Resource limits (CPU, memory)
- Service types and ports
- Environment variables

**Technical background:**  
Helm uses these values to customize the deployment. Understanding this file helps you know what your Spark cluster will look like and how it will behave.

spark-values.yaml:
```yaml
image:
  repository: bitnami/spark
  tag: 3.5.2-debian-12-r1
master:
  resources:
    limits:
      cpu: 1
      memory: 2Gi
    requests:
      cpu: 1
      memory: 1Gi
worker:
  replicaCount: 2
  resources:
    limits:
      cpu: 1
      memory: 2Gi
    requests:
      cpu: 1
      memory: 1Gi
```

Here’s what each part of your spark-values.yaml configuration means:

**image**
- **repository: bitnami/spark**  
  Specifies the Docker image repository to use for Spark. Here, it’s the official Bitnami Spark image.
- **tag: 3.5.2-debian-12-r1**  
  Specifies the exact version of the Spark image (Spark 3.5.2, based on Debian 12).

**master**
- **resources:**  
  Sets resource constraints for the Spark master pod.
  - **limits:**  
    - **cpu: 1** — The master pod can use up to 1 CPU core.
    - **memory: 2Gi** — The master pod can use up to 2 GiB of RAM.
  - **requests:**  
    - **cpu: 1** — The master pod requests 1 CPU core when scheduled.
    - **memory: 1Gi** — The master pod requests 1 GiB of RAM when scheduled.

**Requests** are the minimum resources guaranteed; **limits** are the maximum allowed.

**worker**
- **replicaCount: 2**  
  Deploys 2 Spark worker pods.
- **resources:**  
  Sets resource constraints for each worker pod (same as master).
  - **limits:**  
    - **cpu: 1** — Each worker can use up to 1 CPU core.
    - **memory: 2Gi** — Each worker can use up to 2 GiB of RAM.
  - **requests:**  
    - **cpu: 1** — Each worker requests 1 CPU core.
    - **memory: 1Gi** — Each worker requests 1 GiB of RAM.

**Summary:**  
This configuration deploys Spark using the Bitnami image, with 1 master and 2 worker pods. Each pod is allocated 1 CPU and 1–2 GiB RAM, ensuring resource control and cluster stability.

##### **Step 2: Install the Spark Helm Chart**

**Command:**
```bash
helm install --values spark-values.yaml spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.10
```

**What you do:**  
You use Helm (a Kubernetes package manager) to deploy Spark using the Bitnami Spark Helm chart, with your custom settings from `spark-values.yaml`.

**Why:**  
This command automates the creation of all the Kubernetes resources needed for Spark (pods, services, etc.), saving you from writing complex YAML files by hand.

**Technical background:**  
- `helm install` creates a new release (deployment) named `spark`.
- `--values spark-values.yaml` tells Helm to use your custom configuration.
- The chart from Bitnami contains templates for deploying Spark master and worker nodes, services, and more.
- The `oci://...` URL is the location of the Helm chart in the OCI registry.

##### **Step 3: Inspect the Spark UI and Validate Worker Nodes**

**Command:**
- in k9s it's 8080:8080. Maybe error in the task description?
```bash
kubectl port-forward svc/spark-master-svc 8080:80
```

**What you do:**  
You forward port 80 of the Spark master service in your Kubernetes cluster to port 8080 on your local machine.

**Why:**  
This allows you to access the Spark web UI in your browser at [http://localhost:8080](http://localhost:8080), even though Spark is running inside Kubernetes.

**Technical background:**  
- `kubectl port-forward` creates a tunnel from your local machine to the Kubernetes service.
- The Spark UI shows the status of the cluster, including the number of worker nodes, running jobs, and resource usage.
- You should see **two worker nodes** alive, as specified in your configuration.

##### **Summary Table**

| Step | What you do | Why | Technical background |
|------|-------------|-----|---------------------|
| 1 | Inspect `spark-values.yaml` | Understand deployment settings | Helm values file customizes the chart |
| 2 | Install Helm chart | Deploy Spark easily | Helm automates Kubernetes resource creation |
| 3 | Port-forward Spark UI | Access Spark dashboard | View cluster status and workers in browser |


### Exercise 2 - Running a Spark job locally and in your deployment

The first exercise is to run a Spark job that estimates pi. The program is written in Python and is an example of how to
create a Spark job that both can run on your localhost and in your Spark environment.

**Task**: Inspect the [pi-estimation.py](./pi-estimation.py) file.

**Task**: Run the [pi-estimation.py](./pi-estimation.py) file locally using Python.

**Help**: Running Spark jobs

- Using Python: ``python <SCRIPT.py> <NUMBER_OF_PARTITIONS>``
- Using `spark-submit`: ``spark-submit <SCRIPT.py> <NUMBER_OF_PARTITIONS>``
    - Have a look at the `spark-submit` documentation
      for [submitting-applications](https://spark.apache.org/docs/latest/submitting-applications.html).

**Question**: How will the number of partitions argument affect the result?

<details>
  <summary><strong>Hint</strong>: Run Spark locally</summary>

Change this line of code in [pi-estimation.py](./pi-estimation.py) to point to `SPARK_ENV.LOCAL`

  ```text
  spark = get_spark_context(app_name="Pi estimation", config=SPARK_ENV.LOCAL)
  ```

</details>

**Task**: Update the [pi-estimation.py](./pi-estimation.py) file to be executed on the inside your Kubernetes cluster.

- Does the number of partitions affect the runtime?
- How does the runtime compare to running the program locally?

<details>
  <summary><strong>Hint</strong>: Run Spark within the Kubernetes cluster</summary>

Change this line of code in [pi-estimation.py](./pi-estimation.py) to point to `SPARK_ENV.K8S`

  ```text
  spark = get_spark_context(app_name="Pi estimation", config=SPARK_ENV.K8S)
  ```

</details>

**Help**: Key differences between `python <SCRIPT.py>` and `spark-submit <SCRIPT.py>`

| **Aspect**              | `python <SCRIPT.py>`                                     | `spark-submit <SCRIPT.py>`                                               |
|-------------------------|----------------------------------------------------------|--------------------------------------------------------------------------|
| **Execution Mode**      | Local execution as a regular Python script               | Submit as a Spark job to a cluster                                       |
| **Spark Context**       | Must be created within the script                        | Created and managed by `spark-submit`                                    |
| **Cluster Integration** | Limited to local mode or simple clusters                 | Supports full integration with cluster managers (YARN, Kubernetes, etc.) |
| **Resource Management** | Limited to local machine resources                       | Managed by the cluster, scalable                                         |
| **Use Case**            | Development and testing locally                          | Production and large-scale distributed jobs                              |
| **Ease of Setup**       | Very easy; no additional setup required                  | Requires setup of cluster configuration and environment                  |
| **Dependencies**        | Must be managed manually in the script                   | Can include dependencies via `--packages` or `--jars` options            |
| **Error Handling**      | Errors shown in the console directly                     | Errors logged in cluster logs, more difficult to debug remotely          |
| **Logging**             | Logs output to console                                   | Logs managed by cluster manager, accessible via web UI or files          |
| **Deployment Modes**    | Supports only local mode                                 | Supports local, client, and cluster deployment modes                     |
| **Job Configuration**   | Configuration is hard-coded or via environment variables | Can pass configurations via command-line options                         |
| **Output and Results**  | Printed to console                                       | Can be redirected to files, databases, or external storage               |


#### **Solution**

**This exercise helps you bridge the gap between local Spark development and running scalable, distributed jobs in a real cluster environment.**

##### **Step 1: Inspect the `pi-estimation.py` File**

**What you do:**  
Open and read the `pi-estimation.py` Python script.

**Why:**  
This script contains a Spark job that estimates the value of pi using a Monte Carlo method. Understanding the code helps you see how Spark jobs are structured and how they can be run both locally and on a cluster.

**Technical background:**  
- The script uses Spark’s parallel processing to estimate pi by randomly generating points and checking how many fall inside a unit circle.
- The number of partitions determines how the work is split across the cluster or your local CPU cores.

##### **Step 2: Run the Script Locally**

**Command:**  
```bash
python pi-estimation.py <NUMBER_OF_PARTITIONS>
```
or  
```bash
spark-submit pi-estimation.py <NUMBER_OF_PARTITIONS>
```

**What you do:**  
You run the script on your local machine, either as a regular Python script or using `spark-submit`.

**Why:**  
Running locally is useful for development, debugging, and understanding how the code works before scaling up.

**Technical background:**  
- When run locally, Spark uses your computer’s resources.
- The number of partitions affects how many parallel tasks Spark creates. More partitions can improve parallelism up to the number of available CPU cores, but too many can add overhead.

##### **Step 3: How Does the Number of Partitions Affect the Result?**

**Explanation:**  
- **Accuracy:** The number of partitions does **not** affect the mathematical accuracy of the pi estimation (that depends on the total number of points simulated).
- **Performance:** More partitions can improve performance by allowing more parallel tasks, but too many can cause overhead and slow things down.
- **Resource Utilization:** On a cluster, more partitions can better utilize distributed resources.

##### **Step 4: Run the Script in the Kubernetes Cluster**

**What you do:**  
Update the script to use the Kubernetes Spark environment:

```python
spark = get_spark_context(app_name="Pi estimation", config=SPARK_ENV.K8S)
```

Then submit the job to your Spark cluster (inside Kubernetes):

```bash
spark-submit pi-estimation.py <NUMBER_OF_PARTITIONS>
```

**Why:**  
This lets you leverage the distributed computing power of your Spark cluster, which can handle much larger workloads than your local machine.

**Technical background:**  
- Spark jobs submitted to the cluster are distributed across multiple worker nodes.
- The cluster manager (Kubernetes) handles resource allocation, scheduling, and fault tolerance.

##### **Step 5: Compare Local vs. Cluster Execution**

- **Does the number of partitions affect runtime?**  
  Yes. On a cluster, more partitions can improve parallelism and reduce runtime, up to the point where you saturate the available resources.
- **How does runtime compare to local?**  
  The cluster should be faster for large jobs, especially as you increase the number of partitions and data size, because it can use many machines in parallel.

##### **Key Differences: `python <SCRIPT.py>` vs. `spark-submit <SCRIPT.py>`**

| Aspect                | `python <SCRIPT.py>` (Local)         | `spark-submit <SCRIPT.py>` (Cluster)           |
|-----------------------|--------------------------------------|------------------------------------------------|
| Execution Mode        | Local Python process                 | Distributed Spark job                          |
| Resource Management   | Local machine only                   | Managed by cluster (Kubernetes, YARN, etc.)    |
| Parallelism           | Limited to local CPU cores           | Scales across many nodes                       |
| Use Case              | Development, testing                 | Production, large-scale data processing        |
| Error Handling        | Console output                       | Logs in cluster, may require web UI to debug   |
| Dependencies          | Manual in script                     | Can be specified via `--packages` or `--jars`  |

##### **Summary**

- You learn how to run Spark jobs both locally and on a cluster.
- You see how partitioning affects parallelism and performance.
- You understand the difference between local development and distributed production jobs in Spark.


### Exercise 3 - Analyzing files using Spark jobs

The previous program you ran was estimating pi. This program only used compute resources and in this exercise you will
run a Spark job that will read a file and count the occurrences of different words in the file. You will be analyzing
the alice in wonderland text
from [lecture 2 exercise 3](../02/README.md#exercise-3---uploading-alice-in-wonderland-to-hdfs).

**Task**: Ensure the [alice in wonderland](https://www.gutenberg.org/files/11/11-0.txt) file is within your HDFS
cluster. If not upload the file to HDFS.

**Task**: Inspect the [word-count.py](./word-count.py). The program counts the occurrences of all unique "words" in the
input file.

**Task**: Try to visualize the [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) this program will create.

**Help**:

- Take a look [here](https://stackoverflow.com/a/30685279/9698208) to better understand how the DAG is created for the
  Spark program.
- You are able to get other examples of Spark programs [here](https://spark.apache.org/examples.html).

**Task**: Run the program locally and in the cluster pointing towards different input files.

```bash
spark-submit word-count.py
```

**Notice**:You can read about the word count program from Apache Spark [here](https://spark.apache.org/examples.html)
and [here](https://github.com/apache/spark/blob/c1b12bd56429b98177e5405900a08dedc497e12d/examples/src/main/python/wordcount.py).

#### **Solution**

##### **Step 1: Ensure "Alice in Wonderland" is in HDFS**

**What you do:**  
Check if the file exists in HDFS. If not, upload it.

**How:**
1. **Start an HDFS CLI pod** (if you don’t already have one):
   ```bash
   kubectl run hdfs-cli -it --image apache/hadoop:3 -- bash
   ```
2. **Check if the file exists:**
   ```bash
   hdfs dfs -ls /user/<your-username>/alice.txt
   ```
3. **If not present, upload it:**
   - Download the file locally:
     ```bash
     curl -O https://www.gutenberg.org/files/11/11-0.txt
     ```
   - Upload to HDFS (from inside the pod or by copying the file into the pod first):
     ```bash
     hdfs dfs -put 11-0.txt /user/<your-username>/alice.txt
     ```

**Why:**  
Spark will read the file from HDFS, so it must be available there.

**Technical background:**  
HDFS (Hadoop Distributed File System) is a distributed storage system. Spark can efficiently process files stored in HDFS.

##### **Step 2: Inspect `word-count.py`**

**What you do:**  
Open and read the `word-count.py` script.

**Why:**  
Understand how Spark jobs are structured and how the word count logic is implemented.

**Technical background:**  
A typical Spark word count script:
- Reads a text file into an RDD or DataFrame.
- Splits lines into words.
- Maps each word to a (word, 1) pair.
- Reduces by key to count occurrences.
- Saves or prints the result.

##### **Step 3: Visualize the Spark DAG**

**What you do:**  
Try to draw or imagine the Directed Acyclic Graph (DAG) for the job.

**Why:**  
Understanding the DAG helps you see how Spark breaks down your job into stages and tasks for distributed execution.

**Technical background:**  
- Each Spark transformation (e.g., `flatMap`, `map`, `reduceByKey`) creates a node in the DAG.
- Actions (e.g., `collect`, `saveAsTextFile`) trigger execution.
- The DAG ensures efficient, fault-tolerant execution.

**Example DAG for Word Count:**
1. **Read file** → RDD of lines
2. **flatMap** (split lines to words) → RDD of words
3. **map** (word → (word, 1)) → RDD of pairs
4. **reduceByKey** (sum counts) → RDD of (word, count)
5. **collect/save** (action)

[See this StackOverflow answer for a visual example.](https://stackoverflow.com/a/30685279/9698208)

##### **Step 4: Run the Program Locally**

**What you do:**  
Run the script on your local machine or in an interactive container.

**How:**
```bash
spark-submit word-count.py /path/to/alice.txt
```
- Replace `/path/to/alice.txt` with the local or HDFS path.

**Why:**  
Running locally is useful for development and debugging.

**Technical background:**  
- Spark runs in local mode, using your machine’s resources.
- The script processes the file and outputs word counts.

##### **Step 5: Run the Program in the Cluster**

**What you do:**  
Run the script using `spark-submit` with the HDFS file path.

**How:**
1. Create spark client pod
- ```bash
  kubectl run spark-cli --image=bitnami/spark:3.5.2-debian-12-r1 -- sleep infinity
  ```

2. Within the spark-cli shell, create a directory
- ```bash
  mkdir e4
  ```
- ```bash
  mkdir e4/src
  ```
3. Copy the scripts from your local machine to the spark-cli pod:
- ```bash
  kubectl cp word-count.py spark-cli:/opt/bitnami/spark/e4
  ```
- ```bash
  kubectl cp ./src/utils.py spark-cli:/opt/bitnami/spark/e4/src
  ```

4. Create a directory in HDFS (within hdfs-cli) and copy the script:
- ```bash
  hdfs dfs -mkdir /e4
  ```
- ```bash
  curl -O https://www.gutenberg.org/files/11/11-0.txt
  ```
- ```bash
  hdfs dfs -put 11-0.txt /e4/alice.txt
  ```

5. Run the Spark job:
- ```bash
  spark-submit word-count.py e4/alice.txt
  ```

**Why:**  
Running in the cluster leverages distributed resources for scalability.

**Technical background:**  
- Spark distributes the job across worker nodes.
- Each node processes a partition of the file in parallel.
- Results are aggregated and output as specified in the script.

##### **Step 6: Compare Results and Performance**

**What you do:**  
Compare the output and runtime between local and cluster runs.

**Why:**  
Understand the benefits of distributed processing and how Spark scales with data and resources.

**Technical background:**  
- On small files, local and cluster runtimes may be similar.
- On large files, the cluster should be much faster due to parallelism.

##### **Summary Table**

| Step | What you do | Why | Technical background |
|------|-------------|-----|---------------------|
| 1 | Ensure file in HDFS | Spark reads from HDFS | Distributed storage |
| 2 | Inspect script | Understand logic | Spark transformations/actions |
| 3 | Visualize DAG | See execution plan | Fault-tolerant, parallel execution |
| 4 | Run locally | Test/debug | Local Spark mode |
| 5 | Run in cluster | Scale up | Distributed Spark mode |
| 6 | Compare results | Learn performance impact | Parallelism, resource utilization |

**References:**
- [Spark Word Count Example](https://spark.apache.org/examples.html)
- [DAG in Spark](https://stackoverflow.com/a/30685279/9698208)


### Exercise 4 - Average sample values from JSON files stored in HDFS

Let us assume that you have a dataset of sample records stored in HDFS. The dataset is stored in JSON format and
contains defined by the [exercise 10 from lecture 02](../02/README.md#exercise-10---create-six-fictive-data-sources).

In this exercise you will run a Spark job that will read all the JSON files and computes the average value of the
`payload.modality` field for each station.

**Task**: Inspect the [avg-modalities.py](./avg-modalities.py).

**Task**: Ensure you have records stored in HDFS on the proper location. If not upload the records to HDFS
using [exercise 4 from lecture 03](./../03/README.md#exercise-4---produce-messages-to-kafka-using-python)
and [exercise 7 from lecture 3](../03/README.md#exercise-7---kafka-connect-and-hdfs)

**Task**: Run the Spark application on the cluster. What is the `payload.modality` average value for each station?

```bash
spark-submit avg-modalities.py
```

#### **Solution**
**By following these steps, you learn how to process JSON data in HDFS using Spark, aggregate results efficiently, and interpret distributed computation outputs.**

Calculating Average `payload.modality` per Station with Spark

##### **Step 1: Inspect the `avg-modalities.py` Script**

**What you do:**  
Open and review the `avg-modalities.py` script.

**Why:**  
Understanding the script helps you see how Spark reads JSON data, processes it, and computes averages. Typically, the script will:
- Read JSON files from HDFS into a Spark DataFrame.
- Extract relevant fields (`station` and `payload.modality`).
- Group by station and compute the average modality.

**Technical background:**  
Spark DataFrames make it easy to process structured data (like JSON). Grouping and aggregation are efficient and scalable.

##### **Step 2: Ensure JSON Records Exist in HDFS**

**What you do:**  
Check if your sample JSON records are present in HDFS at the expected location.

**How:**
1. **Open an HDFS CLI pod:**
   ```bash
   kubectl run hdfs-cli -it --image apache/hadoop:3 -- bash
   ```
2. **List files in the target directory (replace `<path>` with your actual path):**
   ```bash
   hdfs dfs -ls /topics/INGESTION/
   ```
3. **If missing, upload the records:**
   - Download or generate the JSON files locally.
   - Upload to HDFS:
     ```bash
     hdfs dfs -put <local-file> /topics/INGESTION/
     ```

**Why:**  
Spark jobs can only process data that is available in HDFS. Ensuring the data is present avoids runtime errors.

**Reference:**  
See Exercise 4 from Lecture 03 and Exercise 7 from Lecture 03 for data generation and ingestion steps.

##### **Step 3: Run the Spark Job on the Cluster**

**What you do:**  
Submit the Spark job to your cluster to process the JSON files.

**How:**
1. **Open a shell in your Spark client pod (if not already running):**
   ```bash
   kubectl run spark-cli --image=bitnami/spark:3.5.2-debian-12-r1 -- sleep infinity
   kubectl exec -it spark-cli -- bash
   ```
2. **Copy the script to the pod if needed:**
   ```bash
   kubectl cp avg-modalities.py spark-cli:/opt/bitnami/spark/e4
   ```
3. **Run the Spark job:**
   ```bash
   spark-submit avg-modalities.py
   ```

**Why:**  
`spark-submit` is the standard way to run Spark applications on a cluster. It distributes the computation across worker nodes for scalability.

##### **Step 4: Interpret the Output**

**What you do:**  
Review the output printed by the script. It should show the average `payload.modality` for each station.

**Example Output:**
```
+--------+------------------+
|station |avg_modality      |
+--------+------------------+
|1       | 421.5            |
|2       | 398.2            |
|...     | ...              |
+--------+------------------+
```

**Why:**  
This output tells you the average modality value for each station, which is the goal of the exercise.


##### **Summary Table**

| Step | What you do | Why | Technical background |
|------|-------------|-----|---------------------|
| 1 | Inspect `avg-modalities.py` | Understand logic | Learn Spark DataFrame operations |
| 2 | Ensure data in HDFS | Avoid errors | Spark reads from HDFS |
| 3 | Run `spark-submit` | Process data at scale | Distributed computation |
| 4 | Interpret output | Get results | Aggregation per station |


##### **Extra: What Does the Script Typically Look Like?**

A typical Spark script for this task might look like:

````python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("AvgModalities").getOrCreate()

# Adjust the path as needed
df = spark.read.json("hdfs://namenode:9000/topics/INGESTION/*.json")

# Compute average modality per station
result = (
    df.groupBy(col("station"))
      .agg(avg(col("payload.modality")).alias("avg_modality"))
)

result.show()
spark.stop()
````

###### **References**
- [Spark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)
- [Spark SQL Aggregations](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html)
- Lecture 03: Data Ingestion


### Exercise 5 - Average sample values from Avro files stored in HDFS (optional)

Let us assume that you have a dataset of sample records stored in HDFS. The dataset is stored in Avro format and
contains defined by the [exercise 10 from lecture 02](../02/README.md#exercise-10---create-six-fictive-data-sources)

In this exercise you will run a Spark job that will read all the Avro files and computes the average value of the
`payload.modality` field for each station.

**Task**: Inspect the [avg-modalities-avro.py](./avg-modalities-avro.py).

**Task**: Ensure you have records stored in HDFS on the proper location. If not upload the records to HDFS
using [exercise 10 from lecture 02](../02/README.md#exercise-10---create-six-fictive-data-sources)

**Task**: Run the Spark application on the cluster. This should produce the same results as
in [Exercise 4](#exercise-4---average-sample-values-from-json-files-stored-in-hdfs)

```bash
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.2 avg-modalities-avro.py
```

#### **Solution**
By following these steps, you learn how to process Avro data in HDFS using Spark, aggregate results efficiently, and interpret distributed computation outputs.

Step-by-Step Solution: Calculating Average `payload.modality` per Station from Avro Files with Spark

##### **Step 1: Inspect the `avg-modalities-avro.py` Script**

**What you do:**  
Open and review the `avg-modalities-avro.py` script.

**Why:**  
Understanding the script helps you see how Spark reads Avro data, processes it, and computes averages. Typically, the script will:
- Read Avro files from HDFS into a Spark DataFrame.
- Extract relevant fields (`station` and `payload.modality`).
- Group by station and compute the average modality.

**Technical background:**  
Spark supports Avro files via the `spark-avro` package. DataFrames make it easy to process structured data and perform aggregations.

##### **Step 2: Ensure Avro Records Exist in HDFS**

**What you do:**  
Check if your sample Avro records are present in HDFS at the expected location.

**How:**
1. **Open an HDFS CLI pod:**
   ```bash
   kubectl run hdfs-cli -it --image apache/hadoop:3 -- bash
   ```
2. **List files in the target directory (replace `<path>` with your actual path):**
   ```bash
   hdfs dfs -ls /topics/INGESTION/
   ```
3. **If missing, upload the records:**
   - Download or generate the Avro files locally.
   - Upload to HDFS:
     ```bash
     hdfs dfs -put <local-avro-file> /topics/INGESTION/
     ```

**Why:**  
Spark jobs can only process data that is available in HDFS. Ensuring the data is present avoids runtime errors.

**Reference:**  
See exercise 10 from lecture 02 for data generation steps.

##### **Step 3: Run the Spark Job on the Cluster**

**What you do:**  
Submit the Spark job to your cluster to process the Avro files.

**How:**
1. **Open a shell in your Spark client pod (if not already running):**
   ```bash
   kubectl run spark-cli --image=bitnami/spark:3.5.2-debian-12-r1 -- sleep infinity
   ```
   ```bash
   kubectl exec -it spark-cli -- bash
   ```
2. **Copy the script to the pod if needed:**
   ```bash
   kubectl cp avg-modalities-avro.py spark-cli:/opt/bitnami/spark/
   ```
3. **Run the Spark job with Avro support:**
   ```bash
   spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.2 avg-modalities-avro.py
   ```

**Why:**  
The `--packages` argument ensures Spark loads the Avro support library. `spark-submit` distributes the computation across the cluster.

##### **Step 4: Interpret the Output**

**What you do:**  
Review the output printed by the script. It should show the average `payload.modality` for each station, similar to the JSON exercise.

**Example Output:**
```
+--------+------------------+
|station |avg_modality      |
+--------+------------------+
|1       | 421.5            |
|2       | 398.2            |
|...     | ...              |
+--------+------------------+
```

**Why:**  
This output tells you the average modality value for each station, confirming your Spark job worked as intended.

##### **Summary Table**

| Step | What you do | Why | Technical background |
|------|-------------|-----|---------------------|
| 1 | Inspect `avg-modalities-avro.py` | Understand logic | Learn Spark DataFrame and Avro operations |
| 2 | Ensure Avro data in HDFS | Avoid errors | Spark reads from HDFS |
| 3 | Run `spark-submit` with Avro package | Process data at scale | Distributed computation, Avro support |
| 4 | Interpret output | Get results | Aggregation per station |

##### **Extra: What Does the Script Typically Look Like?**

````python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("AvgModalitiesAvro").getOrCreate()

# Adjust the path as needed
df = spark.read.format("avro").load("hdfs://namenode:9000/topics/INGESTION/*.avro")

# Compute average modality per station
result = (
    df.groupBy(col("station"))
      .agg(avg(col("payload.modality")).alias("avg_modality"))
)

result.show()
spark.stop()
````

##### **References**
- [Spark Avro Data Source](https://spark.apache.org/docs/latest/sql-data-sources-avro.html)
- [Spark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)

### Exercise 6 - Running Spark Streaming Jobs - Kafka

The objective of this exercise is to create a Spark streaming job that reads from a Kafka topic. This exercise requires
to have a Kafka producer which produces records in the given topic. For convenience, we recommend revisiting
the [exercise 4 from lecture 03](./../03/README.md#exercise-4---produce-messages-to-kafka-using-python).

**Task**: Create a streaming query that calculates the running mean of the six different stations (`payload.sensor_id`)
produced to the Kafka topic `INGESTION`.

**Help**: You need to complete the query inside the [process-streaming.py](process-streaming.py) file.
**Notice**: You need to append additional packages as arguments to run the Spark streaming application to read from
kafka. You can enable an interactive Spark streaming prompt using `pyspark` or submitting your final Spark application
using `spark-submit` as demonstrated below:

```bash
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 
```

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 process-streaming.py
```

**Task**: Run your Spark streaming application and validate that the running means of `payload.modality` field is close
to the calculated values in [exercise 4](README.md#exercise-4---average-sample-values-from-json-files-stored-in-hdfs).

**Important note**: There is no correct solution for this exercise. You may find inspiration in the following links to
complete the streaming query:

- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#structured-streaming-programming-guide)
- [Operations on streaming DataFrames/Datasets](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#operations-on-streaming-dataframesdatasets)
- [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#structured-streaming-kafka-integration-guide-kafka-broker-versio)


#### **Solution**
By following these steps, you learn how to build a real-time streaming analytics pipeline with Spark and Kafka, and how to compute running aggregates on live data.

**Step-by-Step Solution: Calculating Running Mean from Kafka with Spark Structured Streaming**

##### **Step 1: Understand the Goal and Prerequisites**

**What you do:**  
You need to create a Spark Structured Streaming job that reads messages from the Kafka topic `INGESTION` and calculates the running mean of the `payload.modality` field for each station (`payload.sensor_id`).

**Why:**  
Streaming analytics allows you to process data in real-time as it arrives, which is essential for many modern data applications.

**Technical background:**  
- Spark Structured Streaming provides high-level APIs for real-time data processing.
- Kafka is used as the streaming source.

##### **Step 2: Prepare Your Environment**

**What you do:**  
Ensure you have:
- A running Kafka cluster with the `INGESTION` topic.
- A Kafka producer sending records to the topic.
- Spark with the Kafka integration package.

**How:**  
You will need the following package when running your script:
```bash
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2
```
Create python client pod:
```bash
kubectl run python-cli --image=python:3.14-slim -- sleep infinity
```

Within the python-cli shell:
```bash
pip install kafka-python
```

```bash
mkdir e4
```

Located at ~/03/hints/
```bash
kubectl cp client.py python-cli:/e4
```

```bash
kubectl cp data_model.py python-cli:/e4
```

```bash
kubectl cp simple-producer.py python-cli:/e4
```

```bash
pip3 simple-producer.py
```

##### **Step 3: Inspect and Complete `process-streaming.py`**

**What you do:**  
Open `process-streaming.py` and ensure it:
- Reads from the Kafka topic.
- Parses the JSON payload.
- Extracts `payload.sensor_id` and `payload.modality`.
- Calculates the running mean per station.

**Why:**  
This is the core logic for real-time aggregation.

**Technical background:**  
- Spark reads Kafka messages as key-value pairs (both as bytes).
- You need to cast and parse the value as JSON.
- Use groupBy and aggregation functions for running means.

**Example code:**

````python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define schema for the JSON payload
schema = StructType([
    StructField("payload", StructType([
        StructField("sensor_id", StringType()),
        StructField("modality", DoubleType())
    ])),
    StructField("station", StringType())
])

spark = SparkSession.builder.appName("KafkaStreamingAvg").getOrCreate()

# Read from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "INGESTION")
    .option("startingOffsets", "earliest")
    .load()
)

# Parse the JSON value
json_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Extract fields
flat_df = json_df.select(
    col("data.station").alias("station"),
    col("data.payload.sensor_id").alias("sensor_id"),
    col("data.payload.modality").alias("modality")
)

# Calculate running mean per station
result = (
    flat_df.groupBy("station")
    .agg(avg("modality").alias("running_mean_modality"))
)

# Output to console
query = (
    result.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
````

##### **Step 4: Run the Streaming Job**

**What you do:**  
Submit your streaming job with the required Kafka package:

```bash
kubectl cp process-streaming.py spark-cli:/opt/bitnami/spark/e4
```

Then, within the spark-cli shell, run
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 process-streaming.py
```

**Why:**  
This command runs your streaming job, connecting Spark to Kafka.

##### **Step 5: Validate the Output**

**What you do:**  
Check the console output. You should see the running mean of `payload.modality` for each station, updating as new records arrive.

**Why:**  
This confirms your streaming job is working and producing real-time analytics.

##### **Summary Table**

| Step | What you do | Why | Technical background |
|------|-------------|-----|---------------------|
| 1 | Understand goal & prerequisites | Know what to build | Streaming analytics, Kafka, Spark |
| 2 | Prepare environment | Ensure all components are ready | Kafka, Spark, producer |
| 3 | Complete script | Implement logic | Structured Streaming, JSON parsing |
| 4 | Run job | Start real-time processing | `spark-submit` with Kafka package |
| 5 | Validate output | Confirm correctness | Console sink, running mean |


##### **References**
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)


##### Step-by-step guide to clean up

You will be using HDFS, Kafka and the interactive container in next lecture. However, if you will clean up the
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
    1. `helm delete spark`
- `cd` into the `lecture/03` folder in the repository.
    1. `kubectl delete -f redpanda.yaml`
    1. `kubectl delete -f kafka-schema-registry.yaml`
    1. `kubectl delete -f kafka-connect.yaml`
    1. `kubectl delete -f kafka-ksqldb.yaml`
    1. `helm uninstall kafka`
    1. `kubectl delete pvc data-kafka-controller-0 \
      data-kafka-controller-1 \
      data-kafka-controller-2
        `
- `cd` into the `services/interactive` folder in the repository.
    1. `kubectl delete -f interactive.yaml`
- cd into the `services/hdfs` folder in the repository.
    1. `kubectl delete -f hdfs-cli.yaml` (if used)
    1. `kubectl delete -f datanodes.yaml`
    1. `kubectl delete -f namenode.yaml`
    1. `kubectl delete -f configmap.yaml`

You can get a list of the pods and services to verify that they are deleted.

- `kubectl get all`
