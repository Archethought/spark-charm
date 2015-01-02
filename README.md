# Overview

Apache Spark™ is a fast and general purpose engine for large-scale data processing.
Key features:
**Speed:**
Run programs up to 100x faster than Hadoop MapReduce in memory, or 10x faster on disk.
Spark has an advanced DAG execution engine that supports cyclic data flow and in-memory computing.
**Ease of Use:**
Write applications quickly in Java, Scala or Python.
Spark offers over 80 high-level operators that make it easy to build parallel apps. And you can use it interactively from the Scala and Python shells.
**General Purpose Engine:**
Combine SQL, streaming, and complex analytics.
Spark powers a stack of high-level tools including Shark for SQL, MLlib for machine learning, GraphX, and Spark Streaming. You can combine these frameworks seamlessly in the same application.
**Integrated with other Cluster Managers: YARN, EC2, Mesos**
Spark can run on Hadoop 2's YARN cluster manager, and can read any existing Hadoop data.
If you have a Hadoop 2 cluster, you can run Spark without any installation needed. Otherwise, Spark is easy to run standalone or on EC2 or Mesos. It can read from HDFS, HBase, Cassandra, and any Hadoop data source.
 
For more details <http://spark.apache.org/>

# Usage
Supported configurations:
   **Spark Standalone cluster deployment**
   One Spark Master node and 1 to many Spark slave nodes:
    juju deploy spark spark-master
    juju deploy spark spark-slave
    juju add-relation spark-master:master spark-slave:slave
   

## Smoke tests after deployment 
    # Spark admins use ssh to access spark console from master node
    1) juju ssh spark-master/0  <<= ssh to spark master
    2) Use spark-submit to run your application:
    spark-submit --class org.apache.spark.examples.SparkPi  /usr/lib/spark/lib/spark-examples-1.0.0-hadoop2.2.0.jar
    you should get pi = 3.14
    
    3) Spark’s shell provides a simple way to learn the API, as well as a powerful tool to analyze data interactively. It is available in either Scala or Python. Start it by running the following in the Spark directory:
    $spark-shell <== for interaction using scala 
    $pyspark     <== for interaction using python
     

# Configuration
   **From Master node only**
   **actions**
     Select one of following spark cluster management operation, must type "none" for no operation:
     none, spark-submit, start-master, start-slaves, start-all, stop-master, stop-slaves, stop-all.
     * none         - do nothing
     * spark-submit - submit a spark job to spark cluster. MUST have "spark_job_class" and "application_jar"
     * start-master - Starts master on master node. 
     * start-slaves - Starts a slave instance on each slave node.
     * start-all    - Starts both master and all slaves.
     * stop-master  - Stops the master.
     * stop-slaves  - Stops all slave instances on the nodes registered to master node.
     * stop-all     - Stops both the master and the slaves nodes.

  **spark_executor_memory**
       Amount of memory to use per executor process, in the same format as 
       JVM memory strings-512m is default (e.g. 512m, 2g).

  **spark_total_executor_cores**
       Number of cores avaliable to executor process
  **spark_job_class**
       The entry point for your application (e.g. org.apache.spark.examples.SparkPi)
  **application_jar**
      Path to a bundled jar including your application and all dependencies. The URL must be globally visible inside of your cluster, for instance, an hdfs:// path or a file:// path that is present on all nodes (i.e /usr/lib/spark/lib/spark-examples-1.0.0-hadoop2.2.0.jar).
  **application-arguments**
      Arguments passed to the main method of your main class, if any
  **spark_serializer** (currently disabled) 
      Spark can also use the Kryo library (version 2) to serialize objects more quickly


# Contact Information
amir sanjar <amir.sanjar@canonical.com>
## Upstream Project Name
<http://spark.apache.org/>
