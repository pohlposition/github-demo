// Databricks notebook source exported at Tue, 19 Jan 2016 19:08:27 UTC
// MAGIC %md
// MAGIC 
// MAGIC # **Ingest Logs**
// MAGIC This notebook ingests Apache access logs with Scala, persists them as Parquet Files, and exposes them as a Spark SQL Table
// MAGIC 
// MAGIC ![Spark Log Ingestion](/files/SparkLogIngestion.png)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 1:** Change this parameter to your sample logs directory

// COMMAND ----------

val DBFSSampleLogsFolder = "/dbguide/sample_logs"

// COMMAND ----------

// MAGIC %md ### **Step 2:** Create a parser for the Apache Access log lines to create case class objects

// COMMAND ----------

// MAGIC %md Create a Case Class for Appache Access Log Elements

// COMMAND ----------

case class ApacheAccessLog(ipAddress: String, clientIdentd: String,
                           userId: String, dateTime: String, method: String,
                           endpoint: String, protocol: String,
                           responseCode: Int, contentSize: Long) {

}

// COMMAND ----------

// MAGIC %md Define the parsing function

// COMMAND ----------

val Pattern = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

def parseLogLine(log: String): ApacheAccessLog = {
  val res = Pattern.findFirstMatchIn(log)
  if (res.isEmpty) {
    throw new RuntimeException("Cannot parse log line: " + log)
  }
  val m = res.get
  ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
    m.group(5), m.group(6), m.group(7), m.group(8).toInt, m.group(9).toLong)
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 3:** Load the log lines into Spark

// COMMAND ----------

// MAGIC %fs head dbfs:/dbguide/sample_logs/part-00001

// COMMAND ----------

val accessLogs = (sc.textFile(DBFSSampleLogsFolder)              
                  // Call the parse_apace_log_line function on each line.
                  .map(parseLogLine))
// Caches the objects in memory since they will be queried multiple times.
accessLogs.cache()
// An action must be called on the RDD to actually populate the cache.
accessLogs.count()

// COMMAND ----------

// MAGIC %md ### **Step 4:** Create a DataFrame from RDD
// MAGIC * Create PairRDD for ResponseCode (key) and Count (value)
// MAGIC * Create DataFrame from PairRDD
// MAGIC * Display the DataFrame

// COMMAND ----------

// First, calculate the response code to count pairs.
val responseCodeToCountPairRdd = (accessLogs
                                  .map(log => (log.responseCode, 1))
                                  .reduceByKey(_ + _))

// COMMAND ----------

// Convert the RDD of tuples to an RDD case classes
case class ResponseCodeToCount(responseCode: Int, count: Int)
// A simple map can convert the tuples to case classes and then call .toDF()
val responseCodeToCountDataFrame = responseCodeToCountPairRdd.map(t => ResponseCodeToCount(t._1, t._2)).toDF()

// COMMAND ----------

// Now, display can be called on the resulting Schema RDD.
//   For this display, a Pie Chart is chosen by selecting that icon.
//   Then, "Plot Options..." was used to make the responseCode the key and count as the value.
display(responseCodeToCountDataFrame)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 5:** View a list of IPAddresses that have accessed the server more than N times.

// COMMAND ----------

// Change this number as it makes sense for your data set.
val N = 20

// COMMAND ----------

case class IPAddressCaseClass(ipAddress: String)

val ipAddressesDataFrame = (accessLogs
                            .map(log => (log.ipAddress, 1))
                            .reduceByKey(_ + _)
                            .filter(_._2 > N)
                            .map(t => IPAddressCaseClass(t._1))).toDF()

// COMMAND ----------

display(ipAddressesDataFrame)

// COMMAND ----------

// MAGIC %md #### **Tip:** To see how an rdd is computed, use the **toDebugString()** function.

// COMMAND ----------

println(ipAddressesDataFrame.rdd.toDebugString)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 6:** Explore Statistics about the endpoints. 

// COMMAND ----------

// Calculate the number of hits for each endpoint.
case class EndpointCounts(endpoint: String, count: Int)

val endpointCountsRdd = (accessLogs
                         .map(log => (log.endpoint, 1))
                         .reduceByKey(_ + _))

val endpointCountsDataFrame = endpointCountsRdd.map(t => EndpointCounts(t._1, t._2)).toDF()

// COMMAND ----------

// Display a plot of the distribution of the number of hits across the endpoints.
//   Select the option to plot over all the results if you have more than 1000 data points.
display(endpointCountsDataFrame)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 7:** Save to Parquet and Register Table

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### **Extra: Use SQL in a Scala Notebook**
// MAGIC * Convert RDD to DataFrame
// MAGIC * Register DataFrame as a Spark SQL table
// MAGIC   * By default, it will persist in parquet format
// MAGIC   * Partition table by Year, Month, Day
// MAGIC * Then you can do SQL queries against the data in sql
// MAGIC * Results from select statements is SQL are automatically displayed in notebook

// COMMAND ----------

// MAGIC %md Convert DateTime from String to Timestamp

// COMMAND ----------

val accessLogsDf = accessLogs.toDF().selectExpr("*","CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(dateTime, 'dd/MMM/yyyy:HH:mm:ss Z')) AS TIMESTAMP) AS requestTime")

// COMMAND ----------

accessLogsDf.printSchema

// COMMAND ----------

// MAGIC %md Parse out Year, Month, Day

// COMMAND ----------

val accessLogsParsedTime = accessLogsDf.selectExpr("*", "year(requestTime) as year", "month(requestTime) as month","day(requestTime) as day")

// COMMAND ----------

display(accessLogsParsedTime)

// COMMAND ----------

// MAGIC %sql DROP TABLE IF EXISTS SQLApacheAccessLogsTable

// COMMAND ----------

accessLogsParsedTime.write.partitionBy("year","month","day").saveAsTable("SQLApacheAccessLogsTable")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 8:** Query the registered table with Spark SQL

// COMMAND ----------

// MAGIC %sql select * from SQLApacheAccessLogsTable

// COMMAND ----------

// MAGIC %sql describe SQLApacheAccessLogsTable

// COMMAND ----------

// MAGIC %sql show create table SQLApacheAccessLogsTable

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/sqlapacheaccesslogstable/year=2014/month=7/day=21

// COMMAND ----------

// MAGIC %sql SELECT * FROM SQLApacheAccessLogsTable limit 10;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC to_date(requestTime) as requestTime,
// MAGIC responseCode,
// MAGIC count(*) requestCount
// MAGIC FROM SQLApacheAccessLogsTable
// MAGIC GROUP BY
// MAGIC to_date(requestTime), responseCode
// MAGIC ORDER BY
// MAGIC to_date(requestTime), responseCode

// COMMAND ----------

// MAGIC %md ##Query Predicate Pushdown
// MAGIC * Only scan and ingest files in sqlapacheaccesslogstable/year=2014/month=5

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC responseCode, count(*) as count 
// MAGIC FROM SQLApacheAccessLogsTable 
// MAGIC WHERE year = 2014 AND month = 5
// MAGIC GROUP BY
// MAGIC responseCode

// COMMAND ----------

// MAGIC %md #File Size Reduction
// MAGIC * Parquet optimizes the layout of data (especially with partitioning) and can reduce the overall size of data to be persisted

// COMMAND ----------

val inputFileList = dbutils.fs.ls(DBFSSampleLogsFolder)

// COMMAND ----------

val inputFileListDf = sc.parallelize(inputFileList).toDF()

// COMMAND ----------

import org.apache.spark.sql.functions._
val totalInputFileSize = inputFileListDf.agg(sum("size")).collect()(0).getLong(0)

// COMMAND ----------

val outputFiles = dbutils.fs.ls("/user/hive/warehouse/sqlapacheaccesslogstable")

// COMMAND ----------

val outputFileSizes = sc.wholeTextFiles("/user/hive/warehouse/sqlapacheaccesslogstable/*/*/*").map(x => x._2.getBytes.size)

// COMMAND ----------

val totalOutputFileSize = outputFileSizes.reduce(_ + _)

// COMMAND ----------

val pctReduction = 100 * (1 - (totalOutputFileSize.toFloat / totalInputFileSize.toFloat))

// COMMAND ----------

println(s"Total Size of Input Files : ${totalInputFileSize}")
println(s"Total Size of Output Files: ${totalOutputFileSize}")
println(s"Reduction in File Size    : ${pctReduction}%")

// COMMAND ----------

