// Databricks notebook source exported at Mon, 16 Nov 2015 21:34:55 UTC
val simpleData = Seq(("Group A","Section 1",50),("Group B","Section 2", 75), ("Group A", "Section 1", 25))

// COMMAND ----------

val simpleRdd = sc.parallelize(simpleData)

// COMMAND ----------

val simpleDF = sqlContext.createDataFrame(simpleRdd)

// COMMAND ----------

println("Hello Elizabeth")

// COMMAND ----------

simpleDF.registerTempTable("SimpleTempTable")

// COMMAND ----------

// MAGIC %sql Select * from SimpleTempTable

// COMMAND ----------

