// Databricks notebook source exported at Mon, 16 Nov 2015 20:44:10 UTC
val simpleData = Seq(("Group A","Section 1",50),("Group B","Section 2", 75), ("Group A", "Section 1", 25))

// COMMAND ----------

val simpleRdd = sc.parallelize(simpleData)

// COMMAND ----------

val simpleDF = sqlContext.createDataFrame(simpleRdd)

// COMMAND ----------

