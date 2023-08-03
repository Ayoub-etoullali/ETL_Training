package org.data_training.jobs

import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.{DataFrame, SparkSession, types}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Hdfs_into_postgres() extends Runnable{

  def run (spark: SparkSession, engine: Engine,args: String*): Unit = {

    val schema = types.StructType(Array(
      types.StructField("id", types.StringType, true),
      types.StructField("uniqueId", types.StringType, true),
      types.StructField("zipCode", types.StringType, true),
      types.StructField("city", types.StringType, true),
      types.StructField("state", types.StringType, true),
    ))

    var customer_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").schema(schema).option("encoding", "utf-8").load(args(0))
    customer_df.columns.foreach(col_name => {
      customer_df = customer_df.withColumn(col_name, regexp_replace(customer_df(col_name), "\"", ""))
    })

    println ("############## starting Hdfs_into_postgres JOB ##############")
    customer_df.printSchema()
    customer_df.show(4)

    customer_df.columns.mkString("|*|")


    println ("############## Hdfs_into_postgres JOB Finished ##############")
  }
  def JobsName2Log() :String = {
    "Hdfs_into_postgres"
  }


}
