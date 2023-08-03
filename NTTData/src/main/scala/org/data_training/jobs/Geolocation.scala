package org.data_training.jobs

import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Geolocation() extends Runnable  {

  var geolocation_DF: DataFrame = _

  def run(spark: SparkSession, engine: Engine,args: String*): Unit = {
    print("############## starting Geolocation JOB ##############")

    geolocation_DF = spark.sql("SELECT * FROM geolocation_dataset ")

    print("############## processing Geolocation JOB ##############")

    val result = process()

    print("############## writing Geolocation JOB ##############")

    result.write.option("delimiter", ",").csv("/tmp/spark_output/geolocation.csv")

    print("##############  Geolocation JOB Finished ##############")
  }

  def process() = {

    geolocation_DF.withColumn("insertion_date", date_format(current_timestamp(), "yyyy-MM-dd"))


  }

  def JobsName2Log(): String = {
    "Geolocation"
  }


}
