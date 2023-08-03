package org.data_training.jobs

import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Orders() extends Runnable {

  var orders_DF: DataFrame = _

  def run(spark: SparkSession, engine: Engine,args: String*): Unit = {
    print("############## starting Orders JOB ##############")

    orders_DF = spark.sql("SELECT * FROM orders_dataset ")

    print("############## processing Orders JOB ##############")

    val result = process()

    print("############## writing Orders JOB ##############")

    result.write.option("delimiter", ",").csv("/tmp/spark_output/orders.csv")

    print("##############  Orders JOB Finished ##############")
  }

  def process() = {

    orders_DF.withColumn("insertion_date", date_format(current_timestamp(), "yyyy-MM-dd"))


  }

  def JobsName2Log(): String = {
    "Orders"
  }


}

