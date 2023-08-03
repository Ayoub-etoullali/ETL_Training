package org.data_training.jobs

import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Order_reviews() extends Runnable {

  var order_reviews_DF: DataFrame = _

  def run(spark: SparkSession, engine: Engine,args: String*): Unit = {
    print("############## starting Order_reviews JOB ##############")

    order_reviews_DF = spark.sql("SELECT * FROM order_reviews_dataset ")

    print("############## processing Order_reviews JOB ##############")

    val result = process()

    print("############## writing Order_reviews JOB ##############")

    result.write.option("delimiter", ",").csv("/tmp/spark_output/order_reviews.csv")

    print("##############  Order_reviews JOB Finished ##############")
  }

  def process() = {

    order_reviews_DF.withColumn("insertion_date", date_format(current_timestamp(), "yyyy-MM-dd"))


  }

  def JobsName2Log(): String = {
    "Order_reviews"
  }
}
