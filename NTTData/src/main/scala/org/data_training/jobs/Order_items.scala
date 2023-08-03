package org.data_training.jobs

import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Order_items() extends Runnable {

  var order_items_DF: DataFrame = _

  def run(spark: SparkSession, engine: Engine,args: String*): Unit = {
    print("############## starting Order_items JOB ##############")

    order_items_DF = spark.sql("SELECT * FROM order_items_dataset ")

    print("############## processing Order_items JOB ##############")

    val result = process()

    print("############## writing Order_items JOB ##############")

    result.write.option("delimiter", ",").csv("/tmp/spark_output/order_items.csv")

    print("##############  Order_items JOB Finished ##############")
  }

  def process() = {

    order_items_DF.withColumn("insertion_date", date_format(current_timestamp(), "yyyy-MM-dd"))


  }

  def JobsName2Log(): String = {
    "Order_items"
  }


}

