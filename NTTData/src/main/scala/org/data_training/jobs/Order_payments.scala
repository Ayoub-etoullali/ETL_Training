package org.data_training.jobs

import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Order_payments() extends Runnable {

  var order_payments_DF: DataFrame = _

  def run(spark: SparkSession, engine: Engine,args: String*): Unit = {
    print("############## starting Order_payments JOB ##############")

    order_payments_DF = spark.sql("SELECT * FROM order_payments_dataset ")

    print("############## processing Order_payments JOB ##############")

    val result = process()

    print("############## writing Order_payments JOB ##############")

    result.write.option("delimiter", ",").csv("/tmp/spark_output/order_payments.csv")

    print("##############  Order_payments JOB Finished ##############")
  }

  def process() = {

    order_payments_DF.withColumn("insertion_date", date_format(current_timestamp(), "yyyy-MM-dd"))


  }

  def JobsName2Log(): String = {
    "Order_payments"
  }


}

