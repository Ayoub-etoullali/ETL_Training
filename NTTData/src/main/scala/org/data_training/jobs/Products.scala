package org.data_training.jobs

import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Products() extends Runnable {

  var product_DF: DataFrame = _

  def run(spark: SparkSession, engine: Engine,args: String*): Unit = {
    print("############## starting Products JOB ##############")

    product_DF = spark.sql("SELECT * FROM products_dataset ")

    print("############## processing Products JOB ##############")

    val result = process()

    print("############## writing Products JOB ##############")

    result.write.option("delimiter", ",").csv("/tmp/spark_output/products.csv")

    print("##############  Products JOB Finished ##############")
  }

  def process() = {

    product_DF.withColumn("insertion_date", date_format(current_timestamp(), "yyyy-MM-dd"))


  }

  def JobsName2Log(): String = {
    "Products"
  }


}

