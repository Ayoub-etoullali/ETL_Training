package org.data_training.jobs

import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Sellers() extends Runnable {

  var sellers_DF: DataFrame = _

  def run(spark: SparkSession, engine: Engine,args: String*): Unit = {
    print("############## starting Sellers JOB ##############")

    sellers_DF = spark.sql("SELECT * FROM sellers_dataset ")

    print("############## processing Sellers JOB ##############")

    val result = process()

    print("############## writing Sellers JOB ##############")

    result.write.option("delimiter", ",").csv("/tmp/spark_output/sellers.csv")

    print("##############  Sellers JOB Finished ##############")
  }

  def process() = {

    sellers_DF.withColumn("insertion_date", date_format(current_timestamp(), "yyyy-MM-dd"))


  }

  def JobsName2Log(): String = {
    "Sellers"
  }


}
