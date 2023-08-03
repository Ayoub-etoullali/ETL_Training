package org.data_training.jobs

import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Product_cat_ntt() extends Runnable {
  var product_Cat_ntDF: DataFrame = _

  def run(spark: SparkSession, engine: Engine,args: String*): Unit = {
    print("############## starting Product_cat_ntt JOB ##############")

    product_Cat_ntDF = spark.sql("SELECT * FROM product_category_name_translation ")

    print("############## processing Product_cat_ntt JOB ##############")

    val result = process()

    print("############## writing Product_cat_ntt JOB ##############")

    result.write.option("delimiter", ",").csv("/tmp/spark_output/product_category.csv")




    print("##############  Product_cat_ntt JOB Finished ##############")
  }

  def process() = {

    product_Cat_ntDF.withColumn("insertion_date", date_format(current_timestamp(), "yyyy-MM-dd"))


  }

  def JobsName2Log(): String = {
    "Product_cat_ntt"
  }


}
