package org.data_training.engine

import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.xml.crypto.Data
//import scala.sys.props


class Engine extends Constant {
  def init_spark(): SparkSession ={
    //props("HADOOP_USER_NAME") = "hive"
    val spark = SparkSession.builder()
      .master(spark_master)
      .appName(app_name)
      .config("spark.sql.warehouse.dir", spark_warehouse_dir)
      .config("hive.metastore.warehouse.dir", hive_metastore_dir)
      //.config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .config("hive.metastore.uris", hive_metastore_uris)
      .enableHiveSupport()
      .getOrCreate();
    spark
  }



}
