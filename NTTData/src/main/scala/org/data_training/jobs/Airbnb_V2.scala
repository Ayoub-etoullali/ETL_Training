package org.data_training.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.{Constant, Engine, ReadDataframes, WriteDataframes}
import org.yaml.snakeyaml.Yaml

import java.net.URI
import scala.collection.JavaConverters._


class Airbnb_countries_V2 extends Runnable with Constant {
  def run(spark: SparkSession, engine: Engine, args: String*): Unit = {

    //SparkSession
    val spark_session = new Engine().init_spark()

    //Reading from HDFS
    val readFromHDFS = new ReadDataframes(spark_session).read_from_hdfs(hdfs_file_location, spark_session)

    //Writing to PostgreSQL
    new WriteDataframes(spark_session).write_to_postgresql(postgres_url, spark_session, readFromHDFS)
  }

    def JobsName2Log(): String = {
      "Airbnb_countries_V2"
    }
  }