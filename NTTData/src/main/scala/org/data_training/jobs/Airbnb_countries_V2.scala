package org.data_training.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.{Constant, Engine}
import org.yaml.snakeyaml.Yaml

import java.net.URI
import scala.collection.JavaConverters._


class Airbnb_countries extends Runnable with Constant {
  def run(spark: SparkSession, engine: Engine, args: String*): Unit = {
    println(s"-------------- Loading HDFS Files, spark User Name: ${System.getProperty("user.name")} ----------------")
    var fs = FileSystem.get(new URI(hdfs_host_server), new Configuration())
    val yaml_fileStream = fs.open(new Path(load_hdfs_to_dw_settings))
    val yaml = new Yaml()
    val data: java.util.List[java.util.Map[String, Any]] = yaml.load(yaml_fileStream).asInstanceOf[java.util.List[java.util.Map[String, Any]]]

    val countriesData: List[(String, Float, Float, Float, Float, String, Float)] = data.asScala.toList.map { entry =>
      val country = entry.get("Countries").asInstanceOf[String]
      val lat = entry.get("lat_destination").asInstanceOf[Float]
      val lng = entry.get("lng_destination").asInstanceOf[Float]
      val distance = entry.get("distance_km").asInstanceOf[Float]
      val area = entry.get("destination_km2").asInstanceOf[Float]
      val language = entry.get("destination_language").asInstanceOf[String]
      val levenshteinDistance = entry.get("language_levenshtein_distance").asInstanceOf[Float]
      (country, lat, lng, distance, area, language, levenshteinDistance)
    }

    import spark.implicits._
    val countriesDF: DataFrame = countriesData.toDF("country_destination", "lat_destination", "lng_destination", "distance_km", "destination_km2", "destination_language", "language_levenshtein_distance")

    val postgresConfig = Map(
      "url" -> "jdbc:postgresql://192.168.199.177:5432/airbnb_db",
      "user" -> "postgres",
      "password" -> "abJIbg3d53",
      "dbtable" -> "countries",
      "driver" -> "org.postgresql.Driver"
    )

    countriesDF.write
      .format("jdbc")
      .options(postgresConfig)
      .mode("append") // Change to "append" if you want to append data
      .save()

    println("------------ Data loaded into PostgreSQL table 'countries' --------------")
  }

  def JobsName2Log(): String = {
    "LoadDataToDW"
  }
}