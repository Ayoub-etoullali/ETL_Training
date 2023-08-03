package org.data_training.engine

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.utils.WriteDFs
import org.apache.spark.sql.functions.col


class WriteDataframes(spark: SparkSession) extends WriteDFs with Constant {
  var df_to_write: DataFrame=_
  import spark.implicits._
  override def write_df_to_hive(df: DataFrame, database: String, table_name: String, save_mode: String, columns_to_write: Seq[String]=Nil): Unit = {
    df_to_write=df
    if (df_to_write.count()!=0) {
      if (!columns_to_write.isEmpty) {
        //df_to_write=df.select(columns_to_write.map(col):_*);
        println(s"------------- Selecting Columns: Columns to select: $columns_to_write -----------")
        val col_names = columns_to_write.map(name => col(name))
        df_to_write = df.select(col_names: _*)
      }
      println(s"------------- Writing Hive : DataBase= $database, Table Name= $table_name, Save Mode: $save_mode -----------")
      df_to_write.write.mode(save_mode).format("hive").saveAsTable(s"$database.$table_name")
    }
  }

  override def write_df_to_hdfs(df: DataFrame, file_format: String=file_format, location_path: String=location_path, number_of_partitions: Int=number_of_partitions, columns_to_write: Seq[String]=Nil, options: Map[String,String]): Unit = {
    df_to_write=df
    if (df_to_write.count()!=0) {
      if (!columns_to_write.isEmpty) {
        //df_to_write = df.select(columns_to_write.map(col): _*);
        println(s"------------- Selecting Columns: Columns to select: $columns_to_write -----------")
        val col_names = columns_to_write.map(name => col(name))
        df_to_write = df.select(col_names: _*)
      }
      println(s"------------- Writing HDFS : format= $file_format, Number of Partitions= $number_of_partitions -----------")
      df_to_write.repartition(number_of_partitions).write.options(options).format(file_format).save(location_path);
    }
  }

  def write_df_to_postgres(df: DataFrame, database: String, table_name: String, save_mode: String, columns_to_write: Seq[String] = Nil): Unit = {
    df_to_write = df
    if (df_to_write.count() != 0) {
      if (!columns_to_write.isEmpty) {
        //df_to_write = df.select(columns_to_write.map(col): _*);
        println(s"------------- Selecting Columns: Columns to select: $columns_to_write -----------")
        val col_names = columns_to_write.map(name => col(name))
        df_to_write = df.select(col_names: _*)
      }

      // Modify this part to write DataFrame to the Postgres database
      println(s"------------- Writing Postgres : Database= $database, Table Name= $table_name, Save Mode: $save_mode -----------")

      val postgresUrl = "jdbc:postgresql://192.168.199.177:5432/airbnb_db"
      val postgresUser="postgres"
      val postgresPassword="abJIbg3d53"

      df_to_write.write
        .mode(save_mode)
        .format("jdbc")
        .option("url", postgresUrl)
        .option("dbtable", s"$database.$table_name")
        .option("user", postgresUser)
        .option("password", postgresPassword)
        .save()

      println(s"------------- DataFrame written to Postgres table: $table_name -----------")
    } else {
      println("------------- DataFrame is empty. Nothing to write to Postgres -----------")
    }
  }
}

