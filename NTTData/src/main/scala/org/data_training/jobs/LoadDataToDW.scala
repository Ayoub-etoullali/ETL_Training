package org.data_training.jobs

import org.data_training.engine.{Constant, Engine, ReadDataframes, WriteDataframes}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.data_training.Runnable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame}

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.yaml.snakeyaml.Yaml

import java.util.Map
import collection.JavaConverters.mapAsScalaMapConverter
import collection.JavaConverters._


class LoadDataToDW extends Runnable with Constant {
  def run (spark : SparkSession, engine: Engine ,args: String*): Unit={
    println(s"-------------- Loading HDFS Files, spark User Name: ${System.getProperty("user.name")} ----------------")
    var fs = FileSystem.get(new URI(hdfs_host_server), new Configuration())
    val yaml_fileStream =fs.open( new Path(load_hdfs_to_dw_settings))
    val yaml= new Yaml()
    val conf_data= yaml.load(yaml_fileStream).asInstanceOf[java.util.Map[String, Any]]

    val list_hdfs_to_load= conf_data.get("HDFS_FILE_PATHS").asInstanceOf[java.util.ArrayList[String]]
    println(s"----------- Print Settings: $conf_data -------------")
    println(s"----------- Print files paths: $list_hdfs_to_load -------------")
    val hdfs_options= conf_data.get("OPTIONS").asInstanceOf[java.util.Map[String,String]].asScala.toMap
    println(s"----------- Print Options: $hdfs_options ,type: ${hdfs_options.getClass}--------------")
    val columns_name= conf_data.get("COLUMNS_NAME").asInstanceOf[java.util.ArrayList[String]].asScala.asInstanceOf[Seq[String]]
    println(s"----------- Print Field's name $columns_name --------------")
    val header_option= hdfs_options.getOrElse("header", "true").toBoolean
    println(s"----------- Print Header Option $header_option,type: ${header_option.getClass} -------------")
    val hive_db= conf_data.get("HIVE_DATABASE").asInstanceOf[String]
    val hive_table= conf_data.get("HIVE_TABLE").asInstanceOf[String]
    val save_mode= conf_data.get("LOAD_MODE").asInstanceOf[String]
    val primary_key= conf_data.get("PRIMARY_KEY").asInstanceOf[String]

    assert(hive_table!="" || hive_db!="" || save_mode!="", "HIVE_DATABASE, HIVE_TABLE or SAVE_MODE is not configured in the YAML file!")
    println(s"----------- Create Database $hive_db if doesn't exist --------------")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $hive_db")

    spark.sql(s"USE $hive_db")
    var isTableExistsInDB= spark.sql("SHOW tables").filter(col("tableName")===hive_table).count()

    val readDFObj = new ReadDataframes(spark = spark)
    val writeDFObj = new WriteDataframes(spark = spark)
    val extension=conf_data.get("FILE_FORMAT").asInstanceOf[String]

    list_hdfs_to_load.asScala.foreach(file_path =>{
      val full_path = hdfs_host_server+file_path
      if (full_path.endsWith(extension)){
        println(s"----------- Reading $file_path -------------")
        var df= readDFObj.read_hdfs_df(file_path = full_path,file_format = extension, options = hdfs_options)
        if (!header_option){
          println("--------- Changing columns name ----------")
          df=df.toDF(columns_name.map(col_name =>col_name):_*)
        }
        if (isTableExistsInDB==0){
          println(s"------------ Create a Table $hive_table in $hive_db Database -------------")
          val fields_schema = df.schema.fields.map(f=> f.name+" "+f.dataType.typeName).mkString(",")
          val primary_key_check= ""//Option(primary_key).map(pk => ", PRIMARY KEY (" + pk +") DISABLE NOVALIDATE" ).getOrElse("")
          println(s"""CREATE TABLE IF NOT EXISTS $hive_db.$hive_table ($fields_schema $primary_key_check) """)//STORED AS ORC TBLPROPERTIES ('transactional'='true')""")
          spark.sql(s"""CREATE TABLE IF NOT EXISTS $hive_db.$hive_table ($fields_schema $primary_key_check)""")//STORED AS ORC TBLPROPERTIES ('transactional'='true')""")
          isTableExistsInDB = spark.sql(s"SHOW tables").filter(col("tableName")===hive_table).count()
        }

        println(s"------------ Checking table $hive_table is done, Is Table created: ${isTableExistsInDB>0} ---------------")
        //df= df.limit(10)
        var existed_hive_df = readDFObj.read_hive_df(database = hive_db, table_name = hive_table)

        println(s"-------------- Number of records in the dataset ${df.count()}, Number of records in Hive before write ${existed_hive_df.count()} -----------------")
        if (save_mode == "append" && existed_hive_df.count()>0) {
          println("------------- Removing duplicates before writing to hive ------------")
          df= df.except(existed_hive_df)
          println(s"------------ Number of records to write ${df.count()} --------------")
        }
        println(s"------------- Writing $file_path data into $hive_db.$hive_table ----------")
        writeDFObj.write_df_to_hive(df = df, database = hive_db, table_name = hive_table, save_mode = save_mode)
        existed_hive_df = readDFObj.read_hive_df(database = hive_db, table_name = hive_table)
        val count_of_df= df.count()
        val count_of_hive_df=existed_hive_df.count()
        println(s"------------- Load is successfully done: count files records: ${count_of_df}, count hive table records: ${count_of_hive_df} ------------ ")
      }
    })
  }

  def JobsName2Log(): String = {
    "LoadDataToDW"
  }
}
