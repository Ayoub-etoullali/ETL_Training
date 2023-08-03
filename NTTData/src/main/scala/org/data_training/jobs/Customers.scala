package org.data_training.jobs

import org.data_training.Runnable
import org.apache.spark.sql.functions.{column, current_timestamp, date_format, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession, types}
import org.data_training.engine.{Engine, ReadDataframes, WriteDataframes}


case class Customers() extends Runnable {
  var customers_df: DataFrame=_
  def run(spark: SparkSession, engine: Engine, args: String*): Unit = {
    println("############## starting Customers JOB ##############")
    val schema = types.StructType(Array(
      types.StructField("customer_id", types.StringType, true),
      types.StructField("customer_unique_id", types.StringType, true),
      types.StructField("customer_city", types.StringType, true),
      types.StructField("customer_state", types.StringType, true),
    ))

    val readDFObj= new ReadDataframes(spark = spark)
    val writeDFObj= new WriteDataframes(spark=spark)

    val df_hdfs_test=readDFObj.read_hdfs_df(file_path ="hdfs://192.168.182.6:8020/hive/warehouse/processEcomData/customers_dataset_final/part-00000-43f05c10-49a2-43de-ace3-0e1853499889-c000.csv"
    ,file_format = "csv",options = Map("delimiter"->";"))

    df_hdfs_test.show(20)
    /*customers_df= readDFObj.read_hive_df(database = "ecom1",table_name = "customers_dataset",
                  columns_to_read = List("customer_id","customer_unique_id","customer_city","customer_state"),clause = "WHERE customer_state = 'SP'")

    customers_df.show(10)

    val customers_sp_state_df=customers_df.filter(customers_df("customer_state")==="SP")

    writeDFObj.write_df_to_hive(df=customers_sp_state_df,
      database = "ecom1",table_name = "sp_table_from_spark", save_mode = "overwrite")

    writeDFObj.write_df_to_hdfs(df= customers_sp_state_df, file_format = "csv",
      location_path = "hdfs://192.168.182.6:8020/hive/warehouse/processEcomData/customers_dataset_final",
      number_of_partitions = 2, options = Map("delimiter"->";"),
      columns_to_write = Seq[String]("customer_id","customer_unique_id","customer_city","customer_state")
    )*/






    /*customers_DF.columns.foreach(col_name => {
      customers_DF = customers_DF.withColumn(col_name, regexp_replace(customers_DF(col_name), "\"", ""))
    })
    customers_DF.show(5)
    println("############## processing customers JOB ##############")

    val result = process()*/

    /*println("############## writing customers JOB ##############")
    result.show(3)
    result.coalesce(1).write.option("delimiter", ",").csv("hdfs://192.168.182.6:8020/hive/warehouse/processEcomData/customers_dataset_final")

    println("##############  Customers JOB Finished ###s###########")
    println(s"working dir: ${System.getProperty("user.dir")}")*/
  }

  def process() = {
    //customers_DF.withColumn("insertion_date", date_format(current_timestamp(), "yyyy-MM-dd"))
  }

  def JobsName2Log(): String = {
    "Customers"
  }


}
