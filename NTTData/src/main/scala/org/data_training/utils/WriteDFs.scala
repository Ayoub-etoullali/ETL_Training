package org.data_training.utils
import org.apache.spark.sql.DataFrame
import org.data_training.engine.Constant


trait WriteDFs extends Constant {
  def write_df_to_hive(df: DataFrame, database: String, table_name: String, save_mode: String, columns_to_write: Seq[String]=Nil): Unit
  def write_df_to_hdfs(df: DataFrame, file_format: String=file_format, location_path: String=location_path, number_of_partitions: Int= number_of_partitions, columns_to_write: Seq[String]=Nil, options: Map[String,String]): Unit
  def write_df_to_postgres(df: DataFrame, database: String, table_name: String, save_mode: String, columns_to_write: Seq[String] = Nil): Unit

}
