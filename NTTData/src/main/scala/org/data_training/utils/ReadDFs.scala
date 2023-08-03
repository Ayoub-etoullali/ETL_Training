package org.data_training.utils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.data_training.engine.Constant


trait ReadDFs extends Constant{
    def read_hive_df(database: String, table_name: String, clause: String="", columns_to_read: List[String]=Nil): DataFrame
    def read_hdfs_df(file_path: String, file_format: String= file_format, schema: StructType= new StructType(), options: Map[String,String]=Map(), clause: String="", columns_to_read: List[String]=Nil): DataFrame
    def read_postgresql_df():Unit
}
