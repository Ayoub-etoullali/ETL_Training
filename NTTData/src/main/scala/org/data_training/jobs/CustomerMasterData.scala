package org.data_training.jobs

import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.DateType
import org.data_training.Runnable
import org.data_training.engine.Engine

class CustomerMasterData(){

}/* extends Runnable{

  def run(spark: SparkSession, odate: String, engine: Engine, args: String*): Unit = {
    import spark.implicits._
    val schema = types.StructType(Array(
      types.StructField("id", types.StringType, true),
      types.StructField("uniqueId", types.StringType, true),
      types.StructField("zipCode", types.StringType, true),
      types.StructField("city", types.StringType, true),
      types.StructField("state", types.StringType, true),
    ))
    //Read customer_df and remove the " character
    var customer_df=spark.sql("SELECT * FROM ecom1.customers_dataset ")
    customer_df.columns.foreach(col_name => {
      customer_df = customer_df.withColumn(col_name, regexp_replace(customer_df(col_name), "\"", ""))
    })

    // Read orders_df and change type of order_estimated_delivery_date to a short date
    var orders_df = spark.sql("SELECT * FROM ecom1.orders_dataset ")
    orders_df.columns.foreach(col_name => {
      orders_df = orders_df.withColumn(col_name, regexp_replace(orders_df(col_name), "\"", ""))
    })
    orders_df = orders_df.withColumn("order_estimated_delivery_date1",$"order_estimated_delivery_date".cast(DateType))
    var drop_column = orders_df.drop($"order_estimated_delivery_date")
    var rename_column = orders_df.withColumnRenamed("order_estimated_delivery_date1", "order_estimated_delivery_date")


  }

  def JobsName2Log(): String = {
    "CustomerMasterData"
  }


}*/
