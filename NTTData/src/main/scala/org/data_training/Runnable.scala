package org.data_training

import org.apache.spark.sql.SparkSession
import org.data_training.engine.{Constant, Engine}

trait Runnable extends Constant{
 def run (spark : SparkSession, engine: Engine ,args: String*)
 def JobsName2Log(): String

}
