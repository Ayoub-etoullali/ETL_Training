package org.data_training.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{current_timestamp, date_format}

import java.text.SimpleDateFormat
import java.util.Date

class Common_functions(var format_date:Date) {


  def format_dates(odate:Column,isFormatted:Boolean): Column ={
    if (isFormatted){
       return odate
    }
    else{
      return date_format(odate,"yyyy-MM-dd")
    }
  }

}
