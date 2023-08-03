package org.data_training.rdbs
import java.sql.{Connection, DriverManager, Statement, ResultSet}

class MysqlConnection {

  def init_mysqlConnection(): Statement = {
    val url = "jdbc:mysql://10.105.75.116:3306/testConnection"
    val username = "root"
    val password = "passwdmysql22"

    Class.forName("com.mysql.cj.jdbc.Driver")

    val connection = DriverManager.getConnection(url)
    val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)


    statement
  }
}