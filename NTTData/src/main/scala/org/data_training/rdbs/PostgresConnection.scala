package org.data_training.rdbs
import java.sql.{Connection, DriverManager, Statement, ResultSet}

class PostgresConnection {

  def init_postgresConnection(): Statement = {
    val url = "jdbc:postgresql://192.168.199.177:5432/airbnb_db"
    val username = "postgres"
    val password = "abJIbg3d53"

    Class.forName("org.postgresql.Driver")

    val connection = DriverManager.getConnection(url,username,password)
    val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)


    statement
  }
}