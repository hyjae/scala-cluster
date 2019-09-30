package helper

import org.apache.spark.sql.{ DataFrame, SparkSession }

/**
 * https://stackoverflow.com/questions/1755345/difference-between-object-and-class-in-scala
 */
object CSVHelper {
  def loadCSVAsTable(spark: SparkSession, path: String, tableName: String): DataFrame = {
    val data = spark.read.format("com.databricks.spark.csv").
      option("header", "true").
      option("mode", "DROPMALFORMED").
      option("delimiter", ",").
      load(path)
    data.createOrReplaceTempView(tableName)
    data
  }

  def loadCSVAsTable(spark: SparkSession, path: String): DataFrame = {
    loadCSVAsTable(spark, path, inferTableNameFromPath(path))
  }

  private val pattern = "(\\w+)(\\.csv)?$".r.unanchored

  def inferTableNameFromPath(path: String): String = path match {
    case pattern(filename, extension) => filename
    case _                            => path
  }
}
