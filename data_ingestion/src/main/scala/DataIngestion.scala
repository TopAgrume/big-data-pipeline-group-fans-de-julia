import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object DataIngestion {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataIngestion")
      .master("local[*]")  // Use all cores
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val dataPath = "ecommerce_data_with_trends.csv"
    val hdfsPath = "hdfs://namenode:9000/user/hadoop/ecommerce_data"  // HDFS saved data path

    val df: DataFrame = loadData(spark, dataPath)

    df.show(10)

    writeDataToHDFS(df, hdfsPath)

    spark.stop()
  }

  /**
   * A function to load data from a CSV file into a DataFrame.
   * @param spark - The active Spark session
   * @param path - Path to the data file
   * @return DataFrame containing the loaded data
   */
  private def loadData(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  private def writeDataToHDFS(df: DataFrame, hdfsPath: String): Unit = {
    df.write
      .mode("overwrite")
      .parquet(hdfsPath)  // Save as Parquet file, we can change this of we need another type of file.
  }
}
