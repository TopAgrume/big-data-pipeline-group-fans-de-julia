import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object DataIngestion {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataIngestion")
      .master("local[*]")  // Use all cores
      .getOrCreate()

    val dataPath = "ecommerce_data_with_trends.csv"

    val df: DataFrame = loadData(spark, dataPath)

    df.show(10)
    spark.stop()
  }

  /**
   * A function to load data from a CSV file into a DataFrame.
   * @param spark - The active Spark session
   * @param path - Path to the data file
   * @return DataFrame containing the loaded data
   */
  def loadData(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }
}
