import data_ingestion.DataIngestion
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("main")
      .master("local[*]")
      .getOrCreate()
    session.sparkContext.setLogLevel("WARN") // Disable for debugging

    val dataPath = "ecommerce_data_with_trends.csv"

    val df = DataIngestion.loadData(session, dataPath)
    df.show(10)
    session.stop()
  }
}