import data_ingestion.DataIngestion
import data_processing.DataProcessing.{getCleanData, saveIntoFile}
import nosql_processing.SqlAnalytics.showSqlAnalytics
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("main")
      .master("local[*]")
      .getOrCreate()
    session.sparkContext.setLogLevel("WARN") // Disable for debugging

    // ---- LOAD DATA ----
    val dataPath = "ecommerce_data_with_trends.csv"
    val rawDf = DataIngestion.loadData(session, dataPath)
    // --------------------

    rawDf.show(10)

    // --- PROCESS DATA ---
    val cleanedDf = getCleanData(session, rawDf)
    // ---------------------

    // ---- SAVE DATA ----
    // saveIntoFile(cleanedDf, "cleaned_ecommerce_data.parquet")
    // --------------------

    // - DATA SQL ANALYTICS -
    showSqlAnalytics(session)
    // ----------------------

    session.stop()
  }
}