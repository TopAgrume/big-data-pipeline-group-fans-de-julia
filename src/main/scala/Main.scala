import data_ingestion.DataIngestion
import data_processing.DataProcessing.{getCleanData, getTestMetrics, saveIntoFile}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("main")
      .master("local[*]")
      .getOrCreate()
    session.sparkContext.setLogLevel("WARN") // Disable for debugging

    // ---- LOAD DATAS ----
    val dataPath = "ecommerce_data_with_trends.csv"
    val rawDf = DataIngestion.loadData(session, dataPath)
    // --------------------

    rawDf.show(10)

    // --- PROCESS DATAS ---
    val cleanedDf = getCleanData(session, rawDf)
    // ---------------------

    // ---- SAVE DATAS ----
    saveIntoFile(cleanedDf, "cleaned_ecommerce_data.parquet")
    // --------------------

    session.stop()
  }
}