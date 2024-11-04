package data_ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataIngestion {
  def loadData(session: SparkSession, dataPath: String) : DataFrame = {
    session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(dataPath)
  }
}
