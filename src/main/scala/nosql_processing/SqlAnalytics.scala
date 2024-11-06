package nosql_processing

import org.apache.spark.sql.SparkSession

object SqlAnalytics {
  def showSqlAnalytics(spark: SparkSession): Unit = {
    findTopCustomersByTotalSpend(spark)
    purchaseFrequencyByCustomerSegment(spark)
  }

  /**
   * Prints the top 5 customers by total amount spent on all purchases.
   *
   * @param spark The active Spark session.
   * @return Unit, this function only prints the first 20 rows of the Dataframe.
   */
  def findTopCustomersByTotalSpend(spark: SparkSession): Unit = {
    println("\n========== Top 5 customers by total spend ==========")

    spark.sql(
      """
      SELECT
        customer_id,
        customer_name,
        ROUND(SUM(total_amount), 2) AS total_spend
      FROM cleaned_transactions
      GROUP BY
        customer_id,
        customer_name
      ORDER BY total_spend DESC
      LIMIT 5
      """).show(false)
  }

  /**
   * Prints purchase frequency by customer segment for each month.
   *
   * 2 customer segments have been chosen : customer type and customer city.
   * @param spark The active Spark session.
   * @return Unit, this function only prints the first 20 rows of the Dataframes.
   */
  def purchaseFrequencyByCustomerSegment(spark: SparkSession): Unit = {
    println("\n========== Average purchase frequency per customer type per month ==========")

    spark.sql(
      """
      WITH customer_purchase_counts AS (
        SELECT
          customer_id,
          customer_type,
          DATE_TRUNC('month', transaction_timestamp) AS month,
          COUNT(*) AS month_purchases
        FROM cleaned_transactions
        GROUP BY
          customer_id,
          customer_type,
          DATE_TRUNC('month', transaction_timestamp)
      )
      SELECT
        customer_type,
        ROUND(AVG(month_purchases), 2) AS avg_purchase_frequency
      FROM customer_purchase_counts
      GROUP BY customer_type
      ORDER BY avg_purchase_frequency DESC
      """).show(false)


    println("\n========== Average purchase frequency per customer city per month ==========")

    spark.sql(
      """
      WITH customer_purchase_counts AS (
        SELECT
          customer_id,
          city,
          DATE_TRUNC('month', transaction_timestamp) AS month,
          COUNT(*) AS month_purchases
        FROM cleaned_transactions
        GROUP BY
          customer_id,
          city,
          DATE_TRUNC('month', transaction_timestamp)
      )
      SELECT
        city,
        ROUND(AVG(month_purchases), 2) AS avg_purchase_frequency
      FROM customer_purchase_counts
      GROUP BY city
      ORDER BY avg_purchase_frequency DESC
      """).show(false)
  }

  /**
   * @param spark The active Spark session.
   * @return Unit, this function only prints the first 20 rows of the Dataframes.
   */
  def identifyTrendsInProductPurchases(spark: SparkSession): Unit = {
    spark.sql(
      """

      """).show(false)
  }
}