package nosql_processing

import org.apache.spark.sql.SparkSession

object SqlAnalytics {
  def showSqlAnalytics(spark: SparkSession): Unit = {
    findTopCustomersByTotalSpend(spark)
    purchaseFrequencyByCustomerSegment(spark)
    identifyTrendsInProductPurchases(spark)
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
          transaction_month,
          COUNT(*) AS month_purchases
        FROM cleaned_transactions
        GROUP BY
          customer_id,
          customer_type,
          transaction_month
      )
      SELECT
        customer_type,
        ROUND(AVG(month_purchases), 2) AS avg_purchase_frequency
      FROM customer_purchase_counts
      GROUP BY customer_type
      ORDER BY avg_purchase_frequency DESC
      """).show(false)


    println("\n========== Average purchase frequency per customer city per month (descending order) ==========")

    spark.sql(
      """
      WITH customer_purchase_counts AS (
        SELECT
          customer_id,
          city,
          transaction_month,
          COUNT(*) AS month_purchases
        FROM cleaned_transactions
        GROUP BY
          customer_id,
          city,
          transaction_month
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
   * Prints 2 different trends in product purchases over months.
   *
   *
   * @param spark The active Spark session.
   * @return Unit, this function only prints the first 20 rows of the Dataframes.
   */
  def identifyTrendsInProductPurchases(spark: SparkSession): Unit = {
    println("\n========== Evolution of the three categories with max total purchases per month ==========")

    spark.sql(
      """
      WITH monthly_category_sales AS (
        SELECT
          DATE_TRUNC('month', transaction_timestamp) AS month,
          main_category,
          COUNT(*) AS total_purchases
        FROM cleaned_transactions
        GROUP BY
          month,
          main_category
      ),
      ranked_category_sales AS (
        SELECT
          month,
          main_category,
          total_purchases,
          ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_purchases DESC) AS rank
        FROM monthly_category_sales
      )
      SELECT
        month,
        main_category,
        total_purchases
      FROM ranked_category_sales
      WHERE rank <= 3
      ORDER BY month;
      """).show(false)


    println("\n========== Evolution of monthly total revenue compared to previous month per category ==========")

    spark.sql(
      """
      WITH monthly_category_revenue AS (
        SELECT
          DATE_TRUNC('month', transaction_timestamp) AS month,
          main_category,
          SUM(total_amount) AS total_revenue
        FROM cleaned_transactions
        GROUP BY
          month,
          main_category
      ),
      monthly_category_revenue_growth AS (
        SELECT
          month,
          main_category,
          total_revenue,
          LAG(total_revenue) OVER (PARTITION BY main_category ORDER BY month) AS previous_month_revenue,
          (total_revenue - LAG(total_revenue) OVER (PARTITION BY main_category ORDER BY month)) / LAG(total_revenue) OVER (PARTITION BY main_category ORDER BY month) * 100 AS increase_revenue_percentage
        FROM monthly_category_revenue
      )
      SELECT
        month,
        main_category,
        total_revenue,
        CASE
          WHEN increase_revenue_percentage = NULL THEN 0
          ELSE ROUND(increase_revenue_percentage, 1)
        END AS r_increase_revenue_percentage
      FROM monthly_category_revenue_growth
      ORDER BY
        main_category,
        month;
      """).show(false)
  }
}