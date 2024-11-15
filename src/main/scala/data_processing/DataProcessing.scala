package data_processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


import breeze.linalg._
import breeze.plot._

object DataProcessing {

  def getCleanData(session: SparkSession, rawDf: DataFrame): DataFrame = {
    rawDf.createOrReplaceTempView("raw_transactions")
    val cleanedDf = cleanData(session)

    cleanedDf.show(10)

    cleanedDf.createOrReplaceTempView("cleaned_transactions")
    getTestMetrics(session)
    cleanedDf
  }

  def saveIntoFile(cleanedDf: DataFrame, filename: String): Unit = {
    cleanedDf.write
      .mode("overwrite")
      .partitionBy("transaction_date", "main_category")
      //.parquet("cleaned_ecommerce_data.parquet")
      .parquet(filename)
  }

  /**
   * Cleans and processes raw transaction data.
   *
   * This function performs several data cleaning and transformation steps on the raw transaction data:
   * - Parses and cleans timestamps and dates.
   * - Adds time-based metrics such AS transaction hour, day of the week, month, and year.
   * - Trims and standardizes customer and product information.
   * - Extracts main and sub-categories from the product category field.
   * - Cleans and validates numerical values: price, quantity, and total amount.
   * - Adds quality checks and flags for price calculation issues.
   * - Validates customer types and
   * - handles duplicated data.
   * @param spark The active Spark session.
   * @return DataFrame containing the cleaned and processed transaction data.
   */
  def cleanData(spark: SparkSession): DataFrame = {
    spark.sql("""
      -- Pre-process the data and replaces bad values by NULL
      WITH parsed_categories AS (
        SELECT
          transaction_id,
          -- Clean timestamp / date
          CASE
            WHEN timestamp IS NULL THEN NULL
            ELSE to_timestamp(timestamp)
          END AS transaction_timestamp,
          to_date(timestamp) AS transaction_date,

          -- Clean customer information
          customer_id,
          INITCAP(TRIM(customer_name)) AS customer_name,
          INITCAP(TRIM(city)) AS city,
          UPPER(TRIM(customer_type)) AS customer_type,

          -- Clean product information
          TRIM(product_name) AS product_name,

          -- Extract main and sub categories
          TRIM(SPLIT(category, '>')[0]) AS main_category,
          CASE
            WHEN SIZE(SPLIT(category, '>')) > 1 THEN TRIM(SPLIT(category, '>')[1])
            ELSE NULL
          END AS sub_category,

          -- Clean numerical values (irrelevant data)
          CASE
            WHEN price <= 0 OR price IS NULL THEN NULL
            ELSE ROUND(price, 2)
          END AS unit_price,
          CASE
            WHEN quantity <= 0 OR quantity IS NULL THEN NULL
            ELSE quantity
          END AS quantity,
          CASE
            WHEN total_amount <= 0 OR total_amount IS NULL THEN NULL
            ELSE ROUND(total_amount, 2)
          END AS total_amount,

          -- Validate total amount calculation
          ABS(ROUND(price * quantity, 2) - total_amount) AS price_calculation_diff

        FROM raw_transactions
      ),

      -- Add quality checks for filtering + new metrics
      validated_dataset AS (
        SELECT
          *,
          -- Flag price quality issues
          CASE
            WHEN price_calculation_diff > 1.0 THEN true
            ELSE false
          END AS has_price_problem,

          -- Add time-based metrics
          HOUR(transaction_timestamp) AS transaction_hour,
          DAYOFWEEK(transaction_date) AS transaction_day_of_week,
          MONTH(transaction_date) AS transaction_month,
          YEAR(transaction_date) AS transaction_year,

          -- Validate customer type
          CASE
            WHEN customer_type IN ('B2B', 'B2C') THEN customer_type
            ELSE 'UNKNOWN'
          END AS validated_customer_type,

          -- Add row number to handle duplicated data
          ROW_NUMBER() OVER (
            PARTITION BY
              transaction_id,
              customer_id,
              product_name,
              total_amount
            ORDER BY
              transaction_timestamp DESC  -- keep the most recent row
          ) AS row_number

        FROM parsed_categories
      )

      -- Final selection / filtering with valid data
      SELECT
        transaction_id,
        transaction_timestamp,
        transaction_date,
        transaction_hour,
        transaction_day_of_week,
        transaction_month,
        transaction_year,

        -- Customer data
        customer_id,
        customer_name,
        city,
        validated_customer_type AS customer_type,

        -- Product data
        product_name,
        main_category,
        sub_category,

        -- Price data
        unit_price,
        quantity,
        total_amount,
        has_price_problem -- To delete later

      FROM validated_dataset
      WHERE
        -- Filter out data with issues
        transaction_id IS NOT NULL
        AND transaction_timestamp IS NOT NULL
        AND customer_id IS NOT NULL
        AND unit_price IS NOT NULL
        AND quantity IS NOT NULL
        AND total_amount IS NOT NULL
        -- AND has_price_problem IS FALSE

        -- Filter duplicated row
        AND row_number = 1
      ORDER BY transaction_timestamp
    """)
  }

  // TODO remove tests for production (and the has_price_problem filter comment)
  def getTestMetrics(spark: SparkSession): Unit = {
    println("\n========== Transaction metrics ==========")

    spark.sql("""
      SELECT
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT *) AS distinct_transactions,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(DISTINCT product_name) AS unique_products,
        COUNT(DISTINCT main_category) AS unique_main_categories,

        -- Time range
        MIN(transaction_date) AS earliest_date,
        MAX(transaction_date) AS latest_date,

        -- Priec metrics
        ROUND(AVG(total_amount), 2) AS avg_transaction_value,
        ROUND(MIN(total_amount), 2) AS min_transaction_value,
        ROUND(MAX(total_amount), 2) AS max_transaction_value,

        -- Price quality metrics
        SUM(CASE WHEN has_price_problem THEN 1 ELSE 0 END) AS price_problem_count
      FROM cleaned_transactions
    """).show(false)

    println("\n========== Category distribution ==========")
    spark.sql("""
      SELECT
        main_category,
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT customer_id) AS unique_customers,
        ROUND(AVG(total_amount), 2) AS avg_transaction_value,
        ROUND(SUM(total_amount), 2) AS total_revenue
      FROM cleaned_transactions
      GROUP BY main_category
      ORDER BY total_revenue DESC
    """).show(false)

    println("\n========== Customer analysis ==========")
    spark.sql("""
      SELECT
        customer_type,
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT customer_id) AS unique_customers,
        ROUND(AVG(quantity), 2) AS avg_quantity,
        ROUND(AVG(total_amount), 2) AS avg_transaction_value,
        ROUND(SUM(total_amount), 2) AS total_revenue
      FROM cleaned_transactions
      GROUP BY customer_type
      ORDER BY total_revenue DESC
    """).show(false)

    println("\n========== Hourly patterns ==========")
    spark.sql("""
      SELECT
        transaction_hour,
        COUNT(*) AS transaction_count,
        ROUND(AVG(total_amount), 2) AS avg_transaction_value
      FROM cleaned_transactions
      GROUP BY transaction_hour
      ORDER BY transaction_hour
    """).show(false)

    println("\n========== Daily patterns ==========")
    spark.sql("""
      SELECT
        transaction_day_of_week,
        COUNT(*) AS transaction_count,
        ROUND(AVG(total_amount), 2) AS avg_transaction_value
      FROM cleaned_transactions
      GROUP BY transaction_day_of_week
      ORDER BY transaction_day_of_week
    """).show(false)
  }
  def getDescriptiveStatistics(spark: SparkSession): Unit = {
    println("\n========== Descriptive Statistics ==========")
    println("\n============ Mean, Median, Count ============")

    val df = spark.sql("SELECT * FROM cleaned_transactions")

    // Get mean, median, counts
    val statisticsDf = df.select(
      mean("unit_price").alias("mean_unit_price"),
      mean("quantity").alias("mean_quantity"),
      mean("total_amount").alias("mean_total_amount"),

      expr("percentile_approx(unit_price, 0.5)").alias("median_unit_price"),
      expr("percentile_approx(quantity, 0.5)").alias("median_quantity"),
      expr("percentile_approx(total_amount, 0.5)").alias("median_total_amount"),

      count("unit_price").alias("count_unit_price"),
      count("quantity").alias("count_quantity"),
      count("total_amount").alias("count_total_amount")
    )

    statisticsDf.show(false)

    println("\n========== Mode ==========")
    val modeDf = df.groupBy("unit_price", "quantity", "total_amount")
      .count()
      .orderBy(desc("count"))

    val unitPriceMode = modeDf.select("unit_price").head().getDouble(0)
    val quantityMode = modeDf.select("quantity").head().getInt(0)
    val totalAmountMode = modeDf.select("total_amount").head().getDouble(0)

    println(s"unit_price: $unitPriceMode")
    println(s"quantity: $quantityMode")
    println(s"total_amount: $totalAmountMode")
  }

  def getGraphicStatistics(spark: SparkSession): Unit = {
    import spark.implicits._
    import breeze.linalg._
    import breeze.plot._

    val df = spark.sql("SELECT total_amount, quantity, main_category FROM cleaned_transactions")

    // Distribution of Total Amount
    val totalAmounts = df.select("total_amount").as[Double].collect()
    val f1 = Figure()
    val p1 = f1.subplot(0)
    p1 += hist(DenseVector(totalAmounts), bins = 100)
    p1.title = "Total Amount Distribution"
    p1.xlabel = "Total Amount"
    p1.ylabel = "Frequency"
    f1.saveas("fig_total_amount_distribution.png")

    // Quantity vs Total Amount
    val quantityAmountPairs = df.select("quantity", "total_amount").as[(Int, Double)].collect()
    val quantities = DenseVector(quantityAmountPairs.map(_._1.toDouble))
    val amounts = DenseVector(quantityAmountPairs.map(_._2))

    val f2 = Figure()
    val p2 = f2.subplot(0)
    p2 += plot(quantities, amounts, '+')
    p2.title = "Quantity vs Total Amount"
    p2.xlabel = "Quantity"
    p2.ylabel = "Total Amount"
    f2.saveas("fig_quantity_vs_total_amount.png")

    // Total Amount by Category
    val totalAmountByCategory = df
      .groupBy("main_category")
      .agg(org.apache.spark.sql.functions.sum("total_amount").alias("total_amount"))
      .orderBy(desc("total_amount"))
      .collect()
      .map(row => (row.getString(0), row.getDouble(1)))

    val categoryIndices = DenseVector(totalAmountByCategory.indices.map(_.toDouble).toArray)
    val totalAmountsByCategory = DenseVector(totalAmountByCategory.map(_._2): _*)

    val totalAmountCategoriesLabel = "Total Amount by Category\n" + totalAmountByCategory.zipWithIndex.map {
        case ((category, _), index) => s"$index: $category"
      }.grouped(3)
      .map(_.mkString(", ")).mkString("\n")

    val f3 = Figure()
    val p3 = f3.subplot(0)
    for (i <- categoryIndices.data.indices) {
      p3 += plot(DenseVector(categoryIndices(i), categoryIndices(i)), DenseVector(0.0, totalAmountsByCategory(i)), style = '-')
    }
    p3.title = totalAmountCategoriesLabel
    p3.xlabel = "Category Index"
    p3.ylabel = "Total Amount"
    f3.saveas("fig_total_amount_by_category.png")

    // Total Transactions by Category
    val totalTransactionsByCategory = df
      .groupBy("main_category")
      .agg(org.apache.spark.sql.functions.count("total_amount").alias("total_transactions"))
      .orderBy(desc("total_transactions"))
      .collect()
      .map(row => (row.getString(0), row.getLong(1).toDouble))

    val transactionCountsByCategory = DenseVector(totalTransactionsByCategory.map(_._2): _*)

    val totalTransactionCategoriesLabel = "Total Transactions by Category\n" + totalTransactionsByCategory.zipWithIndex.map {
        case ((category, _), index) => s"$index: $category"
      }.grouped(3)
      .map(_.mkString(", ")).mkString("\n")

    val f4 = Figure()
    val p4 = f4.subplot(0)
    for (i <- categoryIndices.data.indices) {
      p4 += plot(DenseVector(categoryIndices(i), categoryIndices(i)), DenseVector(0.0, transactionCountsByCategory(i)), style = '-')
    }
    p4.title = totalTransactionCategoriesLabel
    p4.xlabel = "Category Index"
    p4.ylabel = "Transaction Count"
    f4.saveas("fig_total_transactions_by_category.png")

    println("Statistics Graphs Saved...")
  }

}

