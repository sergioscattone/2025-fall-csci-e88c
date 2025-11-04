package org.cscie88c.spark

import java.sql.Timestamp
import org.apache.spark.sql.{SparkSession, Dataset, functions => F}

case class TaxiTrip(
  VendorID: Option[Long],
  tpep_pickup_datetime: Option[Timestamp],
  tpep_dropoff_datetime: Option[Timestamp],
  passenger_count: Option[Long],
  trip_distance: Option[Double],
  RatecodeID: Option[Long],
  store_and_fwd_flag: Option[String],
  PULocationID: Option[Long],
  DOLocationID: Option[Long],
  payment_type: Option[Long],
  fare_amount: Option[Double],
  extra: Option[Double],
  mta_tax: Option[Double],
  tip_amount: Option[Double],
  tolls_amount: Option[Double],
  improvement_surcharge: Option[Double],
  total_amount: Option[Double],
  congestion_surcharge: Option[Double],
  airport_fee: Option[Double]
)

case class TaxiZone(
  LocationID: Long,
  Borough: String,
  Zone: String,
  service_zone: String
)

/**
 * Weekly KPI metrics by borough.
 * 
 * @param week_start Week starting date (yyyy-MM-dd format)
 * @param borough Borough identifier (based on pickup location)
 * @param trip_volume Number of trips in this borough for this week
 * @param total_trips Total trips across all boroughs for this week
 * @param total_revenue Total revenue across all boroughs for this week
 * @param peak_hour The hour (0-23) with the highest trip volume
 * @param peak_hour_trip_percentage Percentage of trips in the busiest hour
 * @param avg_minutes_per_mile Average trip duration per mile traveled
 * @param avg_revenue_per_mile Average revenue earned per mile traveled
 * @param night_trip_percentage Percentage of trips during night hours (midnight-6am)
 */
case class ProjectKPIs(
  week_start: String,
  borough: String,
  trip_volume: Long,
  total_trips: Long,
  total_revenue: Double,
  peak_hour: Int,
  peak_hour_trip_percentage: Double,
  avg_minutes_per_mile: Long,
  avg_revenue_per_mile: Double,
  night_trip_percentage: Double
)

object SparkJob {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: SparkJob <input_file> <output_path> <taxi_zone_lookup_csv> [weeks]")
      System.exit(1)
    }
    
    val infile = args(0)
    val taxiZoneLookupFile = args(1)
    val outpath = args(2)
    
    implicit val spark = SparkSession.builder()
      .appName("Week8GroupProject")
      // do not hardcode master here so spark-submit's --master is respected
      .getOrCreate()

    // Basic run logging to help debugging when running inside Docker
    println(s"[SparkJob] args=${args.mkString(" ")}")
    spark.sparkContext.setLogLevel("WARN")

    try {
      // 1. Load taxi zone lookup data
      println(s"[SparkJob] Loading taxi zone lookup data from: $taxiZoneLookupFile")
      val taxiZones: Dataset[TaxiZone] = loadTaxiZoneLookup(taxiZoneLookupFile)
      println(s"[SparkJob] Loaded ${taxiZones.count()} taxi zones")

      // 2. Load input file (bronze layer)
      println(s"[SparkJob] Loading input: $infile")
      val rawTaxiTrips: Dataset[TaxiTrip] = loadInputFile(infile)
      println(s"[SparkJob] Loaded input rows=${rawTaxiTrips.count()}")

      // 3. Cleanup data (silver layer)
      println("[SparkJob] Cleaning data")
      val cleanTaxiTrips: Dataset[TaxiTrip] = cleanData(rawTaxiTrips.toDF())
      println(s"[SparkJob] Cleaned rows=${cleanTaxiTrips.count()}")

      // 4. Calculate aggregate KPIs (gold layer)
      println("[SparkJob] Calculating KPIs")
      // optional 4th arg: number of weeks to include (default 4)
      val weeksToInclude: Int = if (args.length >= 4) try { args(3).toInt } catch { case _: Throwable => 4 } else 4
      val projectKPIs: Dataset[ProjectKPIs] = calculateKPIs(cleanTaxiTrips, taxiZones, weeksToInclude, outpath)
      println(s"[SparkJob] KPIs rows=${projectKPIs.count()}")
      projectKPIs.show(false)

      // 4. Save output
  val kpisOut = if (outpath.endsWith("/")) outpath + "kpis" else outpath + "/kpis"
  println(s"[SparkJob] Saving KPIs to: $kpisOut")
  saveOutput(projectKPIs, kpisOut)
  println("[SparkJob] Save complete")

      spark.stop()
    } catch {
      case t: Throwable =>
        println(s"[SparkJob][ERROR] Job failed: ${t.getMessage}")
        t.printStackTrace()
        try { spark.stop() } catch { case _: Throwable => }
        // rethrow to ensure spark-submit sees a non-zero exit
        System.exit(1)
    }
  }

  def loadInputFile(filePath: String)(implicit spark: SparkSession): Dataset[TaxiTrip] = {
    import spark.implicits._
    
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet(filePath)
      .as[TaxiTrip]
  }

  def loadTaxiZoneLookup(filePath: String)(implicit spark: SparkSession): Dataset[TaxiZone] = {
    import spark.implicits._
    
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[TaxiZone]
  }

  /**
   * Validates data quality and cleans taxi trip data.
   * Performs fail-fast validation checks followed by row-level filtering.
   */
  def cleanData(inputData: org.apache.spark.sql.DataFrame): org.apache.spark.sql.Dataset[TaxiTrip] = {
    val df = inputData
    import df.sparkSession.implicits._

    // Step 1: Data quality validation (fail-fast on critical issues)
    validateDataQuality(df)

    // Step 2: Apply row-level filters to remove invalid records
    val cleaned = applyDataFilters(df)

    // Convert to typed Dataset
    cleaned.as[TaxiTrip]
  }

  /**
   * Validates data quality with fail-fast checks:
   * - Required columns present
   * - Null rates below threshold
   * - No duplicate IDs
   * - Complete time series (no missing weeks)
   */
  private def validateDataQuality(df: org.apache.spark.sql.DataFrame): Unit = {
    val totalRows = df.count()
    val requiredCols = Seq(
      "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
      "trip_distance", "fare_amount", "total_amount"
    )

    // Check for missing columns
    val missingCols = requiredCols.filterNot(df.columns.contains)
    if (missingCols.nonEmpty) {
      throw new IllegalArgumentException(s"Missing required columns: ${missingCols.mkString(", ")}")
    }

    // Check null rates
    val nullRateThreshold = 0.20
    val highNulls = requiredCols.map { c =>
      val nullCount = df.filter(F.col(c).isNull).count()
      val rate = if (totalRows == 0) 1.0 else nullCount.toDouble / totalRows
      (c, rate)
    }.filter { case (_, rate) => rate > nullRateThreshold }

    if (highNulls.nonEmpty) {
      val msg = highNulls.map { case (c, r) => f"$c:${r * 100}%.1f%%" }.mkString(", ")
      throw new RuntimeException(s"High null rates detected: $msg")
    }

    // Check for duplicate IDs if present
    if (df.columns.contains("trip_id")) {
      val hasDuplicates = df.groupBy("trip_id").count()
        .filter(F.col("count") > 1).limit(1).count() > 0
      if (hasDuplicates) {
        throw new RuntimeException("Duplicate trip_id values found")
      }
    }

    // Check time series completeness
    validateTimeSeriesCompleteness(df)
  }

  /**
   * Validates that there are no missing weeks in the time series.
   */
  private def validateTimeSeriesCompleteness(df: org.apache.spark.sql.DataFrame): Unit = {
    import df.sparkSession.implicits._
    
    val dfWithTs = df.withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    
    val (minPickupOpt, maxPickupOpt) = (
      dfWithTs.agg(F.min("pickup_ts")).as[java.sql.Timestamp].collect().headOption,
      dfWithTs.agg(F.max("pickup_ts")).as[java.sql.Timestamp].collect().headOption
    )

    if (minPickupOpt.isEmpty || maxPickupOpt.isEmpty) {
      throw new RuntimeException("No valid pickup timestamps found")
    }

    val minDate = minPickupOpt.get.toInstant.atZone(java.time.ZoneId.systemDefault()).toLocalDate
    val maxDate = maxPickupOpt.get.toInstant.atZone(java.time.ZoneId.systemDefault()).toLocalDate
    val expectedWeeks = java.time.temporal.ChronoUnit.WEEKS.between(minDate, maxDate).toInt + 1

    val actualWeeks = dfWithTs
      .withColumn("week", F.concat_ws("-",
        F.year(F.col("pickup_ts")).cast("string"),
        F.lpad(F.weekofyear(F.col("pickup_ts")).cast("string"), 2, "0")
      ))
      .select("week").distinct().count()

    if (actualWeeks < expectedWeeks) {
      throw new RuntimeException(
        f"Incomplete time series: found $actualWeeks weeks, expected $expectedWeeks"
      )
    }
  }

  /**
   * Applies row-level filters to remove invalid trip records:
   * - Non-negative fares and totals
   * - Positive trip distances
   * - Valid passenger counts (0-8)
   * - Valid timestamps (pickup before dropoff)
   * - Non-negative tips
   * - Valid payment types (1-6)
   */
  private def applyDataFilters(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    df
      .filter(F.col("fare_amount").isNotNull && F.col("fare_amount") >= 0)
      .filter(F.col("total_amount").isNotNull && F.col("total_amount") >= 0)
      .filter(F.col("trip_distance").isNotNull && F.col("trip_distance") > 0)
      .filter(F.col("passenger_count").isNotNull && F.col("passenger_count").between(0, 8))
      .filter(F.col("tpep_pickup_datetime").isNotNull && F.col("tpep_dropoff_datetime").isNotNull)
      .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
      .filter(F.col("pickup_ts").isNotNull && F.col("dropoff_ts").isNotNull)
      .filter(F.col("dropoff_ts") >= F.col("pickup_ts"))
      .filter(F.col("tip_amount").isNull || F.col("tip_amount") >= 0)
      .filter(F.col("payment_type").isNull || F.col("payment_type").between(1, 6))
      .drop("pickup_ts", "dropoff_ts")
  }

  /**
   * Calculates KPIs from cleaned taxi trip data.
   * Filters to recent weeks, computes individual KPIs, and aggregates by week and borough.
   */
  def calculateKPIs(inputData: org.apache.spark.sql.Dataset[TaxiTrip],
                    taxiZones: org.apache.spark.sql.Dataset[TaxiZone],
                    weeks: Int = 4,
                    outputPath: String)(implicit spark: SparkSession): org.apache.spark.sql.Dataset[ProjectKPIs] = {
    // Prepare data: enrich with zone information and filter to recent weeks
    val enrichedData = enrichWithZoneData(inputData, taxiZones)
    val filteredData = filterToRecentWeeks(enrichedData, weeks)

    // Calculate individual KPIs
    val (peakHour, peakHourPercentage) = calculatePeakHourPercentage(filteredData)
    val avgRevenuePerMile = calculateAverageRevenuePerMile(filteredData)
    val nightTripPercentage = calculateNightTripPercentage(filteredData)
    val avgMinutesPerMile = calculateAverageMinutesPerMile(filteredData)

    // Generate weekly metrics by borough
    val weeklyMetrics = generateWeeklyMetrics(
      filteredData, 
      peakHour,
      peakHourPercentage, 
      avgMinutesPerMile,
      avgRevenuePerMile, 
      nightTripPercentage
    )

    // Save weekly metrics to output path
    saveWeeklyMetrics(weeklyMetrics, outputPath)

    weeklyMetrics
  }

  /**
   * Enriches taxi trip data with borough, zone, and service_zone information from lookup table.
   * Uses inner joins to ensure only trips with valid location IDs are included.
   */
  private def enrichWithZoneData(trips: org.apache.spark.sql.Dataset[TaxiTrip],
                                  zones: org.apache.spark.sql.Dataset[TaxiZone]): org.apache.spark.sql.DataFrame = {
    val tripsDF = trips.toDF()
    val zonesDF = zones.toDF()
    
    // Join with pickup location zones (inner join)
    val withPickupZone = tripsDF.join(
      zonesDF.select(
        F.col("LocationID").as("PU_LocationID"),
        F.col("Borough").as("PU_Borough"),
        F.col("Zone").as("PU_Zone"),
        F.col("service_zone").as("PU_service_zone")
      ),
      tripsDF("PULocationID") === F.col("PU_LocationID"),
      "inner"
    ).drop("PU_LocationID")
    
    // Join with dropoff location zones (inner join)
    val withBothZones = withPickupZone.join(
      zonesDF.select(
        F.col("LocationID").as("DO_LocationID"),
        F.col("Borough").as("DO_Borough"),
        F.col("Zone").as("DO_Zone"),
        F.col("service_zone").as("DO_service_zone")
      ),
      withPickupZone("DOLocationID") === F.col("DO_LocationID"),
      "inner"
    ).drop("DO_LocationID")
    
    withBothZones
  }

  /**
   * Filters data to include only the most recent N weeks based on max pickup timestamp.
   */
  private def filterToRecentWeeks(data: org.apache.spark.sql.DataFrame, 
                                   weeks: Int): org.apache.spark.sql.DataFrame = {
    import data.sparkSession.implicits._
    
    val withTimestamp = data
      .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("hour", F.hour(F.col("pickup_ts")))

    val maxPickupOpt = withTimestamp
      .agg(F.max("pickup_ts"))
      .as[java.sql.Timestamp]
      .collect()
      .headOption

    maxPickupOpt match {
      case Some(maxTs) =>
        val cutoffMillis = maxTs.getTime - (weeks.toLong * 7L * 24L * 60L * 60L * 1000L)
        val cutoffTs = new java.sql.Timestamp(Math.max(0L, cutoffMillis))
        withTimestamp.filter(F.col("pickup_ts") >= F.lit(cutoffTs))
      case None => withTimestamp
    }
  }

  /**
   * Calculates the percentage of trips that occurred during the peak hour.
   * Returns tuple of (peak_hour, percentage)
   * Formula: (trips in busiest hour / total trips) * 100
   */
  private def calculatePeakHourPercentage(data: org.apache.spark.sql.DataFrame)
                                          (implicit spark: SparkSession): (Int, Double) = {
    import spark.implicits._
    
    val totalTrips = data.count()
    if (totalTrips == 0) return (0, 0.0)

    val tripsByHour = data.groupBy("hour").count()
      .withColumn("percentage", F.col("count") / F.lit(totalTrips) * 100)
      .orderBy(F.desc("percentage"))
      .limit(1)

    val result = tripsByHour
      .select(F.col("hour").cast("int"), F.col("percentage"))
      .as[(Int, Double)]
      .collect()
      .headOption
      .getOrElse((0, 0.0))
    
    result
  }

  /**
   * Calculates average revenue earned per mile traveled.
   * Formula: avg(total_amount / trip_distance)
   */
  private def calculateAverageRevenuePerMile(data: org.apache.spark.sql.DataFrame)
                                             (implicit spark: SparkSession): Double = {
    import spark.implicits._
    
    data.filter(F.col("trip_distance") > 0)
      .agg(F.avg(F.col("total_amount") / F.col("trip_distance")))
      .as[Double]
      .collect()
      .headOption
      .getOrElse(0.0)
  }

  /**
   * Calculates percentage of trips that occurred during night hours (midnight to 6am).
   * Night hours defined as: 0 <= hour < 6
   * Formula: (night trips / total trips) * 100
   */
  private def calculateNightTripPercentage(data: org.apache.spark.sql.DataFrame): Double = {
    val totalTrips = data.count()
    if (totalTrips == 0) return 0.0
    
    val nightTrips = data.filter(F.col("hour") >= 0 && F.col("hour") < 6).count()
    (nightTrips.toDouble / totalTrips) * 100.0
  }

  /**
   * Calculates average trip duration in minutes per mile traveled.
   * Formula: avg((dropoff_time - pickup_time) / 60 / trip_distance)
   */
  private def calculateAverageMinutesPerMile(data: org.apache.spark.sql.DataFrame)
                                             (implicit spark: SparkSession): Long = {
    import spark.implicits._
    
    val withDuration = data
      .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
      .withColumn("trip_minutes", 
        (F.unix_timestamp(F.col("dropoff_ts")) - F.unix_timestamp(F.col("pickup_ts"))) / 60.0)
      .filter(F.col("trip_distance") > 0)
      .withColumn("minutes_per_mile", F.col("trip_minutes") / F.col("trip_distance"))

    withDuration
      .agg(F.avg("minutes_per_mile").cast("long"))
      .as[Long]
      .collect()
      .headOption
      .getOrElse(0L)
  }

  /**
   * Generates weekly metrics aggregated by borough.
   * Combines borough-level trip volumes with overall weekly totals and computed KPIs.
   */
  private def generateWeeklyMetrics(data: org.apache.spark.sql.DataFrame,
                                     peakHour: Int,
                                     peakHourPct: Double,
                                     avgMinPerMile: Long,
                                     avgRevPerMile: Double,
                                     nightPct: Double)
                                    (implicit spark: SparkSession): org.apache.spark.sql.Dataset[ProjectKPIs] = {
    import spark.implicits._

    // Add borough and week identifiers using enriched zone data
    val withBoroughAndWeek = data
      .withColumn("borough", F.col("PU_Borough"))
      .withColumn("week_start", 
        F.date_format(F.date_trunc("week", F.col("pickup_ts")), "yyyy-MM-dd"))

    // Aggregate by borough and week
    val weeklyByBorough = withBoroughAndWeek
      .groupBy("week_start", "borough")
      .agg(F.count("*").as("trip_volume"))

    // Aggregate totals by week (across all boroughs)
    val weeklyTotals = withBoroughAndWeek
      .groupBy("week_start")
      .agg(
        F.count("*").as("total_trips"),
        F.sum("total_amount").as("total_revenue")
      )

    // Join and add computed KPIs
    weeklyByBorough
      .join(weeklyTotals, Seq("week_start"))
      .select(
        F.col("week_start"),
        F.col("borough"),
        F.col("trip_volume"),
        F.col("total_trips"),
        F.col("total_revenue"),
        F.lit(peakHour).as("peak_hour"),
        F.lit(peakHourPct).as("peak_hour_trip_percentage"),
        F.lit(avgMinPerMile).as("avg_minutes_per_mile"),
        F.lit(avgRevPerMile).as("avg_revenue_per_mile"),
        F.lit(nightPct).as("night_trip_percentage")
      )
      .as[ProjectKPIs]
  }

  /**
   * Saves weekly metrics to parquet format at the specified path.
   */
  private def saveWeeklyMetrics(metrics: org.apache.spark.sql.Dataset[ProjectKPIs], 
                                 outputPath: String): Unit = {
    val metricsPath = if (outputPath.endsWith("/")) 
      outputPath + "weekly_metrics" 
    else 
      outputPath + "/weekly_metrics"
    
    metrics.write.mode("overwrite").parquet(metricsPath)
  }

  def saveOutput(kpis: org.apache.spark.sql.Dataset[ProjectKPIs], outputPath: String): Unit = {
    kpis.write
      .mode("overwrite")
      .parquet(outputPath)
  }
}
