package org.cscie88c.spark

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SparkJobSpec extends AnyFunSuite with BeforeAndAfterAll {
	implicit var spark: SparkSession = _

	override def beforeAll(): Unit = {
		spark = SparkSession.builder()
			.appName("SparkJobSpec")
			.master("local[2]")
			.getOrCreate()
		spark.sparkContext.setLogLevel("WARN")
	}

	override def afterAll(): Unit = {
		if (spark != null) spark.stop()
	}

	private def ts(year: Int, month: Int, day: Int, hour: Int, minute: Int): Timestamp = {
		val ldt = LocalDateTime.of(year, month, day, hour, minute)
		Timestamp.from(ldt.atZone(ZoneId.systemDefault()).toInstant)
	}

	test("cleanData filters invalid rows and keeps only valid rows") {
		val s = spark
		import s.implicits._

		// Create TaxiTrip instances directly
		val trips = Seq(
			// valid row
			TaxiTrip(Some(1L), Some(ts(2025,10,10,10,0)), Some(ts(2025,10,10,10,20)), Some(2L), Some(2.0), Some(1L), Some("N"), Some(100L), Some(200L), Some(1L), Some(10.0), Some(0.5), Some(0.5), Some(1.0), Some(0.0), Some(0.0), Some(12.0), Some(0.0), Some(0.0)),
			// negative fare
			TaxiTrip(Some(1L), Some(ts(2025,10,10,11,0)), Some(ts(2025,10,10,11,10)), Some(1L), Some(1.0), Some(1L), Some("N"), Some(101L), Some(201L), Some(1L), Some(-5.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(-5.0), Some(0.0), Some(0.0)),
			// zero distance
			TaxiTrip(Some(1L), Some(ts(2025,10,10,12,0)), Some(ts(2025,10,10,12,5)), Some(1L), Some(0.0), Some(1L), Some("N"), Some(102L), Some(202L), Some(1L), Some(5.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(5.0), Some(0.0), Some(0.0)),
			// invalid passenger_count
			TaxiTrip(Some(1L), Some(ts(2025,10,10,13,0)), Some(ts(2025,10,10,13,20)), Some(9L), Some(3.0), Some(1L), Some("N"), Some(103L), Some(203L), Some(1L), Some(15.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(15.0), Some(0.0), Some(0.0)),
			// dropoff before pickup
			TaxiTrip(Some(1L), Some(ts(2025,10,10,14,30)), Some(ts(2025,10,10,14,0)), Some(1L), Some(2.0), Some(1L), Some("N"), Some(104L), Some(204L), Some(1L), Some(8.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(8.0), Some(0.0), Some(0.0)),
			// negative tip
			TaxiTrip(Some(1L), Some(ts(2025,10,10,15,0)), Some(ts(2025,10,10,15,20)), Some(1L), Some(2.5), Some(1L), Some("N"), Some(105L), Some(205L), Some(1L), Some(12.0), Some(0.0), Some(0.0), Some(-1.0), Some(0.0), Some(0.0), Some(12.0), Some(0.0), Some(0.0)),
			// invalid payment type
			TaxiTrip(Some(1L), Some(ts(2025,10,10,16,0)), Some(ts(2025,10,10,16,10)), Some(1L), Some(1.5), Some(1L), Some("N"), Some(106L), Some(206L), Some(10L), Some(6.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(6.0), Some(0.0), Some(0.0))
		)

		val ds = s.createDataset(trips)
		val df = ds.toDF()
		val cleaned = SparkJob.cleanData(df)
		val collected = cleaned.collect()

		// only the first row should be valid
		assert(collected.length == 1)
		val kept = collected.head
		assert(kept.fare_amount.contains(10.0))
		assert(kept.trip_distance.contains(2.0))
	}

	test("cleanData throws on high null rate for critical columns") {
		val s = spark
		import s.implicits._

		// Create 5 rows where 3 have null fare_amount (60% nulls) -> exceed 20% threshold
		val base = Seq(
			TaxiTrip(Some(1L), Some(ts(2025,10,10,10,0)), Some(ts(2025,10,10,10,10)), Some(1L), Some(1.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), Some(10.0), Some(0.0), Some(0.0), Some(1.0), Some(0.0), Some(0.0), Some(11.0), Some(0.0), Some(0.0)),
			TaxiTrip(Some(1L), Some(ts(2025,10,10,11,0)), Some(ts(2025,10,10,11,10)), Some(1L), Some(1.5), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), None, Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0)),
			TaxiTrip(Some(1L), Some(ts(2025,10,10,12,0)), Some(ts(2025,10,10,12,10)), Some(1L), Some(2.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), None, Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0)),
			TaxiTrip(Some(1L), Some(ts(2025,10,10,13,0)), Some(ts(2025,10,10,13,10)), Some(1L), Some(2.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), None, Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0)),
			TaxiTrip(Some(1L), Some(ts(2025,10,10,14,0)), Some(ts(2025,10,10,14,10)), Some(1L), Some(1.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), Some(8.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(8.0), Some(0.0), Some(0.0))
		)

			val ds = s.createDataset(base)
			val thrown = intercept[RuntimeException] {
				SparkJob.cleanData(ds.toDF())
			}

		assert(thrown.getMessage.contains("High null rates"))
	}

	test("cleanData throws when required column missing") {
		val s = spark

		// Build rows that deliberately omit required columns from schema
		val rows = Seq(
			Row(1L, "N")
		)

		val schema = StructType(Seq(
			StructField("VendorID", LongType, true),
			StructField("store_and_fwd_flag", StringType, true)
		))

		val dfRaw = s.createDataFrame(s.sparkContext.parallelize(rows), schema)
		val thrown = intercept[IllegalArgumentException] {
			SparkJob.cleanData(dfRaw)
		}
		assert(thrown.getMessage.contains("Missing required columns"))
	}

	test("calculateKPIs computes expected metrics and writes weekly metrics") {
	val s = spark
	import s.implicits._

		// Create mock taxi zones
		val zones = s.createDataset(Seq(
			TaxiZone(1L, "Manhattan", "Test Zone 1", "Yellow Zone"),
			TaxiZone(2L, "Queens", "Test Zone 2", "Boro Zone"),
			TaxiZone(3L, "Brooklyn", "Test Zone 3", "Boro Zone")
		))

		// Construct a small deterministic dataset across a single week
	val ds = s.createDataset(Seq(
			// hour 10
			TaxiTrip(Some(1L), Some(ts(2025,10,6,10,0)), Some(ts(2025,10,6,10,20)), Some(1L), Some(2.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), Some(10.0), Some(0.0), Some(0.0), Some(1.0), Some(0.0), Some(0.0), Some(11.0), Some(0.0), Some(0.0)),
			// hour 10
			TaxiTrip(Some(1L), Some(ts(2025,10,7,10,0)), Some(ts(2025,10,7,10,10)), Some(1L), Some(1.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), Some(5.5), Some(0.0), Some(0.0), Some(0.5), Some(0.0), Some(0.0), Some(5.5), Some(0.0), Some(0.0)),
			// hour 2 (night)
			TaxiTrip(Some(1L), Some(ts(2025,10,8,2,0)), Some(ts(2025,10,8,2,30)), Some(1L), Some(3.0), Some(1L), Some("N"), Some(2L), Some(3L), Some(1L), Some(20.0), Some(0.0), Some(0.0), Some(2.0), Some(0.0), Some(0.0), Some(22.0), Some(0.0), Some(0.0))
	))

		val outDir = java.nio.file.Files.createTempDirectory("kpi-test").toAbsolutePath.toString

		val kpis = SparkJob.calculateKPIs(ds.toDF().as[TaxiTrip], zones, weeks = 4, outputPath = outDir)(spark)

		val collected = kpis.collect()
		// Should contain one row per borough-week; here we have two boroughs (Manhattan and Queens)
		assert(collected.nonEmpty)
		assert(collected.exists(_.borough == "Manhattan"))
		assert(collected.exists(_.borough == "Queens"))

		// compute expected values manually
		val totalRevenue = 11.0 + 5.5 + 22.0
		val totalTrips = 3L
		// peak hour is 10 with 2 trips; percentage = 2/3*100 = 66.666...
		val expectedPeakHour = 10
		val expectedPeak = 2.0 / 3.0 * 100.0

		// avg revenue per mile = avg(total_amount / trip_distance)
		val revPerMiles = Seq(11.0/2.0, 5.5/1.0, 22.0/3.0)
		val expectedAvgRevPerMile = revPerMiles.sum / revPerMiles.size

		// avg minutes per mile = compute each: (minutes) / distance
		val minutesPerMile = Seq((20.0)/2.0, (10.0)/1.0, (30.0)/3.0) // minutes: 20,10,30
		val expectedAvgMinutesPerMile = (minutesPerMile.sum / minutesPerMile.size).toLong

		// night trips percentage: 1 out of 3 trips = 33.333%
		val expectedNightPercentage = (1.0 / 3.0) * 100.0

		// Validate that values are present in at least one KPI row (they are repeated for each borough)
		val sample = collected.head
		assert(sample.total_revenue === totalRevenue)
		assert(sample.total_trips === totalTrips)
		assert(sample.peak_hour === expectedPeakHour)
		assert(Math.abs(sample.peak_hour_trip_percentage - expectedPeak) < 0.001)
		assert(Math.abs(sample.avg_revenue_per_mile - expectedAvgRevPerMile) < 0.0001)
		assert(sample.avg_minutes_per_mile === expectedAvgMinutesPerMile)
		assert(Math.abs(sample.night_trip_percentage - expectedNightPercentage) < 0.001)

		// verify weekly metrics were written to output
		val written = java.nio.file.Paths.get(outDir, "weekly_metrics")
		assert(java.nio.file.Files.exists(written))
	}

}

