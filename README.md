## Programming in Scala for Big Data Systems, Fall 2025
Scala Project for Harvard Extension course CSCI E-88C, Fall, 2025. See course details at [Scala for Big Data](https://courses.dce.harvard.edu/?details&srcdb=202601&crn=16769).

This project is a multi-module setup for Scala applications that integrate with big data frameworks like Spark, Beam, and Kafka. It is designed to facilitate development in a structured manner, allowing for modular code organization and easy dependency management.

The project requires Java 17, Scala 2.13 and sbt 1.9.2+ environment to run.

## 🚀 Quick Start (Week 8 Project)

```bash
# 1. Build and run the Spark job
./runSparkJob.sh

# 2. Open dashboard in browser
open http://localhost:8501
```

## Project Structure
- **core**: Contains the core library code shared across other modules.
- **cli**: Implements the command-line interface for the application.
- **spark**: Contains Spark-specific code and configurations.
- **build.sbt**: The main build file for the project, defining the modules and their dependencies.
- **README.md**: This file, providing an overview of the project and its structure.

## Build Tool
This project uses [SBT](https://www.scala-sbt.org/) (Scala Build Tool) for building and managing dependencies. SBT allows for incremental compilation and easy management of multi-module projects.

## Running SparkJobSpec Tests Only
To run only the tests in the `SparkJobSpec` suite (located in the `spark` module), use the following sbt command:

```bash
sbt "testOnly org.cscie88c.spark.SparkJobSpec"
```

This will execute only the tests defined in the `SparkJobSpec` class, which are used to validate Spark job logic and data processing functions.

## Getting Started
1. **Clone the repository**:
   ```bash
   git clone https://github.com/esumitra/2025-fall-csci-e88c.git

   cd 2025-fall-csci-e88c
   ```
2. **Build the project**:
   ```bash
   sbt compile
   ```
3. **Run the CLI**:
   ```bash
   sbt "cli/run John"
   ```

   To packge an executable file use the native packager plugin:
   ```bash
   sbt "cli/stage"
    ```
    This will create a script in the `cli/target/universal/stage/bin` directory that you can run directly.

   e.g., ` ./cli/target/universal/stage/bin/cli John`
4. **Run the Spark application**:
   ```bash
   sbt spark/run
   ```

## Running in Codespaces
This project is configured to run in GitHub Codespaces, providing a ready-to-use development environment. Click the "Open in Github Codespaces" button below to start the developer IDE in the cloud.

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/esumitra/2025-fall-csci-e88c?quickstart=1)


## Running in DevContainer
This project can be run in a DevContainer for a consistent development environment. Ensure you have [Visual Studio Code](https://code.visualstudio.com/) and the [Remote - Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed. Follow these steps:
1. Open the project in Visual Studio Code.
2. Press `(Cmd+Shift+P)` and select `Dev Containers: Reopen in Container`.
3. Wait for the container to build and start.
4. Open a terminal in the DevContainer and run the project using SBT commands as described above.

## Running Spark

1. Start the docker container:
   ```bash
   docker compose -f docker-compose-spark.yml up
   ```
2. Connect to the spark-master:
   ```bash
   docker exec -it spark-master /bin/bash
   ```
3. Run the spark shell interactively:
   ```bash
   /opt/spark/bin/spark-shell
   ```

4. Run the Spark application:

   Submit the Spark job using the `/opt/spark/bin/spark-submit` command.

## Running the Spark Job in Docker
1. First, ensure that your Spark application uberjar file is built. You can build the Spark uberjar file by using the following sbt commands:
   ```bash
   sbt compile
   sbt spark/assembly
   ```

2. Then, copy the JAR file to the `docker/apps/spark` directory:
   ```bash
   cp spark/target/scala-2.13/SparkJob.jar docker/apps/spark
   ```

3. Copy any data files to the `data` directory.

   The `data` directory is mounted to the Docker container at `/opt/spark-data`. Ensure that any input data files required by your Spark job are placed in this directory.

4. Next, start the Docker container:
   ```bash
   docker compose -f docker-compose-spark.yml up -d
   ```
5. Finally, submit the Spark job:
   ```bash
   docker exec -it spark-master /bin/bash
   
   /opt/spark/bin/spark-submit --class org.cscie88c.spark.SparkJob --master spark://spark-master:7077 /opt/spark-apps/SparkJob.jar
   ```
6. To stop the Docker containers,

   Exit the container shell and run:
   ```bash
   docker compose -f docker-compose-spark.yml down
   ```

## Docker commands

1. Start the docker container:
   ```bash
   docker compose -f docker-compose-spark.yml up
   ```
2. Check status of running containers:
   ```bash
   docker ps
   ```
3. Review the logs:
   ```bash
   docker logs -f spark-master
   ```
4. Stop running containers:
   ```bash
   docker compose -f docker-compose-spark.yml down
   ```

## Running a Beam job locally

1. Compile and build the Beam files
   ```bash
   sbt compile
   sbt package
   ```
2. Built the native executable files using sbt-native-packager:
   ```bash
   sbt beam/stage
   ```
3. Copy any data files to the `data` directory.

4. Run the Beam job using the generated script in `beam/target/universal/stage/bin/`:
   ```bash
   ./beam/target/universal/stage/bin/beam --inputFile=<input-file-path> --outputPath=<output-path>
   ```

### Static Analysis Tools

#### Scalafmt
To ensure clean code, run scalafmt periodically. The scalafmt configuration is defined at https://scalameta.org/scalafmt/docs/configuration.html

For source files,

`sbt scalafmt`

For test files.

`sbt test:scalafmt`

#### Scalafix
To ensure clean code, run scalafix periodically. The scalafix rules are listed at https://scalacenter.github.io/scalafix/docs/rules/overview.html

For source files,

`sbt "scalafix RemoveUnused"`

For test files.

`sbt "test:scalafix RemoveUnused"`

# Project week8
This section documents the KPIs produced by the Spark job and how to run the job locally using the provided `runSparkJob.sh` script.

## Code Architecture

**Data Processing Pipeline:**
1. **Bronze Layer** (`loadInputFile`, `loadTaxiZoneLookup`): 
   - Loads raw parquet data into TaxiTrip dataset
   - Loads NYC Taxi Zone lookup data from CSV (LocationID, Borough, Zone, service_zone)
2. **Silver Layer** (`cleanData`, `enrichWithZoneData`): 
   - Validates and cleans data:
     - `validateDataQuality()`: Fail-fast validation (required columns, null rates, duplicates)
     - `validateTimeSeriesCompleteness()`: Ensures no missing weeks
     - `applyDataFilters()`: Row-level filtering (valid fares, distances, timestamps, etc.)
   - Enriches trips with zone data:
     - Inner joins with taxi zone lookup on PULocationID and DOLocationID
     - Adds borough, zone, and service_zone information for both pickup and dropoff locations
     - Only includes trips with valid location IDs
3. **Gold Layer** (`calculateKPIs`): Computes business metrics
   - Individual KPI calculation methods with documented formulas
   - Weekly aggregation by pickup borough
   - Output persistence

**Individual KPI Methods:**
   - `enrichWithZoneData()`: Enriches trips with pickup and dropoff borough/zone data via inner joins
   - `filterToRecentWeeks()`: Filters to N most recent weeks
   - `generateWeeklyMetrics()`: Aggregates metrics by week and pickup borough

### KPIs and exact formulas

The Spark job writes a dataset with the following KPI fields, grouped by **week** and **borough**. The formulas below match the code implementation in `spark/src/main/scala/org/cscie88c/spark/SparkJob.scala`.

**KPI Formulas:**

- **total_trips**: Number of trips in the borough for the week
   - `total_trips = count of rows`

- **total_revenue**: Sum of `total_amount` for all trips in the borough for the week
   - `total_revenue = sum of total_amount`

- **peak_hour**: The hour (0-23) with the highest trip volume
   - `peak_hour = argmax_h(count of trips where hour(pickup_ts) = h)`

- **peak_hour_trip_percentage**: Percentage of trips in the busiest hour
   - `peak_hour_trip_percentage = (trips in peak hour / total_trips) * 100`

- **avg_minutes_per_mile**: Average trip duration per mile traveled
   - `avg_minutes_per_mile = mean((dropoff_ts - pickup_ts) / (60 * trip_distance))`

- **avg_revenue_per_mile**: Average revenue earned per mile traveled
   - `avg_revenue_per_mile = mean(total_amount / trip_distance)`

- **night_trip_percentage**: Percentage of trips during night hours (midnight-6am)
      - `night_trip_percentage = (number of trips where hour(pickup_ts) in [0,5] / total_trips) * 100`

All metrics are aggregated by `week_start` and `borough` (pickup location). See the code for exact implementation details.

### Where outputs are written
The Spark job writes outputs under the provided output path argument (`outpath`):

- `outpath/kpis` — parquet files containing the final KPIs dataset.

When using the included `runSparkJob.sh`, the script passes `/opt/spark-data/output/` as the `outpath`. Because the host `data/` directory is mounted into the container at `/opt/spark-data`, the resulting files will appear on the host under `data/output/kpis`.

### How to run the Spark job (quick steps)
1. Ensure you have the required data files in the `data/` folder:
   - **Input Parquet**: Place your taxi trip data (example: `data/yellow_tripdata_2025-01.parquet`). The job expects a Parquet file with the taxi schema used in `TaxiTrip` (see `spark/src/main/scala/.../SparkJob.scala` for required columns).
   - **Taxi Zone Lookup CSV**: Place the NYC Taxi Zone lookup file (example: `data/taxi_zone_lookup.csv`) with columns: LocationID, Borough, Zone, service_zone.

2. From the repository root, make the run script executable (if needed) and run it:

```bash
chmod +x runSparkJob.sh
./runSparkJob.sh
```

The `runSparkJob.sh` script will:
- build the Spark uberjar (`sbt spark/assembly`),
- copy the jar to `docker/apps/spark`,
- start the Docker compose environment, and
- submit the Spark job inside the `spark-master` container (the script uses `/opt/spark-data/yellow_tripdata_2025-01.parquet` as the input, `/opt/spark-data/taxi_zone_lookup.csv` as the zone lookup file, and `/opt/spark-data/output/` as the output directory).

3. After the job completes, inspect the output directory on the host:

- `data/output/kpis` (Parquet files with the final KPIs)

You can inspect Parquet files with tools like `parquet-tools` or by reading them with Spark/Python. Optionally, you may `docker exec` into the `spark-master` container and inspect `/opt/spark-data/output/` directly.

### Command-line Arguments
The Spark job accepts the following arguments:
1. **input_file** (required): Path to the taxi trip parquet file
2. **taxi_zone_lookup_csv** (required): Path to the NYC Taxi Zone lookup CSV file
3. **output_path** (required): Path where KPIs and weekly metrics will be written
4. **weeks** (optional): Number of weeks to include in KPI calculations (default is 4)

Example with custom number of weeks (inside the container):

```bash
/opt/spark/bin/spark-submit --class org.cscie88c.spark.SparkJob --master local[*] \
  /opt/spark-apps/SparkJob.jar \
  /opt/spark-data/yellow_tripdata_2025-01.parquet \
  /opt/spark-data/taxi_zone_lookup.csv \
  /opt/spark-data/output/ \
  8
```

---

### NYC Taxi Analytics Dashboard (Streamlit + Plotly)

An interactive web dashboard that visualizes the Parquet outputs from the SparkJob, providing comprehensive analytics with beautiful charts and insights.

**Features:**
- 📈 **Real-time KPI Metrics**: Total trips, revenue, efficiency metrics
- �️ **Borough-Level Analytics**: All metrics aggregated by pickup borough using NYC Taxi Zone data
- �📊 **Interactive Visualizations**: 
  - Bar charts for trip volume by pickup borough
  - Pie charts for revenue distribution by pickup location
  - Line charts for trends over time
  - Performance comparisons across pickup boroughs
- 🎨 **Modern UI**: Clean design with Plotly charts and custom styling
- 📥 **Data Export**: Download processed data as CSV
- 🔄 **Auto-refresh**: Updates automatically when SparkJob runs

**Technical Details:**
- Location: `docker/apps/spark/dashboard`
- Service: `evidence-dashboard` in `docker-compose-spark.yml`
- Port: `8501` (http://localhost:8501)
- Stack: Streamlit + Plotly + Pandas + PyArrow
- Data Sources:
  - `./data/output/kpis` — final KPIs (parquet)

**Updated Field Names & Zone Enrichment (November 2025):**
The dashboard now uses the refactored field names and zone-enriched data:
- `borough` (pickup borough from NYC Taxi Zone lookup)
- `peak_hour` (the hour 0-23 with highest trip volume)
- `peak_hour_trip_percentage` (percentage of trips in busiest hour)
- `avg_minutes_per_mile` (average trip duration per mile)
- `night_trip_percentage` (percentage of trips from midnight-6am)
- `avg_revenue_per_mile` (average revenue per mile)
- All borough metrics are based on **pickup location** (PU_Borough)
- Dashboard displays peak hour as a **2-hour range** (e.g., "07-09", "17-19")

**Quick Start:**

1. **Run the Spark job** to generate data (if not already done):
   ```bash
   ./runSparkJob.sh
   ```

2. **Build and start the dashboard**:
   ```bash
   docker-compose -f docker-compose-spark.yml build evidence-dashboard
   docker-compose -f docker-compose-spark.yml up -d evidence-dashboard
   ```

3. **Open in browser**: http://localhost:8501

4. **Stop the dashboard**:
   ```bash
   docker-compose -f docker-compose-spark.yml down
   ```

**Dashboard Sections:**

1. **Key Performance Indicators**
   - 8 metric cards showing critical business metrics
   - Total trips, revenue, efficiency, and operational stats
   - Week and borough coverage indicators

2. **Visual Analytics**
   - Trip volume by pickup borough (top 10)
   - Revenue distribution by pickup location (pie chart)
   - Weekly trends for trips and revenue
   - Performance metrics by pickup borough (revenue/mile, minutes/mile)
   - Trip efficiency comparison across boroughs

3. **Weekly Activity Heatmap**
   - Shows trip patterns across weeks and pickup boroughs
   - Identifies high-activity periods and locations
   - Top 15 boroughs by volume

4. **Raw Data Tables**
   - Sortable, filterable data grid with borough enrichment
   - Summary statistics
   - CSV export functionality

**Troubleshooting:**
- **No data showing?** Verify `data/output/kpis` exists with parquet files
- **Port conflict?** Change port mapping in `docker-compose-spark.yml`
- **Dashboard not updating?** Restart the container: `docker-compose -f docker-compose-spark.yml restart evidence-dashboard`


## License
Copyright 2025, Edward Sumitra

Licensed under the MIT License.