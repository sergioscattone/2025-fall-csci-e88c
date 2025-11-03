# NYC Taxi Analytics Dashboard

Interactive web dashboard for visualizing NYC Taxi trip KPIs processed by Apache Spark.

## Features

**📊 Visualizations:**
- Bar charts: Trip volume and performance metrics by borough
- Pie charts: Revenue distribution across boroughs
- Line charts: Weekly trends for trips and revenue
- Heatmaps: Activity patterns across weeks and locations
- Performance comparisons: Efficiency and revenue metrics

**📈 KPIs Displayed:**
- Total trips and revenue
- Average revenue per mile
- Average trip duration per mile
- Peak hour percentage
- Night trip counts (midnight-6am)
- Weekly and borough-level breakdowns

**🎨 Features:**
- Modern, responsive UI with custom styling
- Interactive Plotly charts
- Data export to CSV
- Summary statistics
- Real-time updates when SparkJob runs

## Updated Field Names (November 2025)

The dashboard has been updated to use the refactored SparkJob field names:
- `peak_hour_trip_percentage` (formerly `per_hour_trip_percentage`)
- `avg_minutes_per_mile` (formerly `avg_trip_time_vs_distance`)
- `night_trip_percentage` (updated from count to percentage for better scheduling insights)
- `avg_revenue_per_mile` (unchanged)

## Quick Start

From repository root:

```bash
# 1. Run the Spark job to generate data
./runSparkJob.sh

# 2. Build the dashboard image
docker-compose -f docker-compose-spark.yml build evidence-dashboard

# 3. Start the dashboard
docker-compose -f docker-compose-spark.yml up -d evidence-dashboard

# 4. Open in browser
open http://localhost:8501
```

## Tech Stack

- **Streamlit**: Web framework
- **Plotly**: Interactive visualizations
- **Pandas**: Data manipulation
- **PyArrow**: Parquet file reading
- **Python 3.10**: Runtime

## Data Sources

The dashboard reads parquet files from:
- `./data/output/kpis` — Final aggregated KPIs
- `./data/output/weekly_metrics` — Weekly metrics by borough

These paths are mounted from the host `./data` directory to `/opt/spark-data` in the container.

## Development

To modify the dashboard:

1. Edit `app.py` with your changes
2. Rebuild the container: `docker-compose -f docker-compose-spark.yml build evidence-dashboard`
3. Restart: `docker-compose -f docker-compose-spark.yml restart evidence-dashboard`

## Troubleshooting

**Dashboard shows "No data found":**
- Verify SparkJob has completed successfully
- Check that `data/output/kpis/` and `data/output/weekly_metrics/` contain parquet files
- Review Spark job logs for errors

**Port 8501 already in use:**
- Change port mapping in `docker-compose-spark.yml`
- Or stop the conflicting service

**Container won't start:**
- Check logs: `docker logs evidence-dashboard`
- Verify all dependencies are in `requirements.txt`
- Rebuild: `docker-compose -f docker-compose-spark.yml build --no-cache evidence-dashboard`
