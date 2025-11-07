import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(page_title="NYC Taxi Analytics - Spark KPIs", layout="wide", page_icon="🚕")

# Custom CSS for better styling
st.markdown("""
    <style>
    .main > div {
        padding-top: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

st.title("🚕 NYC Taxi Analytics Dashboard")
st.markdown("Real-time insights from SparkJob KPI calculations with NYC Taxi Zone enrichment")

st.sidebar.header("📊 Environment")
env = os.environ.get("EVIDENCE_ENV", "(not set)")
st.sidebar.write(f"**EVIDENCE_ENV:** {env}")

DATA_ROOT = "/opt/spark-data/output"
KPIS_PATH = os.path.join(DATA_ROOT, "kpis")

st.sidebar.header("📁 Data Locations")
st.sidebar.code(KPIS_PATH)

@st.cache_data(ttl=60)
def load_parquet(path):
    if not os.path.exists(path):
        return None
    try:
        # pandas can read a directory of parquet files with pyarrow
        df = pd.read_parquet(path)
        return df
    except Exception as e:
        st.sidebar.error(f"Error reading {path}: {e}")
        return None

kpis_df = load_parquet(KPIS_PATH)

if kpis_df is None:
    st.warning("⚠️ No parquet outputs found. Run the Spark job to generate data at ./data/output/")
    st.info("Run: `./runSparkJob.sh` from the repository root")
else:
    df = kpis_df
    
    if df is not None and not df.empty:
        # Top-level metrics
        st.header("📈 Key Performance Indicators")
        
        try:
            # Get unique values for aggregation
            total_trips = int(df['total_trips'].sum()) if 'total_trips' in df.columns else 0
            total_revenue = float(df['total_revenue'].sum()) if 'total_revenue' in df.columns else 0.0
            avg_rev_per_mile = float(df['avg_revenue_per_mile'].mean()) if 'avg_revenue_per_mile' in df.columns else 0.0
            avg_min_per_mile = int(df['avg_minutes_per_mile'].mean()) if 'avg_minutes_per_mile' in df.columns else 0
            peak_hour = int(df['peak_hour'].mode()[0]) if 'peak_hour' in df.columns else 0
            peak_hour_pct = float(df['peak_hour_trip_percentage'].mean()) if 'peak_hour_trip_percentage' in df.columns else 0.0
            night_trip_pct = float(df['night_trip_percentage'].mean()) if 'night_trip_percentage' in df.columns else 0.0
            
            # Format peak hour as 2-hour range
            peak_hour_end = (peak_hour + 2) % 24
            peak_hour_range = f"{peak_hour:02d}-{peak_hour_end:02d}"
            
            # Display metrics in columns
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Trips", f"{total_trips:,}")
            with col2:
                st.metric("Total Revenue", f"${total_revenue:,.2f}")
            with col3:
                st.metric("Avg Revenue/Mile", f"${avg_rev_per_mile:.2f}")
            with col4:
                st.metric("Avg Min/Mile", f"{avg_min_per_mile} min")
            
            col5, col6, col7, col8 = st.columns(4)
            with col5:
                st.metric("Peak Hour Range", peak_hour_range)
            with col6:
                st.metric("Peak Hour %", f"{peak_hour_pct:.1f}%")
            with col7:
                st.metric("Night Trip %", f"{night_trip_pct:.1f}%")
            with col8:
                weeks_count = df['week_start'].nunique()
                st.metric("Weeks Analyzed", weeks_count)
                
        except Exception as e:
            st.error(f"Error displaying metrics: {e}")
        
        st.markdown("---")
        
        # Visualizations
        st.header("📊 Visual Analytics")
        
        # Get latest week data for current analysis
        try:
            latest_week = df["week_start"].max()
            latest_df = df[df["week_start"] == latest_week]
            
            # Row 1: Trip Volume and Revenue Distribution
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🚖 Trip Volume by Pickup Borough")
                vol_by_borough = latest_df.groupby("borough")["total_trips"].sum().sort_values(ascending=False)
                top_boroughs = vol_by_borough.head(10)
                
                fig = px.bar(
                    x=top_boroughs.index,
                    y=top_boroughs.values,
                    labels={'x': 'Pickup Borough', 'y': 'Number of Trips'},
                    title=f"Week: {latest_week} (by Pickup Location)",
                    color=top_boroughs.values,
                    color_continuous_scale='Viridis'
                )
                fig.update_layout(showlegend=False, xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("💰 Revenue Distribution by Pickup Borough")
                # Calculate revenue per borough
                revenue_by_borough = latest_df.groupby("borough").agg({
                    'total_trips': 'sum',
                    'total_revenue': 'first'
                }).reset_index()
                revenue_by_borough['borough_revenue'] = (
                    revenue_by_borough['total_trips'] / revenue_by_borough['total_trips'].sum() * 
                    revenue_by_borough['total_revenue']
                )
                top_revenue = revenue_by_borough.nlargest(10, 'borough_revenue')
                
                fig = px.pie(
                    top_revenue,
                    values='borough_revenue',
                    names='borough',
                    title=f"Top 10 Pickup Boroughs - Week: {latest_week}",
                    hole=0.4
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            
            # Row 3: Performance Metrics
            st.subheader("⚡ Performance Metrics")
            col5, col6 = st.columns(2)
            
            with col5:
                # Top performing boroughs by avg revenue per mile
                borough_metrics = latest_df.groupby('borough').agg({
                    'total_trips': 'sum',
                    'avg_revenue_per_mile': 'first'
                }).reset_index()
                borough_metrics = borough_metrics[borough_metrics['total_trips'] > 100]  # Filter low volume
                top_performers = borough_metrics.nlargest(10, 'avg_revenue_per_mile')
                
                fig = px.bar(
                    top_performers,
                    x='borough',
                    y='avg_revenue_per_mile',
                    title="Top 10 Pickup Boroughs by Revenue/Mile (>100 trips)",
                    labels={'borough': 'Pickup Borough', 'avg_revenue_per_mile': 'Revenue per Mile ($)'},
                    color='avg_revenue_per_mile',
                    color_continuous_scale='RdYlGn'
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col6:
                # Trip efficiency
                efficiency_metrics = latest_df.groupby('borough').agg({
                    'total_trips': 'sum',
                    'avg_minutes_per_mile': 'first'
                }).reset_index()
                efficiency_metrics = efficiency_metrics[efficiency_metrics['total_trips'] > 100]
                top_efficient = efficiency_metrics.nsmallest(10, 'avg_minutes_per_mile')
                
                fig = px.bar(
                    top_efficient,
                    x='borough',
                    y='avg_minutes_per_mile',
                    title="Most Efficient Pickup Boroughs by Travel Time (>100 trips)",
                    labels={'borough': 'Pickup Borough', 'avg_minutes_per_mile': 'Minutes per Mile'},
                    color='avg_minutes_per_mile',
                    color_continuous_scale='RdYlGn_r'
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            
            # Row 4: Trip Time vs Distance Analysis
            st.subheader("⏱️ Trip Time vs Distance Analysis")
            
            # Create three visualizations for time vs distance
            col7, col8 = st.columns(2)
            
            with col7:
                # Scatter plot: Minutes per Mile across boroughs
                time_efficiency = latest_df.groupby('borough').agg({
                    'total_trips': 'sum',
                    'avg_minutes_per_mile': 'first',
                    'avg_revenue_per_mile': 'first'
                }).reset_index()
                time_efficiency = time_efficiency[time_efficiency['total_trips'] > 50]  # Filter low volume
                
                fig = px.scatter(
                    time_efficiency,
                    x='borough',
                    y='avg_minutes_per_mile',
                    size='total_trips',
                    color='avg_revenue_per_mile',
                    title="Trip Efficiency by Borough (Minutes/Mile)",
                    labels={
                        'borough': 'Pickup Borough',
                        'avg_minutes_per_mile': 'Avg Minutes per Mile',
                        'total_trips': 'Trip Volume',
                        'avg_revenue_per_mile': 'Revenue/Mile'
                    },
                    color_continuous_scale='RdYlGn_r',
                    hover_data=['total_trips', 'avg_revenue_per_mile']
                )
                fig.update_layout(xaxis_tickangle=-45, height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col8:
                # Dual-axis comparison: Time vs Revenue efficiency
                top_boroughs_efficiency = time_efficiency.nlargest(10, 'total_trips')
                
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig.add_trace(
                    go.Bar(
                        x=top_boroughs_efficiency['borough'],
                        y=top_boroughs_efficiency['avg_minutes_per_mile'],
                        name="Minutes/Mile",
                        marker_color='lightblue'
                    ),
                    secondary_y=False
                )
                
                fig.add_trace(
                    go.Scatter(
                        x=top_boroughs_efficiency['borough'],
                        y=top_boroughs_efficiency['avg_revenue_per_mile'],
                        name="Revenue/Mile",
                        mode='lines+markers',
                        marker=dict(size=10, color='orange'),
                        line=dict(width=3, color='orange')
                    ),
                    secondary_y=True
                )
                
                fig.update_xaxes(title_text="Pickup Borough", tickangle=-45)
                fig.update_yaxes(title_text="Minutes per Mile", secondary_y=False)
                fig.update_yaxes(title_text="Revenue per Mile ($)", secondary_y=True)
                fig.update_layout(
                    title="Time Efficiency vs Revenue (Top 10 Boroughs)",
                    height=400,
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)

            # Row 4: Weekly Trip Volume Table/Chart
            st.subheader("📅 Weekly Trip Volume by Pickup Borough")
            try:
                # Prepare data aggregated by week and borough
                weekly_borough_data = df.groupby(['week_start', 'borough']).agg({
                    'total_trips': 'sum'
                }).reset_index().sort_values(['week_start', 'total_trips'], ascending=[True, False])
                
                # Create tabs for table and chart views
                table_tab, chart_tab = st.tabs(["📊 Chart View", "📋 Table View"])
                
                with chart_tab:
                    # Get top 10 boroughs by total volume for cleaner visualization
                    top_10_boroughs = df.groupby('borough')['total_trips'].sum().nlargest(10).index.tolist()
                    chart_data = weekly_borough_data[weekly_borough_data['borough'].isin(top_10_boroughs)]
                    
                    fig = px.bar(
                        chart_data,
                        x='week_start',
                        y='total_trips',
                        color='borough',
                        title="Weekly Trip Volume by Pickup Borough (Top 10)",
                        labels={'week_start': 'Week Start', 'total_trips': 'Trip Volume', 'borough': 'Pickup Borough'},
                        barmode='group',
                        height=500
                    )
                    fig.update_layout(xaxis_tickangle=-45, legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02))
                    st.plotly_chart(fig, use_container_width=True)
                
                with table_tab:
                    # Show full table with all boroughs
                    st.dataframe(
                        weekly_borough_data,
                        use_container_width=True,
                        height=500,
                        column_config={
                            "week_start": st.column_config.DateColumn("Week Start", format="YYYY-MM-DD"),
                            "borough": st.column_config.TextColumn("Pickup Borough"),
                            "total_trips": st.column_config.NumberColumn("Trip Volume", format="%d")
                        }
                    )
                    
                    # Download button for this specific view
                    csv_weekly = weekly_borough_data.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Weekly Borough Data as CSV",
                        data=csv_weekly,
                        file_name="weekly_borough_trip_volume.csv",
                        mime="text/csv"
                    )
            except Exception as e:
                st.warning(f"Could not generate weekly borough breakdown: {e}")
                
        except Exception as e:
            st.error(f"Error building visualizations: {e}")
            import traceback
            st.code(traceback.format_exc())
        
        # Data tables
        st.markdown("---")
        st.header("📋 Raw Data")
        
        tab1, tab2 = st.tabs(["KPI Data", "Summary Statistics"])
        
        with tab1:
            if df is not None:
                st.dataframe(
                    df.sort_values(['week_start', 'total_trips'], ascending=[False, False]),
                    use_container_width=True,
                    height=400
                )
                
                # Download button
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"taxi_kpis_{latest_week}.csv",
                    mime="text/csv"
                )
        
        with tab2:
            st.subheader("Statistical Summary")
            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
            st.dataframe(df[numeric_cols].describe(), use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.markdown("""
### 📖 About
This dashboard visualizes NYC Taxi trip data processed by Apache Spark with enriched location data.

**Data Pipeline:**
- Bronze: Raw parquet data
- Silver: Cleaned & validated with zone lookup
- Gold: Aggregated KPIs by pickup borough

**Zone Enrichment:**
- Trips enriched with NYC Taxi Zone data
- Borough metrics based on pickup location
- Inner joins ensure valid location IDs only

**Refresh:** Data updates when SparkJob runs

**Tech Stack:**
- Apache Spark 3.5.1
- Streamlit + Plotly
- Python 3.10
""")
