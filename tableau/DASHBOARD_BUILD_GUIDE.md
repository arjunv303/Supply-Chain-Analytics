# Tableau Public Dashboard Build Guide
## Retail Supply Chain & Demand Intelligence

Step-by-step instructions for building all 4 dashboards in Tableau Public. Each dashboard pulls from a CSV exported by the Python pipeline.

---

## Prerequisites

- Tableau Public Desktop installed (free at public.tableau.com)
- Python pipeline completed (`python 04_export_for_tableau.py`)
- All CSVs present in `data/tableau_exports/`

---

## Connecting Data

1. Open Tableau Public Desktop
2. On the start screen, click **Text File** under Connect
3. Navigate to `data/tableau_exports/` and select the CSV
4. Verify dates are recognized as Date, not String
5. Click **Sheet 1** at the bottom to start building

---

## Dashboard 1: Executive KPI Overview

**Data source:** `exec_kpis.csv`

#### Sheet 1.1: Monthly Order Volume
- Columns: `Order Month` (continuous, MMM YYYY format)
- Rows: `Total Orders`
- Mark type: Line
- Add a reference line for the average
- Title: "Monthly Order Volume"

#### Sheet 1.2: On-Time Delivery Rate Trend
- Columns: `Order Month`
- Rows: `On Time Delivery Rate Pct` (bar), `Avg Lead Time Days` (line)
- Mark types: Bar + Line, dual axis
- Color bars using a calculated field: `IF [On Time Delivery Rate Pct] >= 90 THEN "On Track" ELSEIF [On Time Delivery Rate Pct] >= 75 THEN "At Risk" ELSE "Critical" END`
- Title: "On-Time Delivery Rate vs. Avg Lead Time"

#### Sheet 1.3: Fulfillment Rate KPI Tile
- Drag `Fulfillment Rate Pct` to Text
- Mark type: Text
- Format as `##.#"%"`
- Title: "Fulfillment Rate"

#### Sheet 1.4: Revenue Trend
- Columns: `Order Month`
- Rows: `Total Revenue`
- Mark type: Area
- Format numbers as currency (R$)
- Title: "Monthly Revenue (BRL)"

### Dashboard 1 Layout
```
[Total Orders] [Revenue] [On-Time %] [Avg Lead Time]  <- KPI tiles
Monthly Order Volume (full width)
On-Time Rate Trend  |  Revenue Trend
```

---

## Dashboard 2: Seller Performance Scorecard

**Data source:** `seller_scorecard.csv`

#### Sheet 2.1: Composite Score Bar Chart
- Rows: `Seller Id` sorted descending by `Composite Score`
- Columns: `Composite Score`
- Mark type: Bar
- Color by `Volume Tier`
- Filter to top 30 sellers
- Add a reference line for average composite score
- Title: "Seller Performance Score (Top 30)"

#### Sheet 2.2: Review Score vs. On-Time Rate Scatter
- Rows: `Avg Review Score`
- Columns: `On Time Rate Pct`
- Mark type: Circle
- Size: `Total Orders`
- Color: `Composite Score` (continuous green to red)
- Add reference lines at median on both axes for quadrant view
- Title: "Review Score vs. On-Time Rate"

#### Sheet 2.3: Seller Count by State (Map)
- Mark type: Map
- Detail: `Seller State` set as State/Province for Brazil
- Color: count of sellers
- Title: "Seller Geographic Distribution"

#### Sheet 2.4: Volume Tier Breakdown
- Pie chart of `Volume Tier` with count of sellers
- Title: "Sellers by Volume Tier"

### Dashboard 2 Layout
```
Composite Score Bar Chart  |  Volume Tier Pie
Review vs On-Time Scatter  |  Seller Map
```

Quadrant labels (add as floating text boxes):
- Top-Right: "Stars"
- Top-Left: "Review Leaders"
- Bottom-Right: "Delivery Champions"
- Bottom-Left: "Needs Improvement"

---

## Dashboard 3: Regional Demand Map

**Data source:** `regional_demand.csv`

#### Sheet 3.1: Brazil Choropleth Map
- Mark type: Map
- Set `State Name` to State/Province geographic role
- Color: `Total Revenue` (light to dark sequential)
- Tooltip: State, Region, Orders, Revenue, Avg Order Value, Late Delivery Rate
- Title: "Total Revenue by State"

Note: Go to Map > Edit Locations, set Country = Brazil so it renders correctly.

#### Sheet 3.2: State Revenue Ranking
- Rows: `State` sorted by `Total Revenue` descending
- Columns: `Total Revenue`
- Color by `Region`
- Title: "Revenue by State"

#### Sheet 3.3: Late Delivery Rate by State
- Rows: `State`
- Columns: `Late Delivery Rate Pct`
- Color: red (high) to green (low)
- Add reference line for national average
- Title: "Late Delivery Rate by State (%)"

#### Sheet 3.4: Avg Order Value by Region
- Rows: `Avg Order Value`
- Columns: `Region`
- Mark type: Circle
- Color by `Region`
- Title: "Average Order Value by Region (BRL)"

### Dashboard 3 Layout
```
Brazil Choropleth Map   |  State Revenue Ranking
Late Delivery by State  |  Avg Order Value by Region
```

---

## Dashboard 4: AI-Powered Demand Forecast

**Data sources:** `forecast_output.csv` and `anomaly_flags.csv`

Connect `forecast_output.csv` as primary, add `anomaly_flags.csv` as secondary, blend on `Order Date`.

#### Sheet 4.1: Demand Forecast Line Chart
- Columns: `Order Date` (continuous)
- Rows: `Actual Orders` and `Forecast`
- Mark type: Line
- Actual orders: solid blue line, Forecast: orange line
- Add a vertical reference line at the last actual date
- Title: "90-Day Demand Forecast"

#### Sheet 4.2: Anomaly Detection
- Columns: `Order Date`
- Rows: `Daily Orders`
- Mark type: Circle
- Color: Normal = gray, Anomaly = red
- Size: `Anomaly Score`
- Title: "Demand Anomaly Detection"

#### Sheet 4.3: Anomaly Type Summary
- Filter to anomalies only
- Rows: `Anomaly Type`
- Columns: count of dates
- Color by `Anomaly Type`
- Title: "Anomaly Breakdown by Type"

#### Sheet 4.4: Rolling Average vs. Actual
- Columns: `Order Date`
- Rows: `Daily Orders` (bar), `Rolling 7d Avg` (line), dual axis
- Color bars red if anomaly, gray otherwise
- Title: "Daily Orders vs. 7-Day Rolling Average"

### Dashboard 4 Layout
```
[Category Filter]
90-Day Demand Forecast (full width, top half)
Anomaly Detection  |  Rolling Avg vs. Actual
Anomaly Type Bar (bottom strip)
```

---

## Color Reference

| Element | Color | Hex |
|---|---|---|
| Primary | Steel Blue | `#1F77B4` |
| Good / On-time | Forest Green | `#2CA02C` |
| Bad / Late | Crimson | `#D62728` |
| Warning | Amber | `#FF7F0E` |
| Forecast | Orange | `#FF7F0E` |
| Anomaly | Red | `#D62728` |

---

## Publishing to Tableau Public

1. File > Save to Tableau Public
2. Sign in with your Tableau Public account
3. Copy the URL for your resume or GitHub README
