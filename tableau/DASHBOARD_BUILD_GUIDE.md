# Tableau Public Dashboard Build Guide
## Retail Supply Chain & Demand Intelligence

This guide walks you through building all 4 dashboards in Tableau Public step by step.
Each dashboard connects to a CSV exported by the Python pipeline.

---

## Prerequisites

- Tableau Public Desktop installed (free at public.tableau.com)
- Python pipeline completed (`python 04_export_for_tableau.py`)
- All CSVs present in `data/tableau_exports/`

---

## How to Connect Data in Tableau Public

1. Open Tableau Public Desktop
2. On the start screen, click **"Text File"** under Connect
3. Navigate to `data/tableau_exports/` and select the CSV
4. Tableau will auto-detect column types — verify dates are recognized as **Date** not **String**
5. Click **"Sheet 1"** at the bottom to start building

---

## Dashboard 1: Executive KPI Overview

**Data source:** `exec_kpis.csv`

### Sheets to build

#### Sheet 1.1 — Monthly Order Volume (Line Chart)
- **Rows:** `Total Orders`
- **Columns:** `Order Month` (continuous, formatted as MMM YYYY)
- **Mark type:** Line
- **Add reference line:** Average line across entire period
- **Color:** Blue (#1F77B4)
- **Title:** "Monthly Order Volume"

#### Sheet 1.2 — On-Time Delivery Rate Trend (Dual Axis)
- **Rows:** `On Time Delivery Rate Pct` (bar), `Avg Lead Time Days` (line)
- **Columns:** `Order Month`
- **Mark types:** Bar + Line (dual axis)
- **Bar color:** Green if > 90%, Yellow if 75-90%, Red if < 75%
  - Create calculated field: `IF [On Time Delivery Rate Pct] >= 90 THEN "On Track" ELSEIF [On Time Delivery Rate Pct] >= 75 THEN "At Risk" ELSE "Critical" END`
- **Title:** "On-Time Delivery Rate vs. Avg Lead Time"

#### Sheet 1.3 — Fulfillment Rate KPI Tile (Single Number)
- Drag `Fulfillment Rate Pct` to Text
- Mark type: Text
- Format as: `##.#"%"`
- Add color: Green > 95%, Yellow 90-95%, Red < 90%
- Title: "Fulfillment Rate"

#### Sheet 1.4 — Revenue Trend (Area Chart)
- **Rows:** `Total Revenue`
- **Columns:** `Order Month`
- **Mark type:** Area
- **Color:** Light blue with dark border
- Format numbers as currency (R$)
- **Title:** "Monthly Revenue (BRL)"

#### Sheet 1.5 — Order Status Distribution (Stacked Bar)
- **Note:** This uses the overall counts — create a summary table manually from the SQL output
- Or: Filter the orders data to show status breakdown per month
- **Rows:** `Order Count`
- **Columns:** `Order Month`
- **Color:** `Fulfillment Rate Pct` → discrete status color

### Dashboard 1 Layout
```
┌──────────────────────────────────────────────────────┐
│  KPI TILES: [Total Orders] [Revenue] [On-Time %] [Avg Lead Time]  │
├──────────────────────────────────────────────────────┤
│         Monthly Order Volume (full width line chart)  │
├─────────────────────────┬────────────────────────────┤
│  On-Time Rate Trend     │  Revenue Trend             │
│  (dual axis bar+line)   │  (area chart)              │
└─────────────────────────┴────────────────────────────┘
```

**Dashboard title:** "Supply Chain Executive KPI Dashboard"
**Dashboard size:** 1400 × 900 px

---

## Dashboard 2: Seller Performance Scorecard

**Data source:** `seller_scorecard.csv`

### Sheets to build

#### Sheet 2.1 — Composite Score Bar Chart (Ranked)
- **Rows:** `Seller Id` (sorted descending by `Composite Score`)
- **Columns:** `Composite Score`
- **Mark type:** Bar
- **Color:** `Volume Tier` (use color palette: High=Dark Blue, Mid=Medium Blue, Low=Light Blue)
- **Filter:** Show top 30 sellers
- **Add reference line:** Average composite score
- **Title:** "Seller Performance Score (Top 30)"

#### Sheet 2.2 — Review Score vs. On-Time Rate Scatter
- **Rows:** `Avg Review Score`
- **Columns:** `On Time Rate Pct`
- **Mark type:** Circle
- **Size:** `Total Orders` (bigger circle = more orders)
- **Color:** `Composite Score` (continuous color scale, green to red)
- **Add reference lines:** Both axes at median values → creates 4 quadrants
- **Label:** Top 5 sellers by composite score
- **Tooltip:** Seller ID, City, State, Orders, Score
- **Title:** "Review Score vs. On-Time Rate (Size = Order Volume)"

#### Sheet 2.3 — Seller Count by State (Map)
- **Mark type:** Map (Geographic)
- **Detail:** `Seller State` → assign as State/Province for Brazil
- **Color:** `COUNT(Seller Id)` — dark = more sellers
- **Title:** "Seller Geographic Distribution"

#### Sheet 2.4 — Volume Tier Breakdown (Donut Chart)
- Create donut using dual axis trick:
  - Inner: White circle (set size to small)
  - Outer: Pie chart of `Volume Tier` → count of sellers
- **Colors:** High=Navy, Mid=Steel Blue, Low=Light Blue
- **Title:** "Sellers by Volume Tier"

### Dashboard 2 Layout
```
┌─────────────────────────────────┬────────────────────┐
│  Composite Score Bar Chart      │ Volume Tier Donut  │
│  (Top 30 sellers, ranked)       │                    │
├─────────────────────────────────┼────────────────────┤
│  Review vs On-Time Scatter      │ Seller Map         │
│  (4-quadrant view)              │                    │
└─────────────────────────────────┴────────────────────┘
```

**Quadrant labels (add as floating text boxes):**
- Top-Right: "Stars — High Reviews + On-Time"
- Top-Left: "Review Leaders — Review > Delivery"
- Bottom-Right: "Delivery Champions — Fast but Reviews Lag"
- Bottom-Left: "Improvement Needed"

---

## Dashboard 3: Regional Demand Map

**Data source:** `regional_demand.csv`

### Sheets to build

#### Sheet 3.1 — Brazil Choropleth Map
- **Mark type:** Map
- **Geographic role:** Set `State Name` column → State/Province
- **Color:** `Total Revenue` (continuous, sequential: light yellow to dark blue)
- **Tooltip:** State, Region, Total Orders, Total Revenue, Avg Order Value, Late Delivery Rate %
- **Title:** "Total Revenue by State"

**IMPORTANT for Brazil map:**
- In Tableau, go to Map → Edit Locations
- Set Country = Brazil
- Set State/Province = `State Name` column (uses full names, not abbreviations)

#### Sheet 3.2 — State Revenue Ranking (Horizontal Bar)
- **Rows:** `State` (sorted by `Total Revenue` descending)
- **Columns:** `Total Revenue`
- **Color:** `Region` (use region-based colors)
- **Add data labels:** Show revenue value
- **Title:** "Revenue by State"

#### Sheet 3.3 — Late Delivery Rate by State (Diverging Bar)
- **Rows:** `State`
- **Columns:** `Late Delivery Rate Pct`
- **Color:** Red (high late rate) to Green (low late rate)
- **Reference line:** National average late rate
- **Title:** "Late Delivery Rate by State (%)"

#### Sheet 3.4 — Avg Order Value by Region (Box Plot)
- **Rows:** `Avg Order Value`
- **Columns:** `Region`
- **Mark type:** Circle or Box-Whisker
- **Color:** `Region`
- **Title:** "Average Order Value by Region (BRL)"

### Dashboard 3 Layout
```
┌───────────────────────────┬──────────────────────────┐
│  Brazil Choropleth Map    │  State Revenue Ranking   │
│  (colored by revenue)     │  (horizontal bar chart)  │
│                           │                          │
├───────────────────────────┼──────────────────────────┤
│  Late Delivery by State   │  Avg Order Value Region  │
│  (diverging bar)          │  (box plot)              │
└───────────────────────────┴──────────────────────────┘
```

---

## Dashboard 4: AI-Powered Demand Forecast

**Data sources:** `forecast_output.csv` AND `anomaly_flags.csv`

This is the most impressive dashboard — it shows ML output directly in Tableau.

### Setup: Two Data Sources
1. Connect `forecast_output.csv` as primary source
2. Add `anomaly_flags.csv` as secondary source
3. Blend on `Order Date` field

### Sheets to build

#### Sheet 4.1 — Demand Forecast Line Chart (Main Visualization)
- **Columns:** `Order Date` (continuous)
- **Rows:** `Actual Orders` and `Forecast` (dual measure)
- **Mark type:** Line

**Calculated fields to create:**

```
// Is Forecast Period?
[Is Forecast] = TRUE

// Confidence Band (Area between bounds)
Use Lower Bound and Upper Bound as separate marks
```

**How to build the confidence band:**
1. Add `Lower Bound` and `Upper Bound` as separate marks
2. Change both to Area mark type
3. Dual axis and synchronize
4. Set fill color to light blue, 40% opacity
5. Actual orders: solid dark blue line, thickness 2
6. Forecast: dashed orange line, thickness 2

**Filter:** Category (allow user to select which category to view — add as a dashboard filter)

- **Title:** "90-Day Demand Forecast with Confidence Interval"
- **X-axis label:** "Date"
- **Y-axis label:** "Daily Orders"
- **Annotation:** Add a vertical reference line at the last actual date with label "Forecast starts here →"

#### Sheet 4.2 — Anomaly Detection Overlay (blended)
- **Columns:** `Order Date`
- **Rows:** `Daily Orders` from anomaly_flags.csv
- **Mark type:** Circle
- **Color:** `Is Anomaly` → Normal = Gray (small), Anomaly = Red (large)
- **Size:** `Anomaly Score` (bigger circle = more anomalous)
- **Tooltip:** Date, Orders, Anomaly Type, Deviation %, Anomaly Score
- **Title:** "Demand Anomaly Detection"

#### Sheet 4.3 — Anomaly Type Summary (Bar Chart)
- Filter to anomalies only (`Is Anomaly = TRUE`)
- **Rows:** `Anomaly Type`
- **Columns:** `COUNT(Order Date)`
- **Color:** `Anomaly Type`
- **Title:** "Anomaly Breakdown by Type"

#### Sheet 4.4 — Rolling Average vs. Actual (Dual Axis)
- **Rows:** `Daily Orders` (bar), `Rolling 7d Avg` (line)
- **Columns:** `Order Date`
- **Mark type:** Bar + Line
- **Color for bars:** If `Is Anomaly` = True → Red, else Gray
- **Title:** "Daily Orders vs. 7-Day Rolling Average"

### Dashboard 4 Layout
```
┌───────────────────────────────────────────────────────┐
│  [Category Filter Dropdown]                           │
│                                                       │
│  90-Day Demand Forecast + Confidence Band             │
│  (Full width, ~60% of dashboard height)               │
│                                                       │
├──────────────────────────┬────────────────────────────┤
│  Anomaly Detection       │  Rolling Avg vs. Actual    │
│  (scatter with anomalies │  (bar + line dual axis)    │
│   highlighted in red)    │                            │
│                   [Anomaly Type Bar — bottom strip]   │
└───────────────────────────────────────────────────────┘
```

---

## Tableau Story (Tying it Together)

Create a **Tableau Story** to present all 4 dashboards as a narrative:

**Story title:** "Olist E-Commerce Supply Chain Intelligence Report"

| Story Point | Dashboard | Narrative Caption |
|---|---|---|
| 1 | Executive KPI | "Overview: A growing marketplace facing fulfillment challenges" |
| 2 | Seller Scorecard | "Supplier Performance: Identifying your top and at-risk vendors" |
| 3 | Regional Demand | "Geographic Insights: Southeast dominates, but the North is underserved" |
| 4 | AI Forecast | "Looking Ahead: Demand forecasting with AI to plan inventory" |

---

## Color Palette (Consistent Across All Dashboards)

| Element | Color | Hex |
|---|---|---|
| Primary data | Steel Blue | `#1F77B4` |
| On-time / good | Forest Green | `#2CA02C` |
| Late / bad | Crimson | `#D62728` |
| At risk / warning | Amber | `#FF7F0E` |
| Forecast | Orange | `#FF7F0E` |
| Confidence band | Light Blue | `#AEC7E8` |
| Anomaly | Red | `#D62728` |
| Background | White | `#FFFFFF` |
| Grid lines | Light Gray | `#E5E5E5` |

---

## Publishing to Tableau Public

1. File → Save to Tableau Public
2. Sign in with your free Tableau Public account
3. Your workbook will be published at: `public.tableau.com/app/profile/YOUR_NAME`
4. Copy the URL for your resume/LinkedIn/GitHub README

---

## Tips for a Portfolio-Ready Dashboard

- Add your name and "Supply Chain Analytics Project" as a title banner
- Include a data source note: "Data: Olist Brazilian E-Commerce (Kaggle)"
- Include a method note on Dashboard 4: "AI Forecast: Facebook Prophet | Anomaly Detection: Isolation Forest"
- Use consistent font (Tableau Book or Calibri)
- Keep whitespace clean — less is more
- Test all tooltips — recruiters hover over everything
