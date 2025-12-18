# Forecast Disaccumulation

## Overview
This project implements the **Forecast Disaccumulation** step — transforming forecast data to the required **target time granularity** (e.g. from monthly to daily or weekly) by **proportional time-based disaggregation** of forecast volumes.

This step is required when the time granularity of forecasts produced by previous steps (VF / ML / Hybrid) **differs from the target output granularity**.

---

## Purpose of Disaccumulation
Input forecasts are defined over time intervals `[PERIOD_DT, PERIOD_END_DT]` (for example, a month). However, downstream systems expect forecasts at a **more granular time level** (day or week).

Disaccumulation:
- **preserves total forecast volume**,
- **does not introduce additional modeling assumptions**,
- performs a **uniform proportional split** of forecast values across finer time intervals.

The result is the same forecast expressed at the correct temporal resolution.

---

## Input Data
The main input is an aggregated forecast table:

`AGG_HYB_FCST`

It contains:
- hierarchical identifiers (product / location / customer / channel),
- forecast time interval (`PERIOD_DT`, `PERIOD_END_DT`),
- forecast measures:
  - `VF_FORECAST_VALUE`,
  - `ML_FORECAST_VALUE`,
  - `HYBRID_FORECAST_VALUE`,
- service attributes (`DEMAND_TYPE`, `ASSORTMENT_TYPE`, `SEGMENT_NAME`).

In the notebook, the input data is **synthetically generated** to:
- reproduce the production table structure,
- demonstrate algorithm correctness,
- ensure full reproducibility without external dependencies.

---

## Algorithm Logic

### 1. Time Granularity Check
For each record, the algorithm checks whether the interval `[PERIOD_DT, PERIOD_END_DT]` already corresponds to the target time granularity (`out_time_lvl`).

If the interval matches the target granularity, no transformation is applied.

---

### 2. Time Interval Splitting
If the input granularity is **coarser** than the target one:
- the original interval is split into consecutive sub-intervals of the target granularity (days or weeks),
- a new forecast record is generated for each sub-interval.

Example:
- input: `2015-02-01 – 2015-02-28` (monthly),
- output: 28 daily forecast records.

---

### 3. Proportional Volume Allocation
For each sub-interval, a share is calculated:

```
share = (number of days in sub-interval) / (number of days in original interval)
```

Forecast values are then scaled proportionally:

- `VF_FORECAST_VALUE *= share`
- `ML_FORECAST_VALUE *= share`
- `HYBRID_FORECAST_VALUE *= share`

A **simple linear allocation** is used, with no seasonal, calendar, or promotional adjustments — exactly as stated in the technical specification.

---

### 4. Business Key Preservation
All identifiers and categorical attributes:
- product / location / customer / channel,
- demand type,
- assortment type,
- segment

are **fully preserved** and replicated for the generated time-level records.

---

## Output Data
The result is the table:

`ACC_AGG_HYBRID_FORECAST`

Key properties:
- target time granularity (`DAY` or `WEEK.2`),
- forecast volumes remain consistent when aggregated back,
- compatible with downstream pipeline steps.

Structurally, the output matches the input table, but:
- `PERIOD_DT / PERIOD_END_DT` reflect the target granularity,
- forecast values are disaggregated accordingly.

---

## Notebook Implementation

In `disaccumulation.ipynb`:

1. Test data emulating `AGG_HYB_FCST` is generated.
2. Validation checks include:
   - output row count,
   - forecast volume conservation,
   - correctness of time interval splitting.
3. A simple test (`test_disaccumulation`) demonstrates the expected algorithm behavior.

---

## Assumptions and Limitations

- Forecasts are distributed **uniformly by day**.
- The algorithm does **not** account for:
  - holidays,
  - promotion calendars,
  - seasonal profiles.
- This behavior explicitly follows the requirement: *"no need to consider more complex logic"* from the specification.

