---
title: DuckDB 🦆 - Python 🐍 downloads
---

## How Many People Downloaded DuckDB ?

<BigValue 
    title='Last week downloads'
    data={download_last_week} 
    value='weekly_downloads' 
    fmt='#,##0.00,,"M"'	
/>

<BigValue 
    title='Last month downloads'
    data={download_last_month} 
    value='monthly_downloads' 
    fmt='#,##0.00,,"M"'	
/>

<BigValue 
    title='Total download'
    data={count_over_month} 
    value='weekly_download_sum' 
    fmt='#,##0.00,,"M"'	
/>

<BigValue 
  title='Data last updated on'
  data={last_refresh_date} 
  value=max_date
/>

## Download Over Months
<Grid cols=2>
<LineChart data = {download_week} y=weekly_downloads x=week_start_date  />

<DataTable data="{download_last_6_months}" search="false">
    <Column id="year_month" title="Year Month"/>
    <Column id="monthly_downloads" title="Monthly Downloads" />
</DataTable>
</Grid>

## Downloads by DuckDB and Python Version in the Last 30 Days
<Grid cols=2>
    <BarChart 
        data={download_duckdb_version}
        x=duckdb_version
        y=total_downloads 
        swapXY=true
    />
    <BarChart 
        data={download_python_version}
        x=python_version
        y=total_downloads 
        swapXY=true
    />
</Grid>

## Top 10 Countries Downloading DuckDB in the Last 30 Days

<BarChart 
    data={download_country}
    x=country_code
    y=total_downloads 
    swapXY=true
/>

## Adoption of New Versions over Time

<Alert>
Note: this only includes MotherDuck Compatible Versions (>1.1.0)
</Alert>

<Grid cols=2>
    <AreaChart 
    data={downloads_by_version_md}
    x=download_date
    y=downloads
    series=version
    />
    <AreaChart 
    data={downloads_by_version_md}
    x=download_date
    y=downloads
    series=version
    type=stacked100
    />
</Grid>

```sql count_over_month
SELECT  SUM(weekly_download_sum) as weekly_download_sum
FROM weekly_download
```

```sql download_month
SELECT 
    *
FROM monthly_download
```

```sql download_last_6_months
select * from ${download_month} limit 6
```

```sql download_week 
SELECT 
    week_start_date,
    SUM(weekly_download_sum) AS weekly_downloads
FROM 
    weekly_download
WHERE 
    week_start_date != (SELECT week_start_date FROM ${last_4_weeks} LIMIT 1) -- skipping the current week as it's not complete
GROUP BY 
    week_start_date
ORDER BY 
    week_start_date DESC
 
```

```sql download_last_month
SELECT
    year_month,
    monthly_downloads
FROM
    monthly_download
LIMIT 1 OFFSET 1
```

```sql download_last_week
SELECT 
    week_start_date, 
    weekly_downloads
FROM 
    ${download_week}
WHERE 
    week_start_date IN (SELECT week_start_date FROM ${last_4_weeks} OFFSET 1 LIMIT 1)
LIMIT 1
```

```sql last_4_weeks
SELECT DISTINCT week_start_date
FROM 
    weekly_download
WHERE 
    week_start_date >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 weeks')
ORDER BY 
    week_start_date DESC
```

```sql download_duckdb_version
SELECT 
    version AS duckdb_version,
    SUM(weekly_download_sum) AS total_downloads
FROM 
    weekly_download
WHERE 
    week_start_date IN (SELECT week_start_date FROM ${last_4_weeks})
GROUP BY 
    version
ORDER BY 
    total_downloads DESC
limit 10
```

```sql download_python_version
SELECT 
    python_version,
    SUM(weekly_download_sum) AS total_downloads
FROM 
    weekly_download
WHERE 
    week_start_date IN (SELECT week_start_date FROM ${last_4_weeks})
GROUP BY 
    python_version
ORDER BY 
    total_downloads DESC
limit 10
```

```sql download_country
SELECT 
    country_code,
    SUM(weekly_download_sum) AS total_downloads
FROM 
    weekly_download
WHERE 
    week_start_date IN (SELECT week_start_date FROM ${last_4_weeks})
GROUP BY 
    country_code
ORDER BY 
    total_downloads DESC
limit 10
```

```sql last_refresh_date
select max_date from motherduck.refresh_date
```

```sql downloads_by_version_md
select * from downloads_by_version
where is_dev_release = 0
and (version like '1.1%' or version like '1.2%' or version like '1.3%')
```

## Build Your Own Insights on Any Python Package
This dashboard is powered by [Evidence](https://evidence.dev/), [DuckDB](https://duckdb.org/), and [MotherDuck](https://motherduck.com/). You can find the code for this dashboard on [GitHub](https://github.com/mehd-io/pypi-duck-flow), along with [tutorials](https://www.youtube.com/watch?v=3pLKTmdWDXk) and examples to help you build your own dashboards.

## Accessing the raw data
You can query the raw data directly from any DuckDB client. Here's what you need : 
1) A MotherDuck account : sign-up for free at [MotherDuck](https://app.motherduck.com/)
2) Attach the [shared database to your workspace](https://motherduck.com/docs/getting-started/sample-data-queries/pypi)

```bash
ATTACH 'md:_share/duckdb_stats/1eb684bf-faff-4860-8e7d-92af4ff9a410' AS duckdb_stats;
```

*Made with ❤️ by 🧢 [mehdio](https://www.linkedin.com/in/mehd-io/)*



