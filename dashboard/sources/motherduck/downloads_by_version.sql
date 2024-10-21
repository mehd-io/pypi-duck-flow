with
    cte_start_date as (
        select version, min(download_date) as start_date
        from duckdb_stats.main.pypi_daily_stats
        group by version
        order by start_date
    ),
    cte_downloads_by_day as (
        select download_date, sum(daily_download_sum) as daily_downloads
        from duckdb_stats.main.pypi_daily_stats
        group by all
    )
select
    st.download_date,
    st.version,
    sum(daily_download_sum) as downloads,
    st.download_date - sd.start_date as days_since_launch,
    (floor((st.download_date - sd.start_date) / 7) + 1)::int as week_id,
    downloads / avg(dd.daily_downloads) as pct_of_daily_downloads,
    case when st.version like '%dev%' then 1 else 0 end as is_dev_release
from duckdb_stats.main.pypi_daily_stats st
left join cte_start_date sd on sd.version = st.version
left join cte_downloads_by_day dd on dd.download_date = st.download_date
group by all
order by st.download_date
