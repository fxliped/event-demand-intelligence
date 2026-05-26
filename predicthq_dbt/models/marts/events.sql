with stg as (
    select * from {{ ref('stg_events') }}
),

daily_agg as (
    select
        date_trunc('day', event_start)::date    as event_date,
        country,
        category,

        count(*)                                as event_count,
        sum(phq_attendance)                     as total_attendance,
        avg(phq_rank)                           as avg_phq_rank,
        max(phq_rank)                           as max_phq_rank,
        avg(duration_hours)                     as avg_duration_hours,
        count(distinct city)                    as distinct_cities
    from stg
    where event_start is not null
    group by 1, 2, 3
)

select * from daily_agg
order by event_date, country, category
