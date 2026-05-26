with source as (
    select
        raw,
        file_name,
        loaded_at
    from {{ source('raw', 'events_raw') }}
),

renamed as (
    select
        raw:id::string                                      as event_id,
        raw:title::string                                   as title,
        raw:category::string                                as category,
        raw:phq_labels::array                               as phq_labels,
        raw:state::string                                   as event_state,
        raw:start::timestamp_tz                             as event_start,
        raw:end::timestamp_tz                               as event_end,
        datediff('hour', raw:start::timestamp_tz,
                         raw:end::timestamp_tz)             as duration_hours,
        raw:country::string                                 as country,
        raw:location[0]::float                              as longitude,
        raw:location[1]::float                              as latitude,
        raw:geo:address:locality::string                    as city,
        raw:geo:address:region::string                      as region,
        raw:timezone::string                                as timezone,
        raw:rank::int                                       as phq_rank,
        raw:local_rank::int                                 as local_rank,
        raw:aviation_rank::int                              as aviation_rank,
        raw:phq_attendance::int                             as phq_attendance,
        raw:entities::array                                 as entities,
        raw:scope::string                                   as scope,
        raw:relevance::float                                as relevance,
        raw:brand_safe::boolean                             as brand_safe,
        raw:first_seen::timestamp_tz                        as first_seen,
        raw:updated::timestamp_tz                           as updated_at,
        file_name,
        loaded_at,
        row_number() over (
            partition by raw:id::string
            order by loaded_at desc
        )                                                   as _row_num
    from source
    where raw:id is not null
      and raw:private::boolean = false
)

select * exclude (_row_num)
from renamed
where _row_num = 1
