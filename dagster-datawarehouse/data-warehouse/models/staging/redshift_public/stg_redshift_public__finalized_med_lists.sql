with
{{ import(source('redshift_public', 'fact_medlist'), 'medlists') }},

filter_finalized_med_lists as (

    select
        id as med_list_id,
        iid as institution_id,
        discharge_event_id,
        coalesce(document.patient_opportunity_id::varchar, discharge_event_id) AS opportunity_id,
        patient_id,
        user_id,
        pharmacist_user_id,
        document.coauthor_user_id::varchar as coauthor_user_id,
        document.provider_summary::varchar,
        status as map_status,
        time_created,
        time_updated,
        time_finalized as map_finalized_date,
        convert_timezone('EST', time_finalized) as map_finalized_date_est,

        case
            when lower(map_status) = '{{ var("map_finalized_status") }}'
            then 1 else 0
        end as map_finalized,

        row_number() over (partition by opportunity_id order by map_finalized_date) as map_sequence,
        row_number() over (partition by opportunity_id order by map_finalized_date desc) as map_sequence_desc,

        case
            when lower(map_status) = '{{ var("map_finalized_status") }}'
            then row_number() over (partition by discharge_event_id, map_status order by time_finalized)
        end as map_finalized_sequence,

        case
            when lower(map_status) = '{{ var("map_finalized_status") }}'
            then row_number() over (partition by discharge_event_id, map_status order by time_finalized desc)
        end as map_finalized_sequence_desc

    from medlists
    where lower(map_status) = '{{ var("map_finalized_status") }}'

)

select *
from filter_finalized_med_lists
