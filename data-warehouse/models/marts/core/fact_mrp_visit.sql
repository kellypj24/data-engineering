with
    {{ import(ref('stg_redshift_public__mrp_events'), 'mrp_events') }}



select *,
      case
          when lead(mrp_completed_sequence) over (partition by opportunity_id order by time_start) = 1
               and no_show = 0
           then true
           else false
      end as mrp_disposition_skip

from mrp_events
