with
    {{ import(ref('stg_redshift_public__cmr_events'), 'cmr_events') }}



select *,
      case
          when lead(cmr_completed_sequence) over (partition by opportunity_id order by time_start) = 1
               and no_show = 0
           then true
           else false
      end as cmr_disposition_skip

from cmr_events