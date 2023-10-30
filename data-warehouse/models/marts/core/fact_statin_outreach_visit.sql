with
    {{ import(ref('stg_redshift_public__statin_outreach_events'), 'statin_outreach_events') }}



select *,
      case
          when lead(statin_outreach_completed_sequence) over (partition by opportunity_id order by time_start) = 1
               and no_show = 0
           then true
           else false
      end as statin_outreach_disposition_skip

from statin_outreach_events
