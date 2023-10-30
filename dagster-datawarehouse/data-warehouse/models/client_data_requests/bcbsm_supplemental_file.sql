with
{{ import(ref('advantasure_billing'), 'billing') }},
{{ import(ref('dim_user_cureatr'), 'users') }},

filter_med_lists as (

    select
       billing.source_patient_id          as MemberKey,
       users.npi                          as ProviderKey,
       billing.mrp_visit_date::date       as DOS,
       billing.mrp_visit_date::date       as DOSthru,
       'RPH'                              as ProviderType,
       '1111F'                            as CPTPx,
       ''                                 as HCPCPSPx,
       ''                                 as LOINC,
       ''                                 as SNOWMED,
       ''                                 as ICDDX,
       'Z7189'                            as ICDDX10,
       ''                                 as RxNorm,
       ''                                 as CVX,
       ''                                 as Modifier,
       '0'                                as RxProviderFlag,
       '0'                                as PCPFlag,
       ''                                 as QuantityDispensed,
       ''                                 as ICDPx,
       ''                                 as ICDPx10,
       'M'                                as SuppSource,
       ''                                 as Result
    from billing
    join users on users.cureatr_user_id = billing.pharmacist_user_id
    where extract(year from map_transmission_date ) = 2023



)

select *
from filter_med_lists
