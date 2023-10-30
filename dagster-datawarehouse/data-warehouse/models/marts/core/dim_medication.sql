with
{{ import(ref('stg_redshift_datascience__medispan_gpi_tc_hierarchy'), 'gpi_tc') }},
{{ import(ref('stg_redshift_datascience__medispan_drug_name'), 'drug_name') }},
{{ import(ref('stg_redshift_datascience__medispan_ndc'), 'ndc') }},

full_gpi as (
    select
        tcgpi_id as full_gpi_id,
        translate(tcgpi_name, '*', '') as full_gpi_name
    from
        gpi_tc
    where
        tc_level_code::integer = 14
    group by
        full_gpi_id, full_gpi_name
    order by
        full_gpi_id
                 )

   , drug_group as (
        select
            tcgpi_id as drug_group_id,
            translate(tcgpi_name, '*', '') as drug_group_name
        from
            gpi_tc
        where
            tc_level_code::integer = 2
        group by
            drug_group_id, drug_group_name
        order by
            drug_group_id
                   )

   , drug_class as (
        select
            tcgpi_id as drug_class_id,
            translate(tcgpi_name, '*', '') as drug_class_name
        from
            gpi_tc
        where
            tc_level_code::integer = 4
        group by
            drug_class_id, drug_class_name
        order by
            drug_class_id
                   )

   , drug_subclass as (
        select
            tcgpi_id as drug_subclass_id,
            translate(tcgpi_name, '*', '') as drug_subclass_name
        from
            gpi_tc
        where
            tc_level_code::integer = 6
        group by
            drug_subclass_id, drug_subclass_name
        order by
            drug_subclass_id
                      )

   , drug_base as (
        select
            tcgpi_id as drug_base_name_id,
            translate(tcgpi_name, '*', '') as drug_base_name
        from
            gpi_tc
        where
            tc_level_code::integer = 8
        group by
            drug_base_name_id, drug_base_name
        order by
            drug_base_name_id
                      )

   , drug_extension as (
        select
            tcgpi_id as drug_extension_id,
            translate(tcgpi_name, '*', '') as drug_extension_name
        from
            gpi_tc
        where
            tc_level_code::integer = 10
        group by
            drug_extension_id, drug_extension_name
        order by
            drug_extension_id
                      )

   , drug_dosage_form as (
        select
            tcgpi_id as drug_dosage_form_id,
            translate(tcgpi_name, '*', '') as drug_dosage_form_name
        from
            gpi_tc
        where
            tc_level_code::integer = 12
        group by
            drug_dosage_form_id, drug_dosage_form_name
        order by
            drug_dosage_form_id
                      )

    , drug_id_to_gpi as (
        select
        drug_name.drug_descriptor_id as drug_id,
        drug_name.strength,
        full_gpi.full_gpi_id
    from
        drug_name
        join full_gpi on full_gpi.full_gpi_id = drug_name.generic_product_identifier
    group by
        drug_descriptor_id, strength, full_gpi_id
                    )
--
   , drug_id_to_ndc as (
       select
            drug_descriptor_id as drug_id,
            ndc_upc_hri as ndc
       from
            ndc
       group by
            drug_descriptor_id, ndc_upc_hri
   )

    , final as (
        select
            full_gpi.full_gpi_id,
            drug_id_to_gpi.drug_id,
            drug_id_to_ndc.ndc,
            full_gpi.full_gpi_name,
            drug_group.drug_group_id,
            drug_group.drug_group_name,
            drug_class.drug_class_id,
            drug_class.drug_class_name,
            drug_subclass.drug_subclass_id,
            drug_subclass.drug_subclass_name,
            drug_base.drug_base_name_id,
            drug_base.drug_base_name,
            drug_extension.drug_extension_id,
            drug_extension.drug_extension_name,
            drug_id_to_gpi.strength,
            drug_dosage_form.drug_dosage_form_id,
            drug_dosage_form.drug_dosage_form_name,
            case when drug_class_id in (8337, 8310, 2710)
                or drug_base_name_id in (65200010, 65100025, 65991503, 65100030, 65991702, 65100035,
                                 65991803, 65100050, 65100055, 65100075, 65990002, 65100080,
                                 65100091, 65100095, 65995002)
                then 1
                else 0
            end as generic_risk,
            case when drug_group_id in (03, 59)
                or drug_class_id in (0400, 0800, 6410, 7250, 9055)
                or drug_subclass_id in (471000)
                or drug_base_name_id in (01300020, 02300090, 12104515, 12104530, 15000007, 16200010,
                                 16270030, 16800050, 34000030, 35200020, 41200010, 43401001,
                                 43401003, 43401005, 46400010, 49103010, 52543060, 58200030,
                                 58200050, 59157060, 60201025, 65100020, 65100095, 65991002,
                                 65991004, 65995002, 69100040, 70400050, 72100060, 72600040,
                                 79600020, 79600030, 86101030, 86300012, 86750016, 88150020,
                                 88200006, 88350010, 88350065, 89200010, 89200025, 90070070,
                                 90220010, 90850010, 90850060, 90900020, 90900030, 90972080,
                                 90972084, 92100030, 93400020, 98351012)
                then 1
                else 0
            end as pediatric_risk
            , case when drug_group_id in (23, 24, 25, 26, 30, 31, 34, 41, 43, 55, 57, 59, 60, 61, 65, 66,
                                  72, 75)
                or drug_class_id in (2720, 3510, 3530, 3540, 3620, 4910, 6410, 7310, 8515)
                or drug_subclass_id in (271040, 279910, 312000, 369915, 582000, 604000, 629920, 649910)
                or drug_base_name_id in (21100015, 21100020, 21500020, 27607050, 33100045, 46400010,
                                 50200070, 64100010, 76000030, 85150010, 90972080, 90972084)
                then 1
                else 0
            end as geriatric_risk
            , case when drug_class_id in (3310, 3320, 3610, 3760, 8625)
                or drug_base_name_id in (01200010, 01990002, 02100010, 02100015, 02100020, 02200040,
                                 02200057, 02200060, 02200062, 02200065, 02300060, 02300065,
                                 02300075, 02300080, 02300080, 02300090, 02400040, 02990002,
                                 03400010, 03500010, 11407015, 11407035, 12405010, 12405085,
                                 12990002, 16150030, 16150050, 16159902, 16159902, 16159903,
                                 27200020, 27200030, 27200040, 27250050, 27500010, 27992502,
                                 27992502, 27997002, 27997002, 27997002, 27998802, 37200010,
                                 37200030, 37200080, 37500010, 37500020, 37500030, 37600060,
                                 37990002, 37990002, 37990002, 49993003, 49993202, 49993203,
                                 49993203, 86101004, 90159902, 90159903, 90159904, 90350010,
                                 90359902)
                then 1
                else 0
            end as renal_risk
            , case when drug_group_id in (33, 34)
                or drug_class_id in (3610, 3615, 8625)
                or drug_base_name_id in (05000050, 11407080, 11500025, 12104515, 12104520, 12104525,
                                 12104530, 12104530, 12104545, 12104560, 12105005, 12109902,
                                 12109903, 12109904, 12500070, 16230040, 16270030, 21200020,
                                 21402420, 21402860, 21531835, 21532560, 21990002, 32100030,
                                 35100010, 35100020, 35100030, 35200020, 35200025, 35300010,
                                 35300050, 35400005, 36202030, 36202040, 36402030, 36991502,
                                 36992002, 36992002, 37200030, 38000083, 40143060, 40304070,
                                 40304090, 43995202, 43995202, 43995202, 43995303, 43995303,
                                 43997002, 43997303, 49270025, 49270070, 50250065, 54100010,
                                 54100055, 58180090, 60201025, 60204035, 60990003, 61354015,
                                 62051030, 62549902, 62549903, 62549904, 65100015, 65100020,
                                 65100025, 65100045, 65100050, 65100055, 65100087, 65100090,
                                 65100095, 65200040, 65991002, 65991004, 65991303, 65991503,
                                 65991503, 65995002, 66109902, 66109903, 66991004, 69100040,
                                 69990002, 69991002, 69991003, 72100060, 75990003, 83100020,
                                 85156010, 85158020, 85159902, 85601010, 86101047, 86501030,
                                 86750016, 86759902, 86759902, 86759903, 87100060, 88350065,
                                 88501560, 89200025, 89254060, 89991002, 89992002, 89992003,
                                 90070070, 90079904, 90219903, 90219903, 90219904, 90219904,
                                 90219906, 90850060, 90850060, 90859902, 90859902, 90859903,
                                 90859903, 90859904, 90859904, 90949903, 98609902, 99404070)
                then 1
                else 0
            end as hepatic_risk
            , case when drug_group_id in (37, 41, 43, 65, 72)
                or drug_class_id in (3310, 5440, 5710, 7420, 7510, 7520, 7599)
                or drug_base_name_id in (62100080, 82680080)
                then 1
                else 0
            end as copd_risk
            , case when drug_group_id in (03, 05)
                or drug_class_id in (6610)
                then 1
                else 0
            end as pneumonia_risk
            , case when drug_group_id in (61)
                or drug_class_id in (5820, 6610, 7020, 8625, 8650)
                or drug_subclass_id in (362020, 568520, 602060, 662700)
                or drug_base_name_id in (11000010, 11407035, 13000010, 13000020, 21101020, 21101025,
                                 21170054, 21170070, 21200030, 21200040, 21200042, 21200045,
                                 21200050, 21200055, 21300005, 21300030, 21335020, 21355070,
                                 21500005, 21500012, 21531835, 21533026, 21533060, 21533070,
                                 21700060, 21990002, 21990003, 21990003, 27250050, 27550065,
                                 27550070, 27574020, 27992502, 27995002, 27996002, 27997002,
                                 27998802, 33100045, 34000010, 34000020, 34000030, 35100010,
                                 35300010, 35400028, 36202005, 36400020, 36991502, 40160015,
                                 40170040, 44201010, 44209902, 52505020, 52505040, 56852025,
                                 58160020, 59152020, 59400015, 59500010, 62403060, 62540060,
                                 66290030, 67000020, 67991002, 70400010, 70400020, 70400050,
                                 72600020, 72600057, 73200020, 73203060, 85155516, 85156010,
                                 86101085, 86655020, 90159902, 90159904, 99392070, 99394050)
                then 1
                else 0
            end as heart_failure_risk
        from
            full_gpi
            left join drug_group on left(full_gpi_id, 2) = drug_group.drug_group_id
            left join drug_class on left(full_gpi_id, 4) = drug_class.drug_class_id
            left join drug_subclass on left(full_gpi_id, 6) = drug_subclass.drug_subclass_id
            left join drug_base on left(full_gpi_id, 8) = drug_base.drug_base_name_id
            left join drug_extension on left(full_gpi_id, 10) = drug_extension.drug_extension_id
            left join drug_dosage_form on left(full_gpi_id, 12) = drug_dosage_form.drug_dosage_form_id
            left join drug_id_to_gpi on full_gpi.full_gpi_id = drug_id_to_gpi.full_gpi_id
            left join drug_id_to_ndc on drug_id_to_ndc.drug_id = drug_id_to_gpi.drug_id
        order by
            full_gpi.full_gpi_id
    )

select *
from final