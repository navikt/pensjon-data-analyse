-- lage et view med 1 rad per uføre og litt informasjon som
-- - alder
-- - andre løpende sakstyper på samme person
-- - flagg for kravlinjer: BT, GJT, ...
-- - EPS flagg
-- - alder ved innvilgelse av uføretrygd i saken
-- - første løpende dato
-- - inntektk i år


-- avgrensinger
-- 1. starter med kun løpende vedtak


with

saker as (
    select
        sak_id,
        person_id,
        k_sak_s
    from pen.t_sak
    where
        k_sak_t = 'UFOREP'
        -- and k_sak_s != 'AVSLUTTET'
),

-- joiner inn (løpende) vedtak før kravhode for å få kun 1 rad per sak
vedtak_lopende as (
    select
        s.*,
        v.vedtak_id,
        v.kravhode_id,
        v.k_vedtak_t,
        v.k_vedtak_s
        -- v.k_vilkar_resul_t,
        -- v.k_klageank_res_t,
        -- v.dato_vedtak,
    from saker s
    left join pen.t_vedtak v on s.sak_id = v.sak_id
    where
        v.dato_lopende_fom <= sysdate
        and (v.dato_lopende_tom is null or v.dato_lopende_tom >= trunc(sysdate))
),

kravhoder as (
    select
        v.*,
        kh.k_krav_gjelder,
        kh.k_behandling_t,
        kh.k_krav_s,
        kh.dato_mottatt_krav
    from vedtak_lopende v
    left join pen.t_kravhode kh on v.kravhode_id = kh.kravhode_id
),

kravlinjer as (
    select
        kh.*,
        case when kl.k_kravlinje_t = 'UT' then 1 else 0 end as ut_flagg,
        case when kl.k_kravlinje_t = 'UT_GJT' then 1 else 0 end as ut_gjt_flagg,
        case when kl.k_kravlinje_t = 'FAST_UTG_INST' then 1 else 0 end as fast_utg_inst_flagg,
        case when kl.k_kravlinje_t = 'ET' then 1 else 0 end as et_flagg,
        case when kl.k_kravlinje_t = 'BT' then 1 else 0 end as bt_flagg,
        kl.k_kravlinje_t,
        kl.k_land_3_tegn_id
    from kravhoder kh
    left join pen.t_kravlinje kl on kh.kravhode_id = kl.kravhode_id
),

agg_kravlinjer as (
    select
        vedtak_id,
        -- må være max på neste og ikke sum, fordi bla. BT er flere barn og UT er fordelt på land
        max(ut_flagg) as ut_flagg,
        max(ut_gjt_flagg) as ut_gjt_flagg,
        max(fast_utg_inst_flagg) as fast_utg_inst_flagg,
        max(et_flagg) as et_flagg,
        max(bt_flagg) as bt_flagg
    from kravlinjer
    group by vedtak_id
)

select
    count(*) as ant,
    count(distinct vedtak_id) as ant_saker,
    ut_flagg,
    ut_gjt_flagg,
    fast_utg_inst_flagg,
    et_flagg,
    bt_flagg
from agg_kravlinjer
group by
    ut_flagg,
    ut_gjt_flagg,
    fast_utg_inst_flagg,
    et_flagg,
    bt_flagg
order by ant desc
