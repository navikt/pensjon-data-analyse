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
        case when kl.k_kravlinje_t = 'BT' then 1 else 0 end as flagg_bt,
        -- UT_GJT, FAST_UTG_INST, ET(få så mulig droppe?), BT
        -- kl.k_kravlinje_t,
        -- kls.k_kravlinje_s,
        kl.k_land_3_tegn_id
    from kravhoder kh
    left join pen.t_kravlinje kl on kh.kravhode_id = kl.kravhode_id
    -- left join pen.t_kravlinje_s kls on kl.kravlinje_s_id = kls.kravlinje_s_id
)

select
    count(*) as ant,
    count(distinct sak_id) as ant_saker,
    k_kravlinje_t
    -- k_vedtak_t
from kravlinjer
group by
    k_kravlinje_t
    -- k_vedtak_t
-- ;
-- 
-- 
-- select * from pen.t_sak;
-- select * from pen.t_vedtak;
-- select * from pen.t_kravlinje;
-- select * from pen.t_kravlinje_s;
-- select
--     kl.kravlinje_id,
--     kl.k_land_3_tegn_id,
--     kl.k_kravlinje_t,
--     kls.k_kravlinje_s,
--     kl.kravlinje_s_id
-- from pen.t_kravlinje kl
-- left join pen.t_kravlinje_s kls on kl.kravlinje_s_id = kls.kravlinje_s_id;
-- select * from pen.t_kravhode;
