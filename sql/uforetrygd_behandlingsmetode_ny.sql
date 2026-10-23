with
ref_kravhode as (
    select
        kh.kravhode_id,
        kh.dato_opprettet,
        extract(year from kh.dato_opprettet) as ar_opprettet,
        kh.k_krav_gjelder

    from pen.t_kravhode kh

    inner join pen.t_sak s
        on kh.sak_id = s.sak_id

    where
        k_sak_t = 'UFOREP'
        and k_krav_s = 'FERDIG'

),

ref_kravlinje_s as (
    select
        kl.kravhode_id,
        kls.kravlinje_id,
        kls.opprettet_av,
        case
            when regexp_like(kls.opprettet_av, '^([A-Z])([0-9]{1,6})$') then 'SAKSBEH'
            when regexp_like(kls.opprettet_av, '^[0-9]{11}$') then 'BRUKER'
            else 'SYSTEM'
        end as opprettet_av_kode

    from pen.t_kravlinje_s kls

    inner join pen.t_kravlinje kl
        on kls.kravlinje_id = kl.kravlinje_id


),

kravlinje_s_opprettet_av as (
    select
        kravhode_id,
        kravlinje_id,
        count(
            case when opprettet_av_kode = 'SYSTEM' then 1 end
        ) as antall_av_system,
        count(
            case when opprettet_av_kode = 'SAKSBEH' then 1 end
        ) as antall_av_saksbehandler,
        count(
            case when opprettet_av_kode = 'BRUKER' then 1 end
        ) as antall_av_bruker
    from ref_kravlinje_s

    group by
        kravhode_id,
        kravlinje_id
),

kravlinje_behandling_metode as (
    select
        kravhode_id,
        kravlinje_id,
        antall_av_system,
        antall_av_saksbehandler,
        antall_av_bruker,
        case
            when antall_av_system > 0 and antall_av_saksbehandler > 0
                then 'DELAUTO'
            when antall_av_system > 0
                then 'HELAUTO'
            when antall_av_saksbehandler > 0
                then 'MAN'
            when antall_av_system = 0 and antall_av_saksbehandler = 0 and antall_av_bruker > 0
                then 'HELAUTO'
        end as behandling_metode

    from kravlinje_s_opprettet_av

),


krav_behandling_av as (
    select
        kravhode_id,
        count(kravlinje_id) as antall_kravlinjer,
        sum(antall_av_system) as antall_kravlinje_s_av_system,
        sum(antall_av_saksbehandler) as antall_kravlinje_s_av_saksbehandler,
        sum(antall_av_bruker) as antall_kravlinje_s_av_bruker,
        count(case when behandling_metode = 'HELAUTO' then 1 end) as antall_helauto_kravlinjer,
        count(case when behandling_metode = 'DELAUTO' then 1 end) as antall_delauto_kravlinjer,
        count(case when behandling_metode = 'MAN' then 1 end) as antall_man_kravlinjer

    from kravlinje_behandling_metode

    group by kravhode_id
),

krav_behandling_metoder as (
    select
        kravhode_id,
        antall_kravlinjer,
        antall_kravlinje_s_av_system,
        antall_kravlinje_s_av_saksbehandler,
        antall_kravlinje_s_av_bruker,
        antall_helauto_kravlinjer,
        antall_delauto_kravlinjer,
        antall_man_kravlinjer,
        case
            when antall_delauto_kravlinjer > 0
                then 'DELAUTO'
            when antall_helauto_kravlinjer > 0 and antall_man_kravlinjer > 0
                then 'DELAUTO'
            when antall_helauto_kravlinjer > 0 and antall_man_kravlinjer  = 0
                then 'HELAUTO'
            when antall_man_kravlinjer > 0 and antall_helauto_kravlinjer = 0
                then 'MAN'
            else 'UKJENT'
        end as krav_behandling_metode
    from krav_behandling_av
),

krav_behandling_final as (
    select
        kh.kravhode_id,
        kh.dato_opprettet,
        kh.ar_opprettet,
        kh.k_krav_gjelder,
        kbm.antall_kravlinjer,
        kbm.antall_kravlinje_s_av_system,
        kbm.antall_kravlinje_s_av_saksbehandler,
        kbm.antall_kravlinje_s_av_bruker,
        kbm.antall_helauto_kravlinjer,
        kbm.antall_delauto_kravlinjer,
        kbm.antall_man_kravlinjer,
        kbm.krav_behandling_metode

    from krav_behandling_metoder kbm

    inner join ref_kravhode kh on kbm.kravhode_id = kh.kravhode_id
)


select
    ar_opprettet,
    krav_behandling_metode,
    count(*) as antall

from krav_behandling_final

group by ar_opprettet, krav_behandling_metode

order by ar_opprettet, krav_behandling_metode
