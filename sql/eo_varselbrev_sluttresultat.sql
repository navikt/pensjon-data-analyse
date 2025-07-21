-- eo_oversikt.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.etteroppgjoret.eo_varselbrev_sluttresultat

-- ser på de som får 4-ukers varselbrev om tilbakekreving

with

varselbrevmottakere as (
    -- todo: mulig det er bedre å bruke dato_onsket_virk enn dato_mottatt_krav
    select
        sak_id,
        extract(year from dato_mottatt_krav) - 1 as eo_ar,
        concat('Varselbrev om tilbakekreving fra ', to_char(dato_mottatt_krav, 'DD/MM/YYYY')) as varselbrev,
        dato_mottatt_krav
    from pen.t_kravhode kh
    where 1=1
        and kh.k_krav_gjelder = 'UT_VURDERING_EO' -- kun ett varselbrev per sakid per år
        and kh.dato_mottatt_krav in ( -- hovedkjøringene
            to_date('22/10/24', 'DD/MM/YY'),
            to_date('24/10/23', 'DD/MM/YY'),
            to_date('18/10/22', 'DD/MM/YY'),
            to_date('19/10/21', 'DD/MM/YY'),
            to_date('21/10/20', 'DD/MM/YY')
        )
),

eo_historikk as (
    select
        sak_id,
        ar,
        lower(k_ut_eo_resultat) as k_ut_eo_resultat,
        lower(k_hendelse_t) as k_hendelse_t,
        case
            when
                opprettet_av in ('AutomatiskBehandling', 'BPEN092','srvpensjon', 'srvpen-ejb-adapter')
                and endret_av in ('AutomatiskBehandling', 'BPEN092', 'srvpensjon', 'srvpen-ejb-adapter')
            then 'Automatisk'
            when
                opprettet_av not in ('AutomatiskBehandling', 'BPEN092', 'srvpensjon', 'srvpen-ejb-adapter')
                and endret_av not in ('AutomatiskBehandling', 'BPEN092', 'srvpensjon', 'srvpen-ejb-adapter')
            then 'Manuell'
        end as auto_eller_manuell
        -- count(*) as antall_eo -- for å sjekke de som har flere EO-rader samme år
    from pen.t_eo_ut_historik
    where er_gyldig = 1 -- velger siste rad, altså den som er gyldig
    -- dvs der hvor det er kommet en nyere EO rad på samme sak_id
    -- group by
    --     sak_id,
    --     ar,
    --     k_ut_eo_resultat,
    --     k_hendelse_t,
    --     auto_eller_manuell
),

-- eo_resultat as (...) kan brukes for å se endringer i avviksbelop

sammensmeltet_brevmottaker_historikk as (
    select
        eoh.sak_id,
        ar,
        k_ut_eo_resultat,
        auto_eller_manuell,
        k_hendelse_t,
        -- antall_eo,
        varselbrev,
        dato_mottatt_krav
    from eo_historikk eoh
    left join varselbrevmottakere vbm
        on
            eoh.sak_id = vbm.sak_id
            and eoh.ar = vbm.eo_ar
)

select
    to_char(ar) as ar,
    k_ut_eo_resultat as resultat_eo,
    k_hendelse_t as hendelse,
    auto_eller_manuell,
    -- antall_eo,
    varselbrev,
    dato_mottatt_krav,
    count(*) as antall
from sammensmeltet_brevmottaker_historikk
where varselbrev is not null -- har bare med de som får varselbrev om tilbakekreving
group by 
    ar,
    k_ut_eo_resultat,
    auto_eller_manuell,
    k_hendelse_t,
    -- antall_eo,
    varselbrev,
    dato_mottatt_krav
order by
    ar desc,
    count(*) desc
