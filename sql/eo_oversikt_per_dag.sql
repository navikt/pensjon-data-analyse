-- eo_oversikt.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.etteroppgjoret.eo_oversikt_per_dag

-- gir også oversikt over EO som eo_oversikt, men med dato_endret
-- gir altså mulighet til å se batch-kjøringene og saksbehandlingene per dag
select
    to_char(ar) as ar,
    dato_endret,
    auto_eller_manuell,
    k_ut_eo_resultat as resultat_eo,
    k_hendelse_t as hendelse,
    count(*) as antall
from (
    select
        lower(k_ut_eo_resultat) as k_ut_eo_resultat,
        lower(k_hendelse_t) as k_hendelse_t,
        trunc(dato_endret) as dato_endret,
        ar,
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
    from pen.t_eo_ut_historik
    where er_gyldig = 1 -- velger siste rad, altså den som er gyldig
)
group by
    ar,
    dato_endret,
    k_hendelse_t,
    k_ut_eo_resultat,
    auto_eller_manuell
order by ar desc, count(*) desc
