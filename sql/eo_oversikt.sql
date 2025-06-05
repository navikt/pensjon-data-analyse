-- gir en oversikt over utfall og hendelser for EO årlig
-- dataprodukt som blir visualisert i Metabase
select
    ar,
    auto_eller_manuell,
    k_ut_eo_resultat as resultat_eo,
    k_hendelse_t as hendelse,
    count(*) as antall
from (
    select
        lower(k_ut_eo_resultat) as k_ut_eo_resultat,
        lower(k_hendelse_t) as k_hendelse_t,
        ar,
        case
            when
                opprettet_av in ('AutomatiskBehandling', 'BPEN092')
                and endret_av in ('AutomatiskBehandling', 'BPEN092')
            then 'Automatisk'
            when
                opprettet_av not in ('AutomatiskBehandling', 'BPEN092')
                and endret_av not in ('AutomatiskBehandling', 'BPEN092')
            then 'Manuell'
            else 'Delautomatisk (opprettet/endret manuelt)'
        end as auto_eller_manuell
    from pen.t_eo_ut_historik
)
group by
    ar,
    k_hendelse_t,
    k_ut_eo_resultat,
    auto_eller_manuell
order by ar desc, count(*) desc
