-- viser oversikt over avviksbeløp per år

-- obs! denne fjerner saker med flere EO-hendelser per år
-- eg for 2023 er det litt over 1300 saker og for 2022 er det ca 700 saker

select
    ar,
    lower(k_ut_eo_resultat) as resultat,
    lower(k_hendelse_t) as hendelse,
    round(median(avviksbelop), -2) as median_avviksbelop,
    round(avg(avviksbelop), -2) as snitt_avviksbelop,
    round(sum(avviksbelop), -2) as total_avviksbelop,
    count(*) as antall
from pen.t_eo_ut_historik --noqa
where
    1 = 1
    and er_gyldig = 1
    and (sak_id, ar) in (
        select sak_id, ar from pen.t_eo_ut_historik --noqa
        group by sak_id, ar
        having count(*) = 1
    ) -- tar kun med saker som har én EO-hendelse per år
group by
    ar,
    k_ut_eo_resultat,
    k_hendelse_t
order by ar desc, resultat asc
