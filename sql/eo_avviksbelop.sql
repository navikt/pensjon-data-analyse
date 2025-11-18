-- viser aggregerte og binnede avviksbeløp for EO årlig
-- viser kun gyldige etterbetalinger og tilbakekrevinger

-- obs! denne fjerner saker med flere EO-hendelser per år
-- eg for 2023 er det litt over 1300 saker og for 2022 er det ca 700 saker

select
    ar,
    resultat,
    substr(avviksbelop_sortert, 3, 10) as avviksbelop,
    count(*) as antall,
    avviksbelop_sortert
from (
    select
        lower(k_ut_eo_resultat) as resultat,
        ar,
        case
            when (avviksbelop = 0 or avviksbelop is null) then 'a 0'
            when abs(avviksbelop) > 0 and abs(avviksbelop) < 1000 then 'b 0-1'
            when abs(avviksbelop) >= 1000 and abs(avviksbelop) < 5000 then 'c 1-5'
            when abs(avviksbelop) >= 5000 and abs(avviksbelop) < 10000 then 'd 5-10'
            when abs(avviksbelop) >= 10000 and abs(avviksbelop) < 15000 then 'e 10-15'
            when abs(avviksbelop) >= 15000 and abs(avviksbelop) < 20000 then 'f 15-20'
            when abs(avviksbelop) >= 20000 and abs(avviksbelop) < 30000 then 'g 20-30'
            when abs(avviksbelop) >= 30000 and abs(avviksbelop) < 50000 then 'h 30-50'
            when abs(avviksbelop) >= 50000 and abs(avviksbelop) < 100000 then 'i 50-100'
            when abs(avviksbelop) >= 100000 and abs(avviksbelop) < 150000 then 'j 100-150'
            when abs(avviksbelop) >= 150000 then 'k 150+'
        end as avviksbelop_sortert
    from pen.t_eo_ut_historik --noqa
    where
        1 = 1
        and er_gyldig = 1
        and k_hendelse_t is null
        and k_ut_eo_resultat in ('ETTERBET', 'TILBAKEKR')
        and (sak_id, ar) in (
            select sak_id, ar from pen.t_eo_ut_historik --noqa
            group by sak_id, ar
            having count(*) = 1
        ) -- tar kun med saker som har én EO-hendelse per år
)
group by
    ar,
    resultat,
    avviksbelop_sortert,
    substr(avviksbelop_sortert, 3, 10)
order by ar desc, resultat asc, avviksbelop_sortert asc
