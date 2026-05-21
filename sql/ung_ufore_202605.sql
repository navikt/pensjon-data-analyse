-- Hvor mange har inntekt ved siden av uføretrygden?
--   vi har info ifmb det de oppgir -> evt før 2024 så har vi svaret
-- Hvor mange under 30 har barnetillegg? ja

-- Hvor mange unge har arbeidsforsøk, f eks hadde arbeidsforsøk ila 2025? ja, om etteroppgjort (altså før 2024)
-- i følge etteroppgjøret er det under 15 arbeidsforsøj per år 
-- Hvor mange unge har trekk i uføren (lønnstrekk eller trekk fra tidligere etteroppgjør)? nei, det vet oppdrag
--  Steinar Hansen evt dataprodukt på Økonomi

-- Hvor stort er gjennomsnittlig beløp for etteroppgjør for de under 30 og hvor mange unge får etteroppgjør?
--  t_etteroppgjor_hist 



with

ref_eo as (
    select
        hist.sak_id,
        hist.vedtak_id,
        hist.ar,
        hist.k_ut_eo_resultat,
        hist.dato_opprettet,
        cast(eo.arbeidsforsok as int) as arbeidsforsok,
        eor.avviksbelop, -- hvis avviket er innenfor tolleransegrensner, blir denne null når det joines inn
        first_value(eor.k_ut_eo_resultat) over (partition by hist.sak_id, hist.ar order by eor.dato_opprettet) as forste_k_ut_eo_resultat,
        hist.ar - extract(year from p.dato_fodsel) as alder,
        case when hist.ar - extract(year from p.dato_fodsel) < 31 then 1 else 0 end as under_31,
        case when eo.barnetillegg_sb = 1 or eo.barnetillegg_fb = 1 then 1 else 0 end as barnetillegg,
        count(*) over (partition by hist.sak_id, hist.ar) as antall_eo -- num rows in partition 
    from pen.t_eo_ut_historik hist
    left join pen.t_ut_etteroppgjor eo on hist.ut_etteroppgjor_id = eo.ut_etteroppgjor_id
    left join pen.t_vedtak v on hist.vedtak_id = v.vedtak_id
    left join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
    left join pen.t_eo_resultat_ut eor on v.eo_resultat_ut_id = eor.eo_resultat_ut_id
    inner join pen.t_sak s on s.sak_id = hist.sak_id
    left join pen.t_person p on p.person_id = s.person_id
),

agg_eo_per_ar as (
    select
        sak_id,
        ar,
        listagg(k_ut_eo_resultat, ',') within group (order by k_ut_eo_resultat) as k_ut_eo_resultat,
        listagg(arbeidsforsok, ',') within group (order by dato_opprettet) as arbeidsforsok_list,
        max(under_31) as under_31,
        max(k_ut_eo_resultat) keep (dense_rank last order by dato_opprettet) as siste_k_ut_eo_resultat,
        max(arbeidsforsok) keep (dense_rank last order by dato_opprettet) as arbeidsforsok,
        max(barnetillegg) keep (dense_rank last order by dato_opprettet) as barnetillegg,
        sum(avviksbelop) as sum_avviksbelop
    from ref_eo
    group by sak_id, ar
)

select 
    ar, 
    under_31,
    barnetillegg,
    --arbeidsforsok,
    round(sum(sum_avviksbelop)) totalt_avvik,
    round(avg(sum_avviksbelop), -2) gjennomsnitt_avvik,
    round(median(sum_avviksbelop), -2) median_avvik,
    sum(case when sum_avviksbelop < 0 then 1 else 0 end) antall_tilbakekrevinger,
    count(*) antall
from agg_eo_per_ar
where ar > 2017
group by ar, under_31, barnetillegg
order by ar, under_31, barnetillegg;
