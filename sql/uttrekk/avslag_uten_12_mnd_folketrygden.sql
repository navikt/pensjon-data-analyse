-- forespørsel fra Lars i NAY styringsenhet
-- uttrekk på antall uføreavslag uten 12mnd medlemskap i folketrygden
-- 12 mnd medlemskap i folketrygden er under vurdering av trygdeavtale i skjermbildene
-- skal hente ut:
--> totalt antall saker med avslag
--> årlig, feks for 2023
--> hvor alle sakene har krysset av vurdert "nei" i det feltet om 12 mnd medlemsskap


-- denne går fortest, og gir antall avslag per år som har "nei" på 12mnd medlemskap i folketrygden
select
    extract(year from v.dato_vedtak) as ar, -- var interessert i 2023, men kan ta ut alle læl
    count(*) as antall_avslag_ufore_uten_12mnd
from pen.t_trygdeavtale ta
inner join pen.t_person_grunnlag pg on pg.person_grunnlag_id = ta.person_grunnlag_id
inner join pen.t_kravhode kh on kh.kravhode_id = pg.kravhode_id
inner join pen.t_vedtak v on v.kravhode_id = kh.kravhode_id
where
    v.k_sak_t = 'UFOREP'
    and v.k_vedtak_t = 'AVSL' -- alle avslag
    and v.person_id = pg.person_id -- sikrer at pg er søkeren
    and ta.minst_12mnd_medl_flketrgd = '0' -- det Lars lurer på
    and extract(year from v.dato_vedtak) >= 2020
group by extract(year from v.dato_vedtak), ta.minst_12mnd_medl_flketrgd;


-- join motsatt vei gjør at vi også kan sammenlikne totalt antall avslag per år
select
    extract(year from v.dato_vedtak) as ar, -- var interessert i 2023, men kan ta ut alle læl
    count(*) as antall_avslag,
    case
        when minst_12mnd_medl_flketrgd = '1' then 'Ja'
        when minst_12mnd_medl_flketrgd = '0' then 'Nei'
        else 'Ikke vurdert'
    end as minst_12mnd_medl_flketrgd
from pen.t_vedtak v
left join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
left join pen.t_person_grunnlag pg on pg.kravhode_id = kh.kravhode_id
left join pen.t_trygdeavtale ta on ta.person_grunnlag_id = pg.person_grunnlag_id
where
    v.k_sak_t = 'UFOREP'
    and v.k_vedtak_t = 'AVSL' -- alle avslag
    and v.person_id = pg.person_id -- sikrer at pg er søkeren
    and extract(year from v.dato_vedtak) >= 2020
group by
    extract(year from v.dato_vedtak),
    case
        when minst_12mnd_medl_flketrgd = '1' then 'Ja'
        when minst_12mnd_medl_flketrgd = '0' then 'Nei'
        else 'Ikke vurdert'
    end
order by extract(year from v.dato_vedtak) desc, minst_12mnd_medl_flketrgd desc;
