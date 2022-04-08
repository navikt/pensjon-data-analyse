--MÅNEDSDATA
--Førstegangskrav totalt minus krav som er opprettet i forbindelse med alderskonvertering
SELECT
EXTRACT(YEAR from k.DATO_OPPRETTET) ÅR, 
EXTRACT(MONTH from k.DATO_OPPRETTET) MÅNED,
count(1) antall
from PEN.t_person p
inner join PEN.t_sak s on s.person_id = p.PERSON_ID
inner join PEN.T_KRAVHODE k on k.SAK_ID = s.SAK_ID
where s.K_SAK_T = 'ALDER'
and k.K_KRAV_GJELDER in ('F_BH_BO_UTL','F_BH_MED_UTL','FORSTEG_BH','F_BH_KUN_UTL')
--and EXTRACT (year from k.DATO_OPPRETTET) = 2021
--and k.K_KRAV_S not like 'AVBRUTT'
and k.OPPRETTET_AV not like 'BPEN006'
group by 
EXTRACT(YEAR from k.DATO_OPPRETTET),
EXTRACT(MONTH from k.DATO_OPPRETTET)
order by EXTRACT(YEAR from k.DATO_OPPRETTET),
EXTRACT(MONTH from k.DATO_OPPRETTET) asc