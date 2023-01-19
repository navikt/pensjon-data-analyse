--Automatiseringsgrad det som er registrert i DP
Select 
EXTRACT(YEAR from k.DATO_OPPRETTET) ÅR,
EXTRACT(MONTH from k.DATO_OPPRETTET) MÅNED,
k.K_BEHANDLING_T,
count(1) as antall
from PEN.T_KRAVHODE k, PEN.T_SKJEMA s
where k.kravhode_id=s.kravhode_id
and s.k_skjema_pselv_t='AP'
--and s.OPPRETTET_AV between '00000000000' and '99999999999' --> bruker
--and s.OPPRETTET_AV not between '00000000000' and '99999999999' --> saksbehandler
--and EXTRACT (year from k.DATO_OPPRETTET) = 2021
group by EXTRACT(YEAR from k.DATO_OPPRETTET),
EXTRACT(MONTH from k.DATO_OPPRETTET),
k.K_BEHANDLING_T
order by 1 asc