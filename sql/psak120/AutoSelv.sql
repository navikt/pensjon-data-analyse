--Automatiseringsgrad det som er registrert i DP
Select 
EXTRACT(YEAR from k.DATO_OPPRETTET) ÅR,
EXTRACT(MONTH from k.DATO_OPPRETTET) MÅNED,
CASE
    WHEN s.OPPRETTET_AV BETWEEN '00000000000' AND '99999999999' THEN
        'bruker'
    WHEN s.OPPRETTET_AV NOT BETWEEN '00000000000' AND '99999999999' THEN
        'saksbehandler'
END OPPRETTET_AV,
k.K_BEHANDLING_T,
count(1) as antall
from PEN.T_KRAVHODE k, PEN.T_SKJEMA s
where k.kravhode_id=s.kravhode_id
and s.k_skjema_pselv_t='AP'
group by EXTRACT(YEAR from k.DATO_OPPRETTET),
EXTRACT(MONTH from k.DATO_OPPRETTET),
CASE
    WHEN s.OPPRETTET_AV BETWEEN '00000000000' AND '99999999999' THEN
        'bruker'
    WHEN s.OPPRETTET_AV NOT BETWEEN '00000000000' AND '99999999999' THEN
        'saksbehandler'
END,
k.K_BEHANDLING_T
order by 1 asc