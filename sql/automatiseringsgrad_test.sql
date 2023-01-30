SELECT
EXTRACT(YEAR FROM k.dato_opprettet) ÅR, 
TRUNC((EXTRACT(MONTH FROM k.dato_opprettet) - 1) / 4) + 1 TERTIAL,
EXTRACT(MONTH FROM k.dato_opprettet) MÅNED,
TO_CHAR(k.dato_opprettet, 'yyyy-mm') "ÅR-MÅNED",
CONCAT(EXTRACT(YEAR FROM k.dato_opprettet), TRUNC((EXTRACT(MONTH FROM k.dato_opprettet) - 1) / 4) + 1) "ÅR-TERTIAL",
CASE
    WHEN sk.opprettet_av is not null OR k.kravkilde is not null THEN 
      'PSELV'
    ELSE 
      'MAN'
END SYSTEM,
CASE
    WHEN k.k_behandling_t = 'DEL_AUTO' THEN
        'MAN'
    ELSE k.k_behandling_t
END AUTOMATISERING,
DIM_K.DEKODE KRAVTYPE,
CASE
    WHEN k.OPPRETTET_AV BETWEEN '00000000000' AND '99999999999' THEN
        'Bruker'
    WHEN SUBSTR(k.OPPRETTET_AV,1,1) between 'A' and 'Z' AND SUBSTR(k.OPPRETTET_AV,2,1) between '0' and '9' THEN
        'Saksbehandler'
    WHEN SUBSTR(k.OPPRETTET_AV,1,4) like 'BPEN' THEN
        'Batch'
    WHEN SUBSTR(k.OPPRETTET_AV,1,4) like 'PPEN' OR SUBSTR(k.OPPRETTET_AV,1,4) like 'AUTO' OR k.OPPRETTET_AV like 'PP01' THEN
        'Prosess'
    WHEN SUBSTR(k.OPPRETTET_AV,1,4) like 'TPEN' THEN
        'Tjeneste'
    ELSE k.OPPRETTET_AV
END OPPRETTET_AV,
k.SOKT_AFP_PRIVAT,
k.BODD_ARB_UTL

FROM PEN.t_person p
INNER JOIN PEN.t_sak s ON s.person_id = p.PERSON_ID
INNER JOIN PEN.T_KRAVHODE k ON k.SAK_ID = s.SAK_ID
INNER JOIN PEN.T_K_KRAV_GJELDER dim_k on dim_k.k_krav_gjelder = k.k_krav_gjelder
LEFT JOIN PEN.T_SKJEMA sk on sk.kravhode_id=k.kravhode_id

WHERE s.K_SAK_T = 'ALDER'
AND k.K_KRAV_GJELDER IN ('F_BH_BO_UTL','F_BH_MED_UTL','FORSTEG_BH','F_BH_KUN_UTL')
v
-- and k.kravkilde IS NOT NULL
and k.K_KRAV_S not like 'AVBRUTT'
----- BPEN006 eneste batch siden 2012. For å se på eldre må vi fjerne andre batcher og konvertering i tillegg: -----
--AND SUBSTR(k.OPPRETTET_AV,2,3) not like 'PEN'
--AND k.OPPRETTET_AV not like 'KONVERTERING'

ORDER BY ÅR, MÅNED ASC
