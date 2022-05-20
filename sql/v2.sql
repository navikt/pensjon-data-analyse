SELECT
EXTRACT(YEAR FROM k.DATO_OPPRETTET) ÅR, 
EXTRACT(MONTH FROM k.DATO_OPPRETTET) MÅNED,
CASE
    WHEN sk.OPPRETTET_AV IS NULL THEN
        'saksbehandler'  
    WHEN sk.OPPRETTET_AV BETWEEN '00000000000' AND '99999999999' THEN
        'bruker'
    WHEN sk.OPPRETTET_AV NOT BETWEEN '00000000000' AND '99999999999' THEN
        'saksbehandler'
END OPPRETTET_AV,
CASE
    WHEN k.k_behandling_t = 'DEL_AUTO' THEN
        'MAN'
    ELSE k.k_behandling_t
END AUTOMATISERING,
CASE k.K_KRAV_GJELDER
    WHEN 'F_BH_BO_UTL' THEN 'bosatt utland'
    WHEN 'F_BH_MED_UTL' THEN 'Norge/utland'
    WHEN 'FORSTEG_BH' THEN 'Norge'
    WHEN 'F_BH_KUN_UTL' THEN 'kun utland'
END UTLANDSTILSNITT,
CASE 
    WHEN k.OPPRETTET_AV NOT LIKE 'BPEN006' THEN 'ikke opprettet av batch'
    ELSE 'opprettet av batch'
END BATCH_FLAGG,
COUNT(1) ANTALL

FROM PEN.t_person p
INNER JOIN PEN.t_sak s ON s.person_id = p.PERSON_ID
INNER JOIN PEN.T_KRAVHODE k ON k.SAK_ID = s.SAK_ID
LEFT JOIN PEN.T_SKJEMA sk on sk.kravhode_id=k.kravhode_id

WHERE s.K_SAK_T = 'ALDER'
AND k.K_KRAV_GJELDER IN ('F_BH_BO_UTL','F_BH_MED_UTL','FORSTEG_BH','F_BH_KUN_UTL')
--and EXTRACT (year from k.DATO_OPPRETTET) = 2021
--and k.K_KRAV_S not like 'AVBRUTT'

----- BPEN006 eneste batch siden 2012. For å se på eldre må vi fjerne andre batcher og konvertering i tillegg: -----
--AND SUBSTR(k.OPPRETTET_AV,2,3) not like 'PEN'
--AND k.OPPRETTET_AV not like 'KONVERTERING'

GROUP BY 
EXTRACT(YEAR FROM k.DATO_OPPRETTET),
EXTRACT(MONTH FROM k.DATO_OPPRETTET),
CASE
    WHEN sk.OPPRETTET_AV IS NULL THEN
        'saksbehandler'    
    WHEN sk.OPPRETTET_AV BETWEEN '00000000000' AND '99999999999' THEN
        'bruker'
    WHEN sk.OPPRETTET_AV NOT BETWEEN '00000000000' AND '99999999999' THEN
        'saksbehandler'
END,
CASE
    WHEN k.k_behandling_t = 'DEL_AUTO' THEN
        'MAN'
    ELSE k.k_behandling_t
END,
CASE k.K_KRAV_GJELDER
    WHEN 'F_BH_BO_UTL' THEN 'bosatt utland'
    WHEN 'F_BH_MED_UTL' THEN 'Norge/utland'
    WHEN 'FORSTEG_BH' THEN 'Norge'
    WHEN 'F_BH_KUN_UTL' THEN 'kun utland'
END,
CASE 
    WHEN k.OPPRETTET_AV NOT LIKE 'BPEN006' THEN 'ikke opprettet av batch'
    ELSE 'opprettet av batch'
END

ORDER BY EXTRACT(YEAR FROM k.DATO_OPPRETTET),
EXTRACT(MONTH FROM k.DATO_OPPRETTET) ASC
