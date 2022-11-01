SELECT dim_krav.dekode kravtype, count(1) antall

FROM PEN.t_person p
INNER JOIN PEN.t_sak s ON s.person_id = p.PERSON_ID
INNER JOIN PEN.T_KRAVHODE k ON k.SAK_ID = s.SAK_ID
LEFT JOIN PEN.t_k_krav_gjelder dim_krav on k.k_krav_gjelder = dim_krav.k_krav_gjelder

GROUP BY dim_krav.dekode

ORDER BY COUNT(1)