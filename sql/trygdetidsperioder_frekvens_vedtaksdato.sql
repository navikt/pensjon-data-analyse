WITH personer as(
    SELECT
        EXTRACT(YEAR FROM v.dato_vedtak) ar,
        CASE
        WHEN t.k_land_3_tegn_id = 161 THEN
        'Norge'
        ELSE
        'Utland'
        END                              land,
        COALESCE(COUNT(t.trygdetid_grnl_id), 0) antall
    FROM
        pen.t_person_grunnlag p
        inner join pen.t_vedtak v on v.person_id = p.person_id
        inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
        LEFT JOIN pen.t_trygdetid_grnl t ON t.person_grunnlag_id = p.person_grunnlag_id
    WHERE kh.k_krav_gjelder in ('F_BH_BO_UTL','F_BH_KUN_UTL','F_BH_MED_UTL','FORSTEG_BH')
    AND (t.DATO_TOM <= v.dato_vedtak or t.DATO_TOM is null)
    and v.K_SAK_T = 'ALDER'
    GROUP BY
        p.fnr_fk,
        EXTRACT(YEAR FROM v.dato_vedtak),
        CASE
        WHEN t.k_land_3_tegn_id = 161 THEN
        'Norge'
        ELSE
        'Utland'
        END
)
SELECT ar, land, antall, count(*) frekvens
FROM personer
GROUP BY ar, land, antall
order by ar desc, land, antall asc