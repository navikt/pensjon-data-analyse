SELECT
    p.person_id,
    t.*
FROM
    pen.t_person_grunnlag p
    inner join pen.t_vedtak v on v.person_id = p.person_id
    inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
    LEFT JOIN pen.t_trygdetid_grnl t ON t.person_grunnlag_id = p.person_grunnlag_id
WHERE kh.k_krav_gjelder in ('F_BH_BO_UTL','F_BH_KUN_UTL','F_BH_MED_UTL','FORSTEG_BH')
AND t.DATO_TOM <= v.dato_vedtak
and v.K_SAK_T = 'ALDER'
and v.k_vedtak_s = 'IVERKS'
and extract( year from v.dato_vedtak) = 2022

order by p.person_id