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
--and substr(t.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(t.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') --Bare innslag saksbehandler fører inn?

order by p.person_id, t.dato_fom, t.dato_tom