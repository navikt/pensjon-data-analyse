SELECT
    *
FROM
    pen.t_person_grunnlag p
    inner join pen.t_vedtak v on v.person_id = p.person_id and v.K_SAK_T = 'ALDER' and v.k_vedtak_s = 'IVERKS' and v.k_vedtak_t = 'FORGANG'
    inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id and kh.k_krav_gjelder in ('F_BH_BO_UTL','F_BH_KUN_UTL','F_BH_MED_UTL','FORSTEG_BH')
    LEFT JOIN pen.t_trygdetid_grnl t ON t.person_grunnlag_id = p.person_grunnlag_id AND (t.DATO_TOM <= v.dato_vedtak)
    --inner join pen.t_trygdetid_eos eos ON eos.trygdetid_id = t.trygdetid_id
    --left join pen.t_tt_utl_trgd_avt utl ON utl.trygdetid_id = t.trygdetid_id
    --where eos.trygdetid_eos_id is not null --or utl.trygdetid
order by p.person_id