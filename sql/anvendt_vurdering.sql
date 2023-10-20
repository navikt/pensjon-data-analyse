select d_vv.dekode as anvendt_vurdering, extract(year from vv.dato_opprettet) as aar, TO_CHAR(vv.dato_opprettet, 'yyyy-mm') as aar_maaned, count(*) antall
from pen.t_vilkar_vedtak vv
    inner join pen.t_k_vilk_vurd_t d_vv on d_vv.k_vilk_vurd_t = vv.k_vilk_vurd_t
    inner join pen.t_vedtak v on v.vedtak_id = vv.vedtak_id
    inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
    inner join pen.t_sak s on s.sak_id = v.sak_id
where s.k_sak_t = 'ALDER'
    and kh.k_krav_gjelder in ('F_BH_BO_UTL','F_BH_MED_UTL','FORSTEG_BH','F_BH_KUN_UTL')
group by extract(year from vv.dato_opprettet), TO_CHAR(vv.dato_opprettet, 'yyyy-mm'), d_vv.dekode
order by aar_maaned desc, antall desc
