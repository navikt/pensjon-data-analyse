select trunc(r.dato_opprettet) as dato, d_s.dekode as sakstype, d_k.dekode as kravtype, d_r.dekode as retur_arsak, count(*) as antall 
from PEN.t_krav_retur_arsak r
inner join PEN.t_k_krav_ret_arsak d_r on d_r.k_krav_ret_arsak=r.k_krav_ret_arsak
left join PEN.t_kravhode kh on kh.kravhode_id = r.kravhode_id
left join PEN.t_k_krav_gjelder d_k on d_k.k_krav_gjelder = kh.k_krav_gjelder
left join PEN.t_sak s on s.sak_id = kh.sak_id
left join PEN.t_k_sak_t d_s on d_s.k_sak_t = s.k_sak_t
group by trunc(r.dato_opprettet), d_r.dekode, d_k.dekode, d_s.dekode
order by dato desc
