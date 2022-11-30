select dim_sak_t.dekode sakstype, dim_krav_gjelder.dekode kravtype, dim_krav_s.dekode kravstatus, count(*) antall
from pen.t_kravhode kh
inner join pen.t_k_krav_s dim_krav_s on dim_krav_s.k_krav_s = kh.k_krav_s
inner join pen.t_k_krav_gjelder dim_krav_gjelder on dim_krav_gjelder.k_krav_gjelder = kh.k_krav_gjelder
inner join pen.t_sak s on s.sak_id = kh.sak_id
inner join pen.t_k_sak_t dim_sak_t on dim_sak_t.k_sak_t = s.k_sak_t
where kh.k_krav_s not like 'AVBRUTT'
and kh.k_krav_s not like 'FERDIG'
group by dim_sak_t.dekode, dim_krav_gjelder.dekode, dim_krav_s.dekode
order by antall desc