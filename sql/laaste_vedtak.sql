select l.dato_opprettet, d_s.dekode as sakstype, d_k.dekode as kravtype, d_l.dekode as handling, le.elementtype, le.gammel_verdi, le.ny_verdi
from pen.t_laast_data_logg l
inner join pen.t_laast_data_logg_element le on le.laast_data_logg_id = l.laast_data_logg_id
inner join pen.t_k_laast_data_handling d_l on d_l.k_laast_data_handling = l.k_laast_data_handling
inner join pen.t_kravhode kh on kh.kravhode_id = l.krav_id
inner join pen.t_k_krav_gjelder d_k on d_k.k_krav_gjelder = kh.k_krav_gjelder
inner join pen.t_sak s on s.sak_id = l.sak_id
inner join pen.t_k_sak_t d_s on d_s.k_sak_t = s.k_sak_t