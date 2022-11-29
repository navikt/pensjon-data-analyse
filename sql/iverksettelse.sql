select dato_tilverksett, dato_iverksatt, dato_iverksatt - dato_tilverksett dager, dekode vedtakstype
from PEN.T_VEDTAK
inner join pen.t_k_vedtak_t on pen.t_k_vedtak_t.k_vedtak_t = pen.t_vedtak.k_vedtak_t
where extract(year from dato_iverksatt) = 2022
and k_vedtak_s not like 'AVBR'