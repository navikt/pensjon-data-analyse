-- brukt i vedtakstyper.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.vedtak.vedtakstyper
-- gir ut antall vedtak per vedtakstype per måned

select
    count(*) as antall_vedtak,
    extract (year from dato_vedtak) as ar,
    extract (month from dato_vedtak) as maned,
    concat(
        concat(extract(year from dato_vedtak), '-'),
        lpad(extract(month from dato_vedtak), 2, '0')
    ) as armaned,
    k_vedtak_t as vedtakstype -- FORGANG eller AVSL. Evt alle uten filteret under
from pen.t_vedtak
where 1=1
    and k_sak_t = 'UFOREP'
    and k_vedtak_s = 'IVERKS'
    and extract(year from dato_vedtak) > 2019
group by
    extract (year from dato_vedtak),
    extract (month from dato_vedtak),
    concat(
        concat(extract(year from dato_vedtak), '-'),
        lpad(extract(month from dato_vedtak), 2, '0')
    ),
    k_vedtak_t

order by 
    extract (year from dato_vedtak) desc,
    extract (month from dato_vedtak) desc,
    count(*) desc
