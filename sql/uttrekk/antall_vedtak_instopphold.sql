-- forespørsel fra Stine Otterbekk
-- en advokat spør hvor mange vedtak som fattes årlig med hjemmel i folketrygdloven § 19-21
-- (reduksjon i alderspensjon grunnet institusjonsopphold)
-- https://nav-it.slack.com/archives/C09R8NZ4MQQ/p1764067491829209

-- svar gitt i tråden på slack fra denne SQLen

select
    count(*) as antall_vedtak,
    k_vedtak_s,
    k_krav_arsak_t,
    extract(year from dato_vedtak) as vedtak_ar
from (
    select
        v.sak_id,
        v.vedtak_id,
        v.kravhode_id,
        v.k_sak_t,
        v.k_vedtak_t,
        v.k_vedtak_s,
        v.dato_vedtak,
        ka.k_krav_arsak_t
    from pen.t_vedtak v
    left join pen.t_krav_arsak ka on v.kravhode_id = ka.kravhode_id
    where
        1 = 1
        and v.k_sak_t = 'ALDER'
        and v.k_vedtak_s = 'IVERKS'
        and v.dato_vedtak >= '01.01.2011'
        and ka.k_krav_arsak_t = 'INSTOPPHOLD'
)
group by
    k_vedtak_s,
    k_krav_arsak_t,
    extract(year from dato_vedtak)
order by vedtak_ar desc
