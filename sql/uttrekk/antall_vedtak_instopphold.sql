-- forespørsel fra Stine Otterbekk
-- en advokat spør hvor mange vedtak som fattes årlig med hjemmel i folketrygdloven § 19-21
-- (reduksjon i alderspensjon grunnet institusjonsopphold)
-- https://nav-it.slack.com/archives/C09R8NZ4MQQ/p1764067491829209

-- svar gitt i tråden på slack fra denne SQLen


-- gir ut første vedtaket der en reduksjonsperiode på inst er registrert
-- vi fjerner ikke tilfeller der reduksjonsperioden evt er fjerner tilbake i tid i en revudering

select
    count(*) as antall_vedtak,
    institusjonsopphold,
    extract(year from dato_vedtak) as vedtak_ar
from (
    select
        v.sak_id,
        dekode_just.dekode as institusjonsopphold,
        v.dato_virk_fom,
        iop.dato_fom,
        iop.dato_tom,
        v.dato_vedtak,
        v.k_vedtak_t,
        kh.k_krav_gjelder,
        ka.k_krav_arsak_t,
        rank() over (partition by v.sak_id, iop.dato_fom order by v.dato_opprettet asc) as inst_rank
    from pen.t_vedtak v
    inner join pen.t_kravhode kh on v.kravhode_id = kh.kravhode_id
    inner join pen.t_krav_arsak ka on v.kravhode_id = ka.kravhode_id
    inner join pen.t_person_grunnlag pg
        on
            v.kravhode_id = pg.kravhode_id
            and v.person_id = pg.person_id
    inner join pen.t_inst_op_re_pe iop
        on
            pg.person_grunnlag_id = iop.person_grunnlag_id
            and iop.k_just_periode in ('REDUKSJON_FO', 'REDUKSJON_HS', 'REDUKSJON')
    left join pen.t_k_just_periode dekode_just on iop.k_just_periode = dekode_just.k_just_periode
    inner join pen.t_k_vedtak_t kvt on kvt.k_vedtak_t = v.k_vedtak_t and kvt.kan_lope = '1'

    where
        1 = 1
        and v.k_sak_t = 'ALDER'
        and v.k_vedtak_s = 'IVERKS'
        and (iop.dato_tom > v.dato_virk_fom or iop.dato_tom is null)
    order by v.sak_id desc, v.kravhode_id desc, v.vedtak_id desc
)

where inst_rank = 1
group by
    institusjonsopphold,
    extract(year from dato_vedtak)
order by
    extract(year from dato_vedtak) desc,
    institusjonsopphold desc
