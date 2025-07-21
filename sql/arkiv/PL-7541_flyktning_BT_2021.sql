select count(*) from (

with saker_mulig_feil as (
    select
        sak_id,
        min(coalesce(dato_krav_fremsatt, DATE '2020-12-31')) as forste_dato_krav_fremsatt,
        max(dato_krav_fremsatt) as siste_dato_virk
    from pen.t_forst_virk_dato
    where
        sak_id in (
            select sak_id -- count = 4849 i Q1
            from pen.t_vedtak v
            inner join pen.t_person_grunnlag pg 
                on v.kravhode_id = pg.kravhode_id
                and pg.flyktning = 1
                and pg.person_id = v.person_id
            where
                k_sak_t = 'UFOREP'
                and dato_lopende_fom is not null
                and dato_lopende_tom is null
        )
        and k_kravlinje_t = 'BT'
    group by sak_id
)
select
    sak_id,
    forste_dato_krav_fremsatt,
    siste_dato_krav_fremsatt
from saker_mulig_feil
where
    forste_dato_krav_fremsatt < DATE '2021-01-01'
    and siste_dato_krav_fremsatt >= DATE '2021-01-01'
)