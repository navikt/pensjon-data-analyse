-- feilutbetalinger_dodsalder

-- viser gjennomsnittlig dødsalder for ulike land per år, for opphør av alderspensjonssaker
-- skjuler forekomster med færre enn 10 dødsfall

with

vedtak as (
    select
        p.person_id,
        p.dato_dod,
        p.dato_fodsel,
        land.dekode as bostedsland, -- evt land.land_3_tegn
        floor(months_between(p.dato_dod, p.dato_fodsel) / 12) as dodsalder,
        -- Velg den nyeste saken per person
        max(v.sak_id) as sak_id,
        max(v.vedtak_id) as vedtak_id,
        max(v.dato_vedtak) as dato_vedtak,
        max(v.dato_virk_fom) as dato_virk_fom
    from pen.t_vedtak v
    inner join pen.t_person p on p.person_id = v.person_id
    left join pen.t_k_land_3_tegn land on land.k_land_3_tegn_id = coalesce(p.bostedsland, 161)
    where
        1 = 1
        and v.k_sak_t = 'ALDER'
        and v.k_vedtak_t = 'OPPHOR'
        and p.dato_dod is not null
    group by
        p.person_id,
        p.dato_dod,
        p.dato_fodsel,
        land.dekode
),

-- gjennomsnitt_dodsalder_per_ar as (
--     -- alternativ visning som viser per år
--     select
--         avg(dodsalder) as gjennomsnitt_dodsalder,
--         extract(year from dato_dod) as dodsar,
--         bostedsland
--     from vedtak
--     group by extract(year from dato_dod), bostedsland
-- ),

gjennomsnitt_dodsalder as (
    select
        avg(dodsalder) as gjennomsnitt_dodsalder,
        count(*) as antall_personer,
        bostedsland
    from vedtak
    group by bostedsland
    having count(*) > 10
)

-- select * from gjennomsnitt_dodsalder_per_ar order by dodsar desc, bostedsland, gjennomsnitt_dodsalder desc;
select * from gjennomsnitt_dodsalder
order by gjennomsnitt_dodsalder desc;
