select
    mottaker_alder,
    count(*) as antall_pesys,
    sum(bosatt_norge) as antall_norge,
    count(*) - sum(bosatt_norge) as antall_utland,
    sum(uforep) as uforep,
    sum(gjenlev) as gjenlev,
    sum(afp) as afp,
    sum(afp_privat) as afp_privat, -- alle her har også alder, basically
    sum(alder) as alder
    -- basically bare alder+afp_privat som er kombinasjon
    -- sum(uforep) + sum(gjenlev) + sum(afp) + sum(afp_privat) + sum(alder) - count(*) as antall_flere_ytelser,
    -- veldig få med krigsp og gam_yrk, så skjuler dem
    -- sum(krigsp) as krigsp,
    -- sum(gam_yrk) as gam_yrk,
from (
    select
        p.person_id,
        case when (p.bostedsland = 161 or p.bostedsland is null) then 1 else 0 end as bosatt_norge,
        2024 - extract(year from add_months(p.dato_fodsel, 1)) as mottaker_alder,
        max(case when v.k_sak_t = 'UFOREP' then 1 else 0 end) as uforep,
        max(case when v.k_sak_t = 'GJENLEV' then 1 else 0 end) as gjenlev,
        max(case when v.k_sak_t = 'AFP' then 1 else 0 end) as afp,
        max(case when v.k_sak_t = 'AFP_PRIVAT' then 1 else 0 end) as afp_privat,
        max(case when v.k_sak_t = 'ALDER' then 1 else 0 end) as alder
        -- max(case when v.k_sak_t = 'GAM_YRK' then 1 else 0 end) as gam_yrk,
        -- max(case when v.k_sak_t = 'KRIGSP' then 1 else 0 end) as krigsp,
    from pen.t_vedtak v
    inner join pen.t_person p on p.person_id = v.person_id
    where 1=1
        and v.dato_lopende_fom < '31.12.2024'
        and (v.dato_lopende_tom is null or v.dato_lopende_tom >= '31.12.2024')
        and v.k_sak_t in ('UFOREP', 'GJENLEV', 'AFP', 'AFP_PRIVAT', 'ALDER')
    group by
        p.person_id,
        2024 - extract(year from add_months(p.dato_fodsel, 1)),
        case when (p.bostedsland = 161 or p.bostedsland is null) then 1 else 0 end
)
group by mottaker_alder
order by mottaker_alder