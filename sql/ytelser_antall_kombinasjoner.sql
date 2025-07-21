-- infoskjerm_ytelser.py, og så fra BQ til infoskjerm-i-A6-plott.qmd
-- wendelboe-prod-801c.infoskjerm.ytelser_antall_kombinasjoner

-- viser antall kombinasjoner av ytelsene i Pesys ved kjøretidspunkt, og skjuler alle antall under 10

select 
    count(*) as antall,
    afp,
    alder,
    ufore,
    krigsp,
    gam_yrk,
    afp_privat,
    gjenlevende
from (
    select
        --p.person_id,
        floor(months_between(sysdate, p.dato_fodsel) / 12) as mottaker_alder,
        max(case when v.k_sak_t = 'UFOREP' then 1 else 0 end) as ufore,
        max(case when v.k_sak_t = 'GJENLEV' then 1 else 0 end) as gjenlevende,
        max(case when v.k_sak_t = 'AFP' then 1 else 0 end) as afp,
        max(case when v.k_sak_t = 'AFP_PRIVAT' then 1 else 0 end) as afp_privat,
        max(case when v.k_sak_t = 'ALDER' then 1 else 0 end) as alder,
        max(case when v.k_sak_t = 'GAM_YRK' then 1 else 0 end) as gam_yrk,
        max(case when v.k_sak_t = 'KRIGSP' then 1 else 0 end) as krigsp
    from pen.t_vedtak v
    inner join pen.t_person p on p.person_id = v.person_id
    where 1=1
        and v.dato_lopende_fom < sysdate
        and (v.dato_lopende_tom is null or v.dato_lopende_tom >= sysdate)
        and v.k_sak_t in ('UFOREP', 'GJENLEV', 'AFP', 'AFP_PRIVAT', 'ALDER', 'GAM_YRK', 'KRIGSP')
    group by
        p.person_id,
        floor(months_between(sysdate, p.dato_fodsel) / 12)
    )
group by
    afp,
    alder,
    ufore,
    krigsp,
    gam_yrk,
    afp_privat,
    gjenlevende
having count(*) > 9
order by count(*)
