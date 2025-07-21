-- infoskjerm_ytelser.py, og så fra BQ til infoskjerm-i-A6-plott.qmd
-- wendelboe-prod-801c.infoskjerm.ytelser_per_alderskull

-- henter status nå på ytelser i PESYS per aldersgruppe
-- skjuler de små ytelsene. Skiller også på de som bor i Norge og i utlandet
-- noen personer har kombinasjon av ytelser, feks alder og uføre

with

ytelse_per_person_med_alder as (
    select
        --p.person_id,
        case when (p.bostedsland = 161 or p.bostedsland is null) then 1 else 0 end as bosatt_norge,
        floor(months_between(sysdate, p.dato_fodsel) / 12) as mottaker_alder,
        max(case when v.k_sak_t = 'UFOREP' then 1 else 0 end) as ufore,
        max(case when v.k_sak_t = 'GJENLEV' then 1 else 0 end) as gjenlevende,
        max(case when v.k_sak_t = 'AFP' then 1 else 0 end) as afp,
        max(case when v.k_sak_t = 'AFP_PRIVAT' then 1 else 0 end) as afp_privat,
        max(case when v.k_sak_t = 'ALDER' then 1 else 0 end) as alder
    from pen.t_vedtak v
    inner join pen.t_person p on p.person_id = v.person_id
    where 1=1
        and v.dato_lopende_fom < sysdate
        and (v.dato_lopende_tom is null or v.dato_lopende_tom >= sysdate)
        and v.k_sak_t in ('UFOREP', 'GJENLEV', 'AFP', 'AFP_PRIVAT', 'ALDER')
    group by
        p.person_id,
        floor(months_between(sysdate, p.dato_fodsel) / 12),
        case when (p.bostedsland = 161 or p.bostedsland is null) then 1 else 0 end
),

aggregering as (
    select
        mottaker_alder,
        count(*) as antall_personer_pesys,
        count(*) - sum(bosatt_norge) as antall_bosatt_utland,
        sum(afp) as afp,
        sum(alder) as alder,
        sum(ufore) as ufore,
        sum(afp_privat) as afp_privat, -- så si alle afp_privat har også alder
        sum(gjenlevende) as gjenlevende,
        -- kombinasjoner: alder+uføre, 
        -- de andre kopmbinasjonene gir små tall: alder+krigsp, alder+gam_yrk, alder+afp, uføre+gjenlevende
        sum(case when ufore = 1 and alder = 1 then 1 else 0 end) as ufore_og_alder,
        -- ikke-kombinasjoner
        sum(case when ufore = 1 and alder = 0 then 1 else 0 end) as kun_ufore,
        sum(case when alder = 1 and ufore = 0 and afp_privat = 0 and afp = 0 then 1 else 0 end) as kun_alder
    from ytelse_per_person_med_alder
    where mottaker_alder >= 18 and mottaker_alder <= 104
    group by mottaker_alder
),

maskering as (
    select
        mottaker_alder,
        case when antall_personer_pesys > 9 then antall_personer_pesys else null end as antall_personer_pesys,
        case when afp > 9 then afp else null end as afp,
        case when alder > 9 then alder else null end as alder,
        case when ufore > 9 then ufore else null end as ufore,
        case when afp_privat > 9 then afp_privat else null end as afp_privat,
        case when gjenlevende > 9 then gjenlevende else null end as gjenlevende,
        case when ufore_og_alder > 9 then ufore_og_alder else null end as ufore_og_alder,
        case when kun_ufore > 9 then kun_ufore else null end as kun_ufore,
        case when kun_alder > 9 then kun_alder else null end as kun_alder,
        case when antall_bosatt_utland > 9 then antall_bosatt_utland else null end as antall_bosatt_utland
    from aggregering
)

select
    mottaker_alder,
    antall_personer_pesys,
    afp,
    alder,
    ufore,
    afp_privat,
    gjenlevende,
    -- kombinasjoner
    ufore_og_alder,
    -- ikke-kombinasjoner
    kun_ufore,
    kun_alder,
    antall_bosatt_utland
from maskering
order by mottaker_alder
