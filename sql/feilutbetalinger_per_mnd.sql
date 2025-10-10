-- feilutbetalinger_per_mnd
-- finner antall måneder med feilutbetalinger for saker
-- ser på diff mellom dato_vedtak og dato_virk_fom i vedtak om opphør
-- feilutbetalinger_per_mnd
-- finner antall måneder med feilutbetalinger for saker

with

vedtak as (
    select
        p.dato_dod,
        case
            when (p.bostedsland = 161 or p.bostedsland is null) then 'nor'
            when p.bostedsland in (60, 72, 202) then 'dan/fin/sve' -- 202 er Sverige, 72 er Finland, 60 er Danmark
            when p.bostedsland = 173 then 'polen' -- Polen, bruker EESSI som et slags unntak
            when p.bostedsland = 57 then 'tyskland'-- Tyskland. Evt se på hele EU?
            else 'utl'
        end as bosatt,
        v.sak_id,
        v.vedtak_id,
        v.dato_vedtak,
        v.dato_virk_fom
        -- v.basert_pa_vedtak, -- vedtaket som var løpende
    from pen.t_vedtak v
    inner join pen.t_person p on p.person_id = v.person_id
    where
        1 = 1
        and v.k_sak_t = 'ALDER'
        and v.k_vedtak_t = 'OPPHOR'
        -- and v.ansv_saksbh = 'DødsfallBehandling'
        and v.basert_pa_vedtak in (select vedtak_id from pen.t_vedtak where dato_lopende_tom is not null) --noqa
        -- filteret over fjerner saker som aldri har hatt et løpende vedtak, feks planlagte pensjoner
        and p.dato_dod <= v.dato_vedtak -- dødsfallet må ha skjedd før vedtaket. dette fjerner 12 vedtak fra 2024-2025
        and p.dato_dod is not null
        and p.dato_dod >= to_date('01.01.2011', 'DD.MM.YYYY') -- fjerner 2009 og 2010
        -- and v.dato_vedtak >= to_date('01.09.2025', 'DD.MM.YYYY')
),

finner_antall_mnd as (
    select
        months_between(trunc(dato_vedtak, 'MM'), dato_virk_fom) + 1 as mnd_feilutbet,
        bosatt,
        dato_dod,
        to_char(dato_dod, 'YYYY') as dod_ar,
        sak_id,
        vedtak_id,
        dato_vedtak,
        dato_virk_fom
    from vedtak
),

setter_10_dager_margin as (
    select
        -- mnd_feilutbet as mnd_feilutbet_uten_margin,
        case
            when (mnd_feilutbet = 1 and extract(day from dato_vedtak) <= 10) then mnd_feilutbet - 1
            else mnd_feilutbet
        end as mnd_feilutbet,
        bosatt,
        dato_dod,
        dod_ar,
        sak_id,
        vedtak_id,
        dato_vedtak,
        dato_virk_fom
    from finner_antall_mnd
),

finner_eventuell_tilbakekreving as (
    select
        setter_10_dager_margin.*,
        v.vedtak_id as tilbakekreving_vedtak_id,
        v.k_sak_t as tilbakekreving_k_sak_t,
        v.dato_virk_fom as tilbakekreving_dato_virk_fom,
        v.dato_virk_tom as tilbakekreving_dato_virk_tom,
        months_between(v.dato_virk_tom + 1, v.dato_virk_fom) as mnd_tilbakekrevd
    from setter_10_dager_margin
    left join pen.t_vedtak v
        on
            v.sak_id = setter_10_dager_margin.sak_id
            and v.k_vedtak_t = 'TILBAKEKR'
            and v.k_vedtak_s = 'IVERKS'
            and v.k_sak_t = 'ALDER'
            and trunc(v.dato_virk_fom) >= setter_10_dager_margin.dato_dod
)

select
    mnd_feilutbet,
    -- mnd_feilutbet_uten_margin,
    mnd_tilbakekrevd,
    -- tilbakekreving_dato_virk_fom,
    -- tilbakekreving_dato_virk_tom,
    bosatt,
    dod_ar,
    dato_dod,
    dato_virk_fom,
    dato_vedtak
    -- sak_id,
    -- vedtak_id,
    -- tilbakekreving_vedtak_id,
from finner_eventuell_tilbakekreving
order by
    mnd_feilutbet desc,
    bosatt desc
