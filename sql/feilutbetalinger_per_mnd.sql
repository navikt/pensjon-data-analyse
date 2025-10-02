-- feilutbetalinger_per_mnd
-- finner antall måneder med feilutbetalinger for saker

-- 12 vedtak er 

with

vedtak as (
    select
        p.dato_dod,
        v.sak_id,
        v.vedtak_id,
        v.dato_vedtak,
        v.dato_virk_fom
        -- v.k_vedtak_t,
        -- v.dato_lopende_fom,
        -- v.dato_lopende_tom,
        -- v.basert_pa_vedtak, -- vedtaket som var løpende
    from pen.t_vedtak v
    inner join pen.t_person p on p.person_id = v.person_id
    where
        1 = 1
        and v.k_sak_t = 'ALDER'
        and v.k_vedtak_t = 'OPPHOR'
        and v.ansv_saksbh = 'DødsfallBehandling' -- todo: 'BPEN002' for før 2023??? noe skjedde i 2021/2022 med BPEN002
        and v.basert_pa_vedtak in (select vedtak_id from pen.t_vedtak where dato_lopende_tom is not null) --noqa
        -- filteret over fjerner saker som aldri har hatt et løpende vedtak, feks planlagte pensjoner
        and p.dato_dod <= v.dato_vedtak -- dødsfallet må ha skjedd før vedtaket. dette fjerner 12 vedtak fra 2024-2025
        and v.dato_vedtak >= to_date('01.01.2024', 'DD.MM.YYYY')
),

finner_antall_mnd as (
    select
        dato_dod,
        sak_id,
        vedtak_id,
        dato_vedtak,
        dato_virk_fom,
        to_char(dato_vedtak, 'YYYYMM') as vedtak_aar_mnd,
        trunc(dato_vedtak, 'MM') as dato_vedtak_mnd,
        trunc(dato_vedtak, 'YYYY') as dato_vedtak_ar,
        months_between(trunc(dato_vedtak, 'MM'), dato_virk_fom) + 1 as diff_vedtak_virk
    from vedtak
),

aggregering as (
    select
        diff_vedtak_virk,
        count(diff_vedtak_virk) as antall,
        sum(diff_vedtak_virk) as sum_mnd
    from finner_antall_mnd
    group by diff_vedtak_virk
    order by sum(diff_vedtak_virk) desc
)

select * from aggregering;
