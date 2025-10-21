-- eo_oversikt.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.etteroppgjoret.eo_varselbrev

-- ser på de som får 4-ukers varselbrev om tilbakekreving, men kun via kravhode
-- altså de som skal få/får varselbrev om etteroppgjør
-- tror dette kun er de som kommer fra automatiske kjøringer, men ikke helt sikker

select
    extract(year from dato_onsket_virk) as eo_ar,
    case
        when dato_mottatt_krav in ( -- hovedkjøringene
            to_date('21/10/25', 'DD/MM/YY'),
            to_date('22/10/24', 'DD/MM/YY'),
            to_date('24/10/23', 'DD/MM/YY'),
            to_date('18/10/22', 'DD/MM/YY'),
            to_date('19/10/21', 'DD/MM/YY'),
            to_date('21/10/20', 'DD/MM/YY')
        ) then 'Hovedkjøring'
        else 'Ikke hovedkjøring'
    end as hovedkjoring,
    trunc(dato_opprettet) as dato_opprettet,
    count(*) as antall_krav_ut_vurdering_eo
from pen.t_kravhode
where
    1 = 1
    and k_krav_gjelder = 'UT_VURDERING_EO' -- kun ett varselbrev per sakid per år
    and extract(year from dato_onsket_virk) >= 2019
group by
    extract(year from dato_onsket_virk),
    case
        when dato_mottatt_krav in ( -- hovedkjøringene
            to_date('21/10/25', 'DD/MM/YY'),
            to_date('22/10/24', 'DD/MM/YY'),
            to_date('24/10/23', 'DD/MM/YY'),
            to_date('18/10/22', 'DD/MM/YY'),
            to_date('19/10/21', 'DD/MM/YY'),
            to_date('21/10/20', 'DD/MM/YY')
        ) then 'Hovedkjøring'
        else 'Ikke hovedkjøring'
    end,
    trunc(dato_opprettet)
order by
    eo_ar desc,
    dato_opprettet desc
