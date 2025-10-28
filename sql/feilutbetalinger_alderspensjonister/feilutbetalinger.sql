-- feilutbetalinger
-- finner antall måneder med feilutbetalinger ved å telle tilbakekrevinger
-- ser på diff mellom fom og tom på tilbakekrevingskravet etter dødsfallet
-- skjuler alle saker med mindre enn 10 årlige dødsfallopphør

with

vedtak as (
    select
        p.dato_dod,
        to_char(p.dato_dod, 'YYYY') as dod_ar,
        land.land_3_tegn as bosatt, -- evt land.dekode
        v_opph.dato_vedtak as opph_dato_vedtak,
        v_opph.dato_virk_fom as opph_dato_virk_fom,
        v_lop.dato_lopende_tom as lop_dato_lopende_tom,
        v_tilb.dato_virk_fom as tilb_dato_virk_fom,
        v_tilb.dato_virk_tom as tilb_dato_virk_tom,
        v_opph.sak_id,
        v_opph.vedtak_id as opph_vedtak_id,
        v_lop.vedtak_id as lop_vedtak_id,
        v_tilb.vedtak_id as tilb_vedtak_id
    from pen.t_vedtak v_opph
    left join pen.t_person p
        on
            p.person_id = v_opph.person_id
    left join pen.t_vedtak v_lop on v_opph.basert_pa_vedtak = v_lop.vedtak_id
    left join pen.t_vedtak v_tilb
        on
            v_tilb.sak_id = v_opph.sak_id
            and v_tilb.k_vedtak_t = 'TILBAKEKR'
            and v_tilb.k_sak_t = 'ALDER'
            and trunc(v_tilb.dato_virk_fom) >= p.dato_dod
    left join pen.t_k_land_3_tegn land on land.k_land_3_tegn_id = coalesce(p.bostedsland, 161)
    where
        1 = 1
        and p.dato_dod is not null
        and p.dato_dod <= v_opph.dato_vedtak -- dødsfallet må ha skjedd før vedtaket. dette fjerner 12 vedtak fra 2024-2025
        and p.dato_dod >= to_date('01.01.2012', 'DD.MM.YYYY') -- fjerner 2009 og 2010
        and v_opph.k_sak_t = 'ALDER'
        and v_opph.k_vedtak_t = 'OPPHOR'
        and v_opph.basert_pa_vedtak in (select vedtak_id from pen.t_vedtak where dato_lopende_tom is not null) --noqa
        -- filteret over fjerner saker som aldri har hatt et løpende vedtak, feks planlagte pensjoner
),

finner_antall_mnd_feil as (
    select
        -- finner antall måneder tilbakekrevd, som tilsvarer antall mnd feil
        case
            when tilb_vedtak_id is not null and (tilb_dato_virk_tom + 1 > tilb_dato_virk_fom) -- fjerner 2 saker
                then months_between(tilb_dato_virk_tom + 1, tilb_dato_virk_fom)
        end as mnd_tilbakekrevd, -- ser ut som noen gamle tilbakekrevinger kun har virk fom, ikke virk tom ...
        case when tilb_vedtak_id is not null then 1 else 0 end as ,
        dod_ar,
        bosatt
    from vedtak
),

aggregering as (
    select
        count(*) as antall_saker_totalt,
        sum(mnd_tilbakekrevd) as sum_tilbakekreving,
        sum(tilbakekrevd_flagg) as antall_saker_med_tilbakekreving,
        dod_ar,
        bosatt
    from finner_antall_mnd_feil
    group by
        dod_ar,
        bosatt
    having count(*) > 10
    order by
        dod_ar desc,
        bosatt desc
),

final as (
    select
        dod_ar,
        bosatt,
        antall_saker_totalt,
        antall_saker_med_tilbakekreving,
        sum_tilbakekreving
    from aggregering
)

select * from final
order by
    dod_ar asc,
    antall_saker_totalt desc,
    bosatt desc
