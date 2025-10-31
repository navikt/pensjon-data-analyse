-- feilutbetalinger_med_avtaleland

-- finner antall måneder med feilutbetalinger ved å telle tilbakekrevinger
-- kobler til trygdeavtaler for å finne avtaleland
-- viser også utlandstilknytning og om trygdeavtalen er vurdert fra kravhodet

with

vedtak as (
    select
        p.dato_dod,
        kh.k_regelverk_t,
        kh.k_utlandstilknytning,
        substr(kh.vur_trygdeav, 0, 1) as vur_trygdeav, -- er 0/1, men noen har 0 + 17 mellomrom –.–
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
        v_tilb.vedtak_id as tilb_vedtak_id,
        kh.kravhode_id as kh_kravhode_id,
        pg.kravhode_id as pg_kravhode_id,
        v_lop.kravhode_id as v_lop_kravhode_id,
        -- ta.k_avtale_t,
        dekode_avtale.dekode as avtaleland,
        case
            when v_tilb.vedtak_id is not null and (v_tilb.dato_virk_tom + 1 > v_tilb.dato_virk_fom)
                then months_between(v_tilb.dato_virk_tom + 1, v_tilb.dato_virk_fom)
        end as mnd_tilbakekrevd, -- ser ut som noen gamle tilbakekrevinger kun har virk fom, ikke virk tom ...
        case when v_tilb.vedtak_id is not null then 1 else 0 end as tilbakekrevd_flagg
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
    -- finner trygdeavtaler
    left join pen.t_kravhode kh on kh.kravhode_id = v_lop.kravhode_id
    left join pen.t_person_grunnlag pg on pg.kravhode_id = kh.kravhode_id and pg.person_id = p.person_id
    left join pen.t_trygdeavtale ta on ta.person_grunnlag_id = pg.person_grunnlag_id
    left join pen.t_k_avtale_t dekode_avtale on dekode_avtale.k_avtale_t = ta.k_avtale_t
    -- alternativt var pg.person_grunnlag_id til pen.t_trygdetid og videre til pen.t_tt_utl_trgd_avt for å finne k_avtaleland, men ga få treff
    where
        1 = 1
        and p.dato_dod is not null
        and p.dato_dod <= v_opph.dato_vedtak -- dødsfallet må ha skjedd før vedtaket. dette fjerner 12 vedtak fra 2024-2025
        and p.dato_dod >= to_date('01.01.2012', 'DD.MM.YYYY') -- fjerner 2009 og 2010
        and v_opph.k_sak_t = 'ALDER'
        and v_opph.k_vedtak_t = 'OPPHOR'
        and v_opph.basert_pa_vedtak in (select vedtak_id from pen.t_vedtak where dato_lopende_tom is not null) --noqa
)

select
    dod_ar,
    bosatt,
    avtaleland,
    k_utlandstilknytning as utl_tilknytning,
    vur_trygdeav,
    sum(mnd_tilbakekrevd) as sum_tilbakekreving,
    sum(tilbakekrevd_flagg) as antall_saker_med_tilbakekreving,
    count(distinct sak_id) as antall_saker -- joinen mot trygdeavtale gir noen få duplikater
from vedtak
group by
    dod_ar,
    bosatt,
    avtaleland,
    k_utlandstilknytning,
    vur_trygdeav
having count(distinct sak_id) >= 4
order by
    dod_ar desc,
    antall_saker desc
