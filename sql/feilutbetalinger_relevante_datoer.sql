-- feilutbetalinger_relevante_datoer
-- henter ut datoer knytter til opphørssaker ved dødsfall
-- dødsdato, datoer fra opphørsvedtak, datoer fra løpende vedtak, datoer fra evt tilbakekrevingsvedtak
select
    p.dato_dod,
    v_lop.dato_stoppet as lop_dato_stoppet,
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
        --and v_tilb.k_vedtak_s = 'IVERKS'
        and v_tilb.k_sak_t = 'ALDER'
        and trunc(v_tilb.dato_virk_fom) >= p.dato_dod
where
    1 = 1
    and p.dato_dod is not null
    and p.dato_dod <= v_opph.dato_vedtak -- dødsfallet må ha skjedd før vedtaket. dette fjerner 12 vedtak fra 2024-2025
    and p.dato_dod >= to_date('01.01.2011', 'DD.MM.YYYY') -- fjerner 2009 og 2010
    and v_opph.k_sak_t = 'ALDER'
    and v_opph.k_vedtak_t = 'OPPHOR'
    and v_opph.basert_pa_vedtak in (select vedtak_id from pen.t_vedtak where dato_lopende_tom is not null) --noqa
    -- filteret over fjerner saker som aldri har hatt et løpende vedtak, feks planlagte pensjoner
order by months_between(coalesce(v_lop.dato_stoppet, v_opph.dato_vedtak), p.dato_dod) desc
