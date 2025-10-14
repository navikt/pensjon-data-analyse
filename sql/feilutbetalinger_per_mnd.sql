-- feilutbetalinger_per_mnd
-- finner antall måneder med feilutbetalinger for saker
-- ser på diff mellom dato_vedtak og dato_virk_fom i vedtak om opphør
-- feilutbetalinger_per_mnd
-- finner antall måneder med feilutbetalinger for saker

with

vedtak as (
    select
        p.dato_dod,
        to_char(p.dato_dod, 'YYYY') as dod_ar,
        case
            when (p.bostedsland = 161 or p.bostedsland is null) then 'nor'
            when p.bostedsland in (60, 72, 202) then 'dan/fin/sve' -- 202 er Sverige, 72 er Finland, 60 er Danmark
            when p.bostedsland = 173 then 'polen' -- Polen, bruker EESSI som et slags unntak
            when p.bostedsland = 57 then 'tyskland'-- Tyskland. Evt se på hele EU?
            else 'utl'
        end as bosatt,
        coalesce(v_lop.dato_stoppet, v_opph.dato_vedtak) as dato_utbetaling_stopp, -- vedtak som stoppes uten formell dødsmelding
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
),

finner_antall_mnd_feil_med_buffer as (
    select
        case
            -- ingen feilutbetalinger
            when trunc(dato_dod, 'MM') = trunc(dato_utbetaling_stopp, 'MM')
                then 0
            -- hvis stopp er før den 12. i en mnd regner vi fra forrige mnd
            when extract(day from dato_utbetaling_stopp) < 12
                then months_between(trunc(dato_utbetaling_stopp, 'MM'), trunc(dato_dod, 'MM')) - 1
            -- hvis stopp er etter den 12. i en mnd regner vi fra inneværende mnd
            else
                months_between(trunc(dato_utbetaling_stopp, 'MM'), trunc(dato_dod, 'MM'))
        end as mnd_feilutbetaling,
        case
            when tilb_vedtak_id is not null
                then months_between(tilb_dato_virk_tom + 1, tilb_dato_virk_fom)
        end as mnd_tilbakekrevd, -- ser ut som noen gamle tilbakekrevinger kun har virk fom, ikke virk tom ...
        dato_dod,
        dod_ar,
        bosatt,
        lop_dato_stoppet,
        opph_dato_vedtak,
        opph_dato_virk_fom,
        lop_dato_lopende_tom,
        tilb_dato_virk_fom,
        tilb_dato_virk_tom,
        opph_vedtak_id,
        lop_vedtak_id,
        tilb_vedtak_id,
        sak_id
    from vedtak
),

finner_feil_etter_tilbakekreving as (
    select
        mnd_feilutbetaling,
        mnd_tilbakekrevd,
        mnd_feilutbetaling - mnd_tilbakekrevd as mnd_feilutbetaling_etter_tilbakekreving,
        dato_dod,
        dod_ar,
        bosatt,
        lop_dato_stoppet,
        opph_dato_vedtak,
        opph_dato_virk_fom,
        lop_dato_lopende_tom,
        tilb_dato_virk_fom,
        tilb_dato_virk_tom,
        opph_vedtak_id,
        lop_vedtak_id,
        tilb_vedtak_id,
        sak_id
    from finner_antall_mnd_feil_med_buffer
),

aggregering as (
    select
        sum(mnd_feilutbetaling) as sum_feilutbetaling,
        sum(mnd_tilbakekrevd) as sum_tilbakekreving,
        sum(mnd_feilutbetaling_etter_tilbakekreving) as sum_feil_etter_tilbakekreving,
        count(*) as antall_saker_totalt,
        count(case when tilb_vedtak_id is not null then 1 end) as antall_saker_med_tilbakekreving,
        count(case when mnd_feilutbetaling = 0 then 1 end) as antall_saker_uten_feil,
        count(case when mnd_feilutbetaling = 1 then 1 end) as antall_saker_med_1_feil,
        count(case when mnd_feilutbetaling > 1 then 1 end) as antall_saker_med_flere_feil,
        sum(case when mnd_feilutbetaling > 1 then mnd_feilutbetaling end) as sum_i_saker_med_flere_feil,
        dod_ar,
        bosatt
    from finner_feil_etter_tilbakekreving
    group by
        dod_ar,
        bosatt
    order by
        dod_ar desc,
        bosatt desc
),

final as (
    select
        dod_ar,
        bosatt,
        antall_saker_totalt,
        sum_feilutbetaling,
        sum_tilbakekreving,
        sum_feil_etter_tilbakekreving,
        sum_i_saker_med_flere_feil,
        antall_saker_uten_feil,
        antall_saker_med_1_feil,
        antall_saker_med_flere_feil,
        antall_saker_med_tilbakekreving
    from aggregering
)

select * from final
order by
    dod_ar asc,
    bosatt desc
