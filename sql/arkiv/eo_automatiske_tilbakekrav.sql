-- tall til media mars 2025, PL-7541

select
    eo_ar,
    case
            when (avviksbelop_ut = 0 and (avviksbelop_tfb + avviksbelop_tsb < 0)) then 'BT tilbakekreving (med UT avvik=0)'
            when (avviksbelop_tfb + avviksbelop_tsb < 0) then 'BT tilbakekreving (pluss et UT avvik)'
            when (avviksbelop_tfb + avviksbelop_tsb > 0) then 'BT etterbetaling, men totalt tilbakekreving'
            else 'UT uten negativ avvik på BT'
    end as type_mottaker,
    count(*) as antall_tilbakekrevinger,
    sum(avviksbelop) as sum_belop_tilbakekrevinger,
    sum(avviksbelop_tfb + avviksbelop_tsb) as sum_belop_kun_bt_avvik,
    round(avg(avviksbelop), 0) as gjennomsnitt_belop,
    round(avg(avviksbelop_ut), 0) as gjennomsnitt_belop_ut,
    round(avg(avviksbelop_tfb), 0) as gjennomsnitt_belop_tfb,
    round(avg(avviksbelop_tsb), 0) as gjennomsnitt_belop_tsb,
    count(case when (inntekt_ut < 97448) then 1 else null end) as ant_inntekt_under_97448,
    count(case when (inntekt_ut > 97448 and inntekt_ut < 147448) then 1 else null end) as ant_inntekt_fra_97448_til_147448,
    count(case when (inntekt_ut > 147448) then 1 else null end) as ant_inntekt_over_147448,
    count(case when avviksbelop_tfb < 0 then 1 end) as ant_tfb_negativt_belop,
    count(case when avviksbelop_tsb < 0 then 1 end) as ant_tsb_negativt_belop,
    count(case when (avviksbelop_tfb < 0 or avviksbelop_tsb < 0) then 1 end) as ant_bt_totalt
from (
    select
        eo_ar,
        sak_id,
        vedtak_id,
        kravhode_id,
        avviksbelop,
        avviksbelop_ut,
        avviksbelop_tfb,
        avviksbelop_tsb,
        inntekt_ut,
        inntekt_tsb,
        inntekt_tfb,
        total_belop,
        tidligere_belop,
        total_belop_tfb,
        total_belop_ut,
        total_belop_tsb
    from (
        select
            kh.k_krav_gjelder,
            v.*,
            eor.*,
            extract(year from v.dato_virk_fom) as eo_ar,
            -- fjerner duplikater med row_number under
            row_number() over (partition by v.sak_id, v.dato_virk_fom order by eor.dato_endret desc) as row_num
        from pen.t_eo_resultat_ut eor 
        join pen.t_vedtak v on v.eo_resultat_ut_id = eor.eo_resultat_ut_id
        left join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
        where 1=1
            and eor.k_ut_eo_resultat = 'TILBAKEKR'
            -- UT_VURDERING_EO betyr at batch-resultatet var tilbakekreving (brev med 4 ukers frist)
            and kh.k_krav_gjelder = 'UT_VURDERING_EO'
    )
    where row_num = 1
)
group by
    eo_ar,
    rollup(
        case 
            when (avviksbelop_ut = 0 and (avviksbelop_tfb + avviksbelop_tsb < 0)) then 'BT tilbakekreving (med UT avvik=0)'
            when (avviksbelop_tfb + avviksbelop_tsb < 0) then 'BT tilbakekreving (pluss et UT avvik)'
            when (avviksbelop_tfb + avviksbelop_tsb > 0) then 'BT etterbetaling, men totalt tilbakekreving'
            else 'UT uten negativ avvik på BT'
        end
    )
order by eo_ar desc
