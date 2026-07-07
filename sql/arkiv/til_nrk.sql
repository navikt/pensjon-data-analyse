-- til_nrk.sql
-- engangsanalyse på spørsmål til NRK.
    select
        eo_ar,
        type_mottaker,
        count(*) as antall_tilbakekrevinger,
        sum(avviksbelop) as sum_belop_tilbakekrevinger,
        sum(avviksbelop_tfb + avviksbelop_tsb) as sum_belop_kun_bt_avvik,
        round(avg(avviksbelop), 0) as gjennomsnitt_belop,
        round(avg(avviksbelop_ut), 0) as gjennomsnitt_belop_ut,
        round(avg(avviksbelop_tfb), 0) as gjennomsnitt_belop_tfb,
        round(avg(avviksbelop_tsb), 0) as gjennomsnitt_belop_tsb,
        round(avg(avviksbelop_tfb+avviksbelop_tsb), 0) as gjennomsnitt_belop_bt,
        
        -- count(case when (inntekt_ut < 50000 and inntekt_ut > 0.4*118620) then 1 else null end) as ant_mellom_04G_og_50k,
        -- count(case when (inntekt_ut > 50000 and inntekt_ut < 118620) then 1 else null end) as ant_mellom_50k_og_1G,
        count(case when avviksbelop_tfb < 0 then 1 end) as ant_tfb_negativt_belop,
        count(case when avviksbelop_tsb < 0 then 1 end) as ant_tsb_negativt_belop,
        
        count(case when (avviksbelop_tfb < 0 or avviksbelop_tsb < 0) then 1 end) as ant_bt_totalt,
        count(case when ((avviksbelop_tfb < 0 or avviksbelop_tsb < 0) and total_belop_tfb + total_belop_tsb > 50000) then 1 end) as ant_bt_over_50k,
        count(case when ((avviksbelop_tfb < 0 or avviksbelop_tsb < 0) and total_belop_tfb + total_belop_tsb <= 50000) then 1 end) as ant_bt_under_50k,

        count(case when (inntekt_ut > 50000) then 1 else null end) as ant_inntekt_over_50k,
        count(case when (inntekt_ut <= 50000) then 1 else null end) as ant_inntekt_under_50k

        
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
            total_belop_tsb,
            case
                -- when (avviksbelop_ut = 0 and (avviksbelop_tfb < 0 or avviksbelop_tsb < 0)) then 'BT tilbakekreving (med UT avvik=0)'
                when (avviksbelop_tfb < 0 or avviksbelop_tsb < 0) then 'BT tilbakekreving'
                when (avviksbelop_tfb > 0 or avviksbelop_tsb > 0) then 'BT etterbetaling, men totalt tilbakekreving'
                else 'UT uten negativ avvik på BT'
            end as type_mottaker
        from (
            select
                kh.k_krav_gjelder,
                v.*,
                eor.*,
                extract(year from v.dato_virk_fom) as eo_ar,
                row_number() over (partition by v.sak_id, v.dato_virk_fom order by eor.dato_endret desc) as row_num
            from pen.t_eo_resultat_ut eor 
            join pen.t_vedtak v on v.eo_resultat_ut_id = eor.eo_resultat_ut_id
            left join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
            where 1=1
                --and extract(year from v.dato_virk_fom) = 2023
                and eor.k_ut_eo_resultat = 'TILBAKEKR'
                and kh.k_krav_gjelder = 'UT_VURDERING_EO' -- betyr at resultatet var tilbakekreving, og at det var fra automatisk kjøring
        )
        where row_num = 1
        and eo_ar >= 2017

    )


    group by
        eo_ar,
        rollup(
            type_mottaker
    )
    order by eo_ar desc, type_mottaker
;


