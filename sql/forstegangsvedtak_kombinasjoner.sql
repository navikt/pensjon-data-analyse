
select
    ar_vedtak as ar,
    kravtype,
    k_krav_arsak_t as krav_arsak,
    k_vedtak_t,
    inngangsvilkar_oppfylt,
    hensiktsmessig_beh,
    hens_arbrett_tiltak,
    sykdom_skade_lyte,
    alder,
    ungufor,
    nedsatt_innt_evne,
    yrkesskade,
    -- nie_min_halv,
    forut_medl,
    fortsatt_medl,
    medl_avtale,
    eksport_avtale,
    bt_innv,
    bt_avsl,
    count(*) as antall
from (

select
    /*+ NO_QUERY_TRANSFORMATION */
    extract(year from v.dato_vedtak) as ar_vedtak,
    kg.dekode as kravtype,
    case when (
        vi_hensi.k_vilkar_oppfylt_ut_t = 'OPPFYLT'
        and vi_hens.k_vilkar_oppfylt_ut_t  = 'OPPFYLT'
        and vi_sykdo.k_vilkar_oppfylt_ut_t  = 'OPPFYLT'
        )
        then 'ja'
        else 'nei'
    end as inngangsvilkar_oppfylt,
    v.k_vedtak_t,
    ka.k_krav_arsak_t,
    --vi_HENSI.k_vilkar_oppfylt_ut_t || " - " || vi_HENSI.k_vilkar_stdbegr as HENSIKTSMESSIG_BEH,
    vi_HENSI.k_vilkar_oppfylt_ut_t as HENSIKTSMESSIG_BEH,
    vi_HENS.k_vilkar_oppfylt_ut_t as HENS_ARBRETT_TILTAK,
    vi_SYKDO.k_vilkar_oppfylt_ut_t as SYKDOM_SKADE_LYTE,
    vi_ALDER.k_vilkar_oppfylt_ut_t as ALDER,
    vi_GARAN.k_vilkar_oppfylt_ut_t as UNGUFOR,
    vi_NEDSA.k_vilkar_oppfylt_ut_t as NEDSATT_INNT_EVNE,
    vi_YRKES.k_vilkar_oppfylt_ut_t as YRKESSKADE,
    -- vi_NIE_M.k_vilkar_oppfylt_ut_t as NIE_MIN_HALV,
    vi_FORUT.k_vilkar_oppfylt_ut_t as FORUT_MEDL,
    vi_FORTS.k_vilkar_oppfylt_ut_t as FORTSATT_MEDL,
    vi_M_F_U.k_vilkar_oppfylt_ut_t as medl_avtale,
    vi_R_T_E.k_vilkar_oppfylt_ut_t as eksport_avtale,
    (select 'innvilget'
        from pen.t_vilkar_vedtak vvbt
        inner join pen.t_kravlinje klbt on klbt.kravlinje_id = vvbt.kravlinje_id and klbt.k_land_3_tegn_id = 161
        where vvbt.vedtak_id = vv.vedtak_id
        and vvbt.k_kravlinje_t = 'BT'
        and vvbt.dato_virk_fom = v.dato_virk_fom
        and vvbt.k_vilkar_resul_t = 'INNV'
        fetch first 1 rows only
    ) as bt_innv,
        (select 'avslag'
        from pen.t_vilkar_vedtak vvbt
        inner join pen.t_kravlinje klbt on klbt.kravlinje_id = vvbt.kravlinje_id and klbt.k_land_3_tegn_id = 161
        where vvbt.vedtak_id = vv.vedtak_id
        and vvbt.k_kravlinje_t = 'BT'
        and vvbt.dato_virk_fom = v.dato_virk_fom
        and vvbt.k_vilkar_resul_t = 'AVSL'
        fetch first 1 rows only
    ) as bt_avsl
from pen.t_vedtak v
inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
inner join pen.t_k_krav_gjelder kg on kg.k_krav_gjelder = kh.k_krav_gjelder
inner join pen.t_vilkar_vedtak vv on vv.vedtak_id = v.vedtak_id
                                 and vv.dato_virk_fom = v.dato_virk_fom
                                 and vv.k_kravlinje_t = 'UT'
inner join pen.t_kravlinje kl on kl.kravlinje_id = vv.kravlinje_id and kl.k_land_3_tegn_id = 161
inner join pen.t_krav_arsak ka on ka.kravhode_id = kh.kravhode_id

--left outer join pen.t_vilkar vi on vi.vilkar_vedtak_id = vv.vilkar_vedtak_id
left outer join pen.t_vilkar vi_HENSI on vi_HENSI.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_HENSI.k_vilkar_t = 'HENSIKTSMESSIG_BEH'
left outer join pen.t_vilkar vi_HENS  on vi_HENS.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_HENS.k_vilkar_t = 'HENS_ARBRETT_TILTAK'
left outer join pen.t_vilkar vi_SYKDO on vi_SYKDO.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_SYKDO.k_vilkar_t = 'SYKDOM_SKADE_LYTE'
left outer join pen.t_vilkar vi_ALDER on vi_ALDER.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_ALDER.k_vilkar_t = 'ALDER'
left outer join pen.t_vilkar vi_GARAN on vi_GARAN.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_GARAN.k_vilkar_t = 'GARANTERTUNGUFOR'
left outer join pen.t_vilkar vi_NEDSA on vi_NEDSA.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_NEDSA.k_vilkar_t = 'NEDSATT_INNT_EVNE'
left outer join pen.t_vilkar vi_YRKES on vi_YRKES.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_YRKES.k_vilkar_t = 'YRKESSKADE'
-- left outer join pen.t_vilkar vi_NIE_M on vi_NIE_M.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_NIE_M.k_vilkar_t = 'NIE_MIN_HALV'
left outer join pen.t_vilkar vi_FORUT on vi_FORUT.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_FORUT.k_vilkar_t = 'FORUT_MEDL'
left outer join pen.t_vilkar vi_FORTS on vi_FORTS.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_FORTS.k_vilkar_t = 'FORTSATT_MEDL'
left outer join pen.t_vilkar vi_M_F_U on vi_M_F_U.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_M_F_U.k_vilkar_t = 'M_F_UT_ETR_TRYAVTAL'
left outer join pen.t_vilkar vi_R_T_E on vi_R_T_E.vilkar_vedtak_id = vv.vilkar_vedtak_id and vi_R_T_E.k_vilkar_t = 'R_T_E_ETR_TRYAVTAL'

where
    v.k_sak_t = 'UFOREP'
    and v.k_vedtak_t in ('FORGANG', 'AVSL')
    and v.dato_vedtak between to_date('01.01.2024','DD.MM.YYYY') and to_date('31.12.2024','DD.MM.YYYY')
    and kh.k_krav_gjelder in ('F_BH_BO_UTL', 'F_BH_MED_UTL', 'FORSTEG_BH')
)

group by
    ar_vedtak,
    kravtype,
    k_krav_arsak_t,
    k_vedtak_t,
    inngangsvilkar_oppfylt,
    hensiktsmessig_beh,
    hens_arbrett_tiltak,
    sykdom_skade_lyte,
    alder,
    ungufor,
    nedsatt_innt_evne,
    yrkesskade,
    -- nie_min_halv,
    forut_medl,
    fortsatt_medl,
    medl_avtale,
    eksport_avtale,
    bt_innv,
    bt_avsl
having count(*) > 9
order by count(*) desc
