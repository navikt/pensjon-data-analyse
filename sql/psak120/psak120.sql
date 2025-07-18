SELECT
    DIM_TID_PERIODE.AAR_MAANED,
    DIM_TID_PERIODE.AAR,
    DT_P.DIM_OPPRETTET_AV_TYPE.OPPRETTET_AV_TYPE_KODE,
    DT_P.FAK_RAP_KRAVH_FERDIGBH_NAV.FRA_SELVBETJ_LOSNING_FLAGG,
    SUM(
        DT_P.FAK_RAP_KRAVH_FERDIGBH_NAV.ANALYSE_KRAVHODE_ANTALL
    )
FROM
    DT_P.DIM_TID DIM_TID_PERIODE
    INNER JOIN DT_P.FAK_RAP_KRAVH_FERDIGBH_NAV ON (
        DT_P.FAK_RAP_KRAVH_FERDIGBH_NAV.FK_DIM_TID_PERIODE = DIM_TID_PERIODE.PK_DIM_TID
        AND DT_P.FAK_RAP_KRAVH_FERDIGBH_NAV.FK_DIM_VERSJON IN (
            select
                pk_dim_versjon
            from
                dt_p.dim_versjon
            where
                offentlig_flagg = 1
        )
    )
    INNER JOIN DT_P.DIM_OPPRETTET_AV_TYPE ON (
        DT_P.FAK_RAP_KRAVH_FERDIGBH_NAV.FK_DIM_OPPRETTET_AV_TYPE = DT_P.DIM_OPPRETTET_AV_TYPE.PK_DIM_OPPRETTET_AV_TYPE
    )
    INNER JOIN DT_P.DIM_KRAVTYPE ON (
        DT_P.DIM_KRAVTYPE.PK_DIM_KRAVTYPE = DT_P.FAK_RAP_KRAVH_FERDIGBH_NAV.FK_DIM_KRAVTYPE
    )
WHERE
    (
        (DT_P.DIM_KRAVTYPE.KRAVTYPE_KODE) IN (
            'F_BH_BO_UTL',
            'F_BH_KUN_UTL',
            'F_BH_MED_UTL',
            'FORSTEG_BH',
            'INNT_E'
        )
        AND DIM_TID_PERIODE.AAR = 2022
    )
GROUP BY
    DIM_TID_PERIODE.AAR_MAANED,
    DIM_TID_PERIODE.AAR,
    DT_P.DIM_OPPRETTET_AV_TYPE.OPPRETTET_AV_TYPE_KODE,
    DT_P.FAK_RAP_KRAVH_FERDIGBH_NAV.FRA_SELVBETJ_LOSNING_FLAGG;