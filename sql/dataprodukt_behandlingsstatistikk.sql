select
    lk_behandling_id as behandling_id,
    fk_pen_kravhode_rel as relatertbehandling_id,
    '-1' as relatert_fagsystem,
    lk_fagsak_id as sak_id,
    '-1' as saksnummer,
    '-1' as aktor_id,
    mottatt_tid,
    innreg_tid as registrert_tid,
    avsl_tid as ferdigbehandlet_tid,
    '-1' as utbetalt_tid,
    funk_tid_dato as funksjonell_tid,
    '-1' as forventetoppstart_tid,
    lastet_tid as teknisk_tid,
    sak_ytelse_kode as sak_ytelse,
    utenlandstilsnitt_fin_kode as sak_utland,
    behandling_type,
    behandling_status,
    behandling_resultat,
    '-1' as resultat_begrunnelse,
    behandling_metode_vedtak,
    lk_pen_krav_arsak_type_kode as behandling_arsak_krav,
    opprettet_av,
    lk_saksbehandler as saksbehandler,
    ansv_saksbh as ansvarlig_beslutter,
    org_enhet_id_fk as ansvarlig_enhet,
    '-1' as tilbakekrev_belop,
    '-1' as funksjonell_periode_fom,
    '-1' as funksjonell_periode_tom,
    'PESYS' as fagsystem_navn,
    '-1' as fagsystem_versjon
select * from dvh_sbs.vip4679_tim 

--lk_behandling_id
--lk_fagsak_id
--funk_tid
--pen_org_enhet_id
--org_enhet_id_fk
--fk_ek_org_node
--fk_ek_org_node_saksbehandler
--fk_ek_org_node_sb_vedtak
--fk_pen_krav_status
--behandling_status
--fk_pen_sak_type
--lk_pen_sak_type_kode
--fk_pen_krav_velg_type
--lk_pen_krav_velg_type_kode
--fk_pen_krav_gjelder
--behandling_type
--slettet_dato_kilde
--dvh_krav_avsluttet_dato
--vedtak_fattet_dato
--bruker_sluttbeh_dato
--behandling_resultat
--behandling_metode_vedtak
--behandling_metode_krav
--sak_ytelse_kode
--fk_dim_sak_ytelse
--fk_dim_behandling_type
--utenlandstilsnitt_fin_kode
--innreg_flagg
--avsl_flagg
--sendt_tr_flagg
--lk_saksbehandler
--opprettet_av
--innreg_tid
--mottatt_tid
--avsl_tid
--fk_person1
--funk_tid_dato
--opprettet_av_type_kode
--fra_sb_loesning_flagg
--selvbetj_bruker_flagg
--ansv_saksbh
--attesterer
--lk_pen_krav_arsak_type_kode
--gyldig_fra_dato_venter_rett
--gyldig_til_dato_venter_rett
--ka_dato
--sendt_ka_flagg
--tilsendt_flagg
--fk_pen_kravhode
--fk_pen_kravhode_rel
--lk_pen_kravhode_rel_ka
--sb_enhet_0001
--lastet_tid
--kildesystem