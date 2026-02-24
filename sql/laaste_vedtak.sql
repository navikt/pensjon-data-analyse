-- dataproduct_laaste_vedtak.py
-- pensjon-saksbehandli-prod-1f83.vedtak.laast_data_handling

-- oversikt over krav og vedtak som har blitt "låst opp" ved å endre status eller behandlingstype
-- lagt til mapping av k_laast_data_handling, k_sak_t og k_krav_gjelder til beskrivende navn

with laast_data_handling_map as (
    select 'ENDR_KRAV_BEH_TYPE' as kode, 'Endre behandlingstype' as dekode from dual
    union all
    select 'ENDR_STATUS_TYPE' as kode, 'Endre status' as dekode from dual
),
sak_t_map as (
         select 'AFP' as k_sak_t, 'AFP' as dekode from dual union all
         select 'AFP_PRIVAT', 'AFP Privat' from dual union all
         select 'ALDER', 'Alderspensjon' from dual union all
         select 'BARNEP', 'Barnepensjon' from dual union all
         select 'FAM_PL', 'Familiepleierytelse' from dual union all
         select 'GAM_YRK', 'Gammel yrkesskade' from dual union all
         select 'GENRL', 'Generell' from dual union all
         select 'GJENLEV', 'Gjenlevendeytelse' from dual union all
         select 'GRBL', 'Grunnblanketter' from dual union all
         select 'KRIGSP', 'Krigspensjon' from dual union all
         select 'OMSORG', 'Omsorgsopptjening' from dual union all
         select 'UFOREP', 'Uføretrygd' from dual
     ),
krav_gjelder_map as (
         select 'AFP_EO' as k_krav_gjelder, 'AFP etteroppgjør' as dekode from dual union all
         select 'ANKE', 'Anke' from dual union all
         select 'EKSPORT', 'Eksport' from dual union all
         select 'ENDR_UTTAKSGRAD', 'Endring uttaksgrad' from dual union all
         select 'ERSTATNING', 'Erstatning' from dual union all
         select 'ETTERGIV_GJELD', 'Ettergivelse av gjeld' from dual union all
         select 'FAS_UTG_IO', 'Dekning faste utgifter inst.opphold' from dual union all
         select 'FORSTEG_BH', 'Førstegangsbehandling' from dual union all
         select 'F_BH_BO_UTL', 'Førstegangsbehandling bosatt utland' from dual union all
         select 'F_BH_KUN_UTL', 'Førstegangsbehandling kun utland' from dual union all
         select 'F_BH_MED_UTL', 'Førstegangsbehandling Norge/utland' from dual union all
         select 'GJ_RETT', 'Gjenlevenderettighet' from dual union all
         select 'GOD_OMSGSP', 'Godskriving omsorgsopptjening' from dual union all
         select 'GOMR', 'G-omregning' from dual union all
         select 'HJLPBER_OVERG_UT', 'Hjelpeberegning ved overgang til uføretrygd' from dual union all
         select 'INNT_E', 'Inntektsendring' from dual union all
         select 'INNT_KTRL', 'Inntektskontroll' from dual union all
         select 'KLAGE', 'Klage' from dual union all
         select 'KONTROLL_3_17_A', 'Kontroll 3-17 a' from dual union all
         select 'KONVERTERING', 'Konvertert krav' from dual union all
         select 'KONVERTERING_MIN', 'Minimalt konvertert krav' from dual union all
         select 'KONV_AVVIK_G_BATCH', 'Konvertering - Avvik ved G-omr' from dual union all
         select 'MELLOMBH', 'Mellombehandling' from dual union all
         select 'MTK', 'Merskatt tilbakekreving' from dual union all
         select 'OMGJ_TILBAKE', 'Omgjøring av tilbakekreving' from dual union all
         select 'OVERF_OMSGSP', 'Overføring omsorgsopptjening' from dual union all
         select 'REGULERING', 'Regulering' from dual union all
         select 'REVURD', 'Revurdering' from dual union all
         select 'SAK_OMKOST', 'Saksomkostninger' from dual union all
         select 'SLUTTBEH_KUN_UTL', 'Sluttbehandling kun utland' from dual union all
         select 'SLUTT_BH_UTL', 'Sluttbehandling Norge/utland' from dual union all
         select 'SOK_OKN_UG', 'Søknad om økning av uføregrad' from dual union all
         select 'SOK_RED_UG', 'Søknad om reduksjon av uføregrad' from dual union all
         select 'SOK_UU', 'Søknad om ung ufør' from dual union all
         select 'SOK_YS', 'Søknad om yrkesskade' from dual union all
         select 'TILBAKEKR', 'Tilbakekreving' from dual union all
         select 'UTSEND_AVTALELAND', 'Utsendelse til avtaleland' from dual union all
         select 'UT_EO', 'Uføretrygd etteroppgjør' from dual union all
         select 'UT_VURDERING_EO', 'Uføretrygd vurdering av etteroppgjør' from dual
     ),
base as (
        select l.dato_opprettet as dato_opprettet,
                coalesce(d_s.dekode, s.k_sak_t) as sakstype,
                coalesce(d_k.dekode, kh.k_krav_gjelder) as kravtype,
                coalesce(kam.dekode, l.k_laast_data_handling) as handling,
                le.elementtype as elementtype,
                le.gammel_verdi as gammel_verdi,
                le.ny_verdi as ny_verdi
         from pen.t_laast_data_logg l
                  inner join pen.t_laast_data_logg_element le on le.laast_data_logg_id = l.laast_data_logg_id
                  inner join pen.t_kravhode kh on kh.kravhode_id = l.krav_id
                  left join krav_gjelder_map d_k on d_k.k_krav_gjelder = kh.k_krav_gjelder
                  inner join pen.t_sak s on s.sak_id = l.sak_id
                  left join sak_t_map d_s on d_s.k_sak_t = s.k_sak_t
                  left join laast_data_handling_map kam on kam.kode = l.k_laast_data_handling
         where le.elementtype != 'KRAVLINJE'
           and not (le.elementtype = 'KRAVHODE' and l.k_laast_data_handling = 'ENDR_STATUS_TYPE'))
select dato_opprettet,
       sakstype,
       kravtype,
       handling,
       elementtype,
       gammel_verdi,
       ny_verdi
from base