-- kravstatus.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.saksstatistikk.kravstatus
-- Lagt til div mappinger mellom kodeverkskoder og dekoder

with krav_s_map as (
    select 'ATT' as k_krav_s, 'Attestert' as decode from dual union all
    select 'AVBRUTT', 'Avbrutt behandling' from dual union all
    select 'BEREGNET', 'Beregnet' from dual union all
    select 'FERDIG', 'Ferdig behandlet' from dual union all
    select 'KLAR_TIL_ATT', 'Klar til attestering' from dual union all
    select 'PA_VENT', 'På vent' from dual union all
    select 'TIL_BEHANDLING', 'Til behandling' from dual union all
    select 'VENTER_AFP', 'Venter på Fellesordningen' from dual union all
    select 'VENTER_ANDRE', 'Venter på andre' from dual union all
    select 'VENTER_BRUKER', 'Venter på bruker' from dual union all
    select 'VENTER_KLAGEINSTANS', 'Venter på klageinstans' from dual union all
    select 'VENTER_SAKSBEH', 'Venter på saksbehandling' from dual union all
    select 'VILKARSPROVD', 'Vilkårsprøvd' from dual
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
)
select
    coalesce(dim_sak_t.dekode, s.k_sak_t) sakstype,
    coalesce(dim_krav_gjelder.dekode, kh.k_krav_gjelder) kravtype,
    coalesce(dim_krav_s.decode, kh.k_krav_s) kravstatus,
    count(*) antall
from pen.t_kravhode kh
left join krav_s_map dim_krav_s on dim_krav_s.k_krav_s = kh.k_krav_s
left join krav_gjelder_map dim_krav_gjelder on dim_krav_gjelder.k_krav_gjelder = kh.k_krav_gjelder
inner join pen.t_sak s on s.sak_id = kh.sak_id
left join sak_t_map dim_sak_t on dim_sak_t.k_sak_t = s.k_sak_t
where
    kh.k_krav_s not like 'AVBRUTT'
    and kh.k_krav_s not like 'FERDIG'
group by
    coalesce(dim_sak_t.dekode, s.k_sak_t),
    coalesce(dim_krav_gjelder.dekode, kh.k_krav_gjelder),
    coalesce(dim_krav_s.decode, kh.k_krav_s)
order by antall desc
