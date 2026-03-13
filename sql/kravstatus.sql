-- kravstatus.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.saksstatistikk.kravstatus
-- Lagt til div mappinger mellom kodeverkskoder og dekoder

with
-- noqa: disable=all
krav_s_map as (
    select 'ATT' as k_krav_s, 'Attestert' as dekode from dual union all
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
-- k_krav_gjelder, potensielt_lopende, dekode
    select 'ENDR_UTTAKSGRAD' as k_krav_gjelder, '1' as potensielt_lopende, 'Endring uttaksgrad' as dekode from dual union all
    select 'FAS_UTG_IO', '1', 'Dekning faste utgifter inst.opphold' from dual union all
    select 'FORSTEG_BH', '1', 'Førstegangsbehandling' from dual union all
    select 'GJ_RETT', '1', 'Gjenlevenderettighet' from dual union all
    select 'GOMR', '1', 'G-omregning' from dual union all
    select 'INNT_E', '1', 'Inntektsendring' from dual union all
    select 'KONVERTERING', '1', 'Konvertert krav' from dual union all
    select 'KONVERTERING_MIN', '1', 'Minimalt konvertert krav' from dual union all
    select 'KONV_AVVIK_G_BATCH', '1', 'Konvertering - Avvik ved G-omr' from dual union all
    select 'MTK', '1', 'Merskatt tilbakekreving' from dual union all
    select 'REGULERING', '1', 'Regulering' from dual union all
    select 'REVURD', '1', 'Revurdering' from dual union all
    select 'SLUTT_BH_UTL', '1', 'Sluttbehandling Norge/utland' from dual union all
    select 'SOK_OKN_UG', '1', 'Søknad om økning av uføregrad' from dual union all
    select 'SOK_RED_UG', '1', 'Søknad om reduksjon av uføregrad' from dual union all
    select 'SOK_UU', '1', 'Søknad om ung ufør' from dual union all
    select 'SOK_YS', '1', 'Søknad om yrkesskade' from dual union all
    select 'UT_EO', '1', 'Uføretrygd etteroppgjør' from dual union all
    select 'EKSPORT', '1', 'Eksport' from dual union all
    select 'F_BH_BO_UTL', '1', 'Førstegangsbehandling bosatt utland' from dual union all
    select 'F_BH_KUN_UTL', '1', 'Førstegangsbehandling kun utland' from dual union all
    select 'F_BH_MED_UTL', '1', 'Førstegangsbehandling Norge/utland' from dual union all
    select 'MELLOMBH', '1', 'Mellombehandling' from dual union all
    select 'SLUTTBEH_KUN_UTL', '1', 'Sluttbehandling kun utland' from dual union all
    select 'ANKE', '0', 'Anke' from dual union all
    select 'ERSTATNING', '0', 'Erstatning' from dual union all
    select 'ETTERGIV_GJELD', '0', 'Ettergivelse av gjeld' from dual union all
    select 'KLAGE', '0', 'Klage' from dual union all
    select 'SAK_OMKOST', '0', 'Saksomkostninger' from dual union all
    select 'AFP_EO', '0', 'AFP etteroppgjør' from dual union all
    select 'GOD_OMSGSP', '0', 'Godskriving omsorgsopptjening' from dual union all
    select 'HJLPBER_OVERG_UT', '0', 'Hjelpeberegning ved overgang til uføretrygd' from dual union all
    select 'INNT_KTRL', '0', 'Inntektskontroll' from dual union all
    select 'KONTROLL_3_17_A', '0', 'Kontroll 3-17 a' from dual union all
    select 'OMGJ_TILBAKE', '0', 'Omgjøring av tilbakekreving' from dual union all
    select 'OVERF_OMSGSP', '0', 'Overføring omsorgsopptjening' from dual union all
    select 'TILBAKEKR', '0', 'Tilbakekreving' from dual union all
    select 'UT_VURDERING_EO', '0', 'Uføretrygd vurdering av etteroppgjør' from dual union all
    select 'UTSEND_AVTALELAND', '0', 'Utsendelse til avtaleland' from dual
)

-- noqa: enable=all
select
    coalesce(sak_t_map.dekode, s.k_sak_t) as sakstype,
    coalesce(krav_gjelder_map.dekode, kh.k_krav_gjelder) as kravtype,
    coalesce(krav_s_map.dekode, kh.k_krav_s) as kravstatus,
    count(*) as antall,
    coalesce(krav_gjelder_map.potensielt_lopende, 'UKJENT') as kan_lope
from pen.t_kravhode kh
left join krav_s_map on krav_s_map.k_krav_s = kh.k_krav_s
left join krav_gjelder_map on krav_gjelder_map.k_krav_gjelder = kh.k_krav_gjelder
inner join pen.t_sak s on s.sak_id = kh.sak_id
left join sak_t_map on sak_t_map.k_sak_t = s.k_sak_t
where kh.k_krav_s not in ('AVBRUTT', 'FERDIG')
group by
    coalesce(sak_t_map.dekode, s.k_sak_t),
    coalesce(krav_gjelder_map.dekode, kh.k_krav_gjelder),
    coalesce(krav_s_map.dekode, kh.k_krav_s),
    coalesce(krav_gjelder_map.potensielt_lopende, 'UKJENT')
order by antall desc
