-- kravstatus.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.saksstatistikk.kravstatus_med_kravarsak

-- Kravårsak-mapping som erstatter t_k_krav_arsak_t, t_k_krav_gjelder, t_k_sak_t, t_k_krav_s
with kravarsak_map as (
    select 'AFP_EO' as k_krav_arsak_t, 'AFP etteroppgjør' as dekode from dual union all
    select 'ALDERSOVERGANG', 'Aldersovergang' from dual union all
    select 'ANNEN_ARSAK', 'Annen årsak til saksbehandling' from dual union all
    select 'ANNEN_ARSAK_END_IN', 'Annen årsak til inntektsendring' from dual union all
    select 'ANNEN_FOR_END_IN', 'Annen forelder har endret inntekt' from dual union all
    select 'ANNET', 'Annet' from dual union all
    select 'AVSLAG', 'Avslag' from dual union all
    select 'AVSLAG_INVAL_ENKEP', 'Avslag (Invalide-/eller enkepensjon)' from dual union all
    select 'AVSLAG_UNG_UFR', 'Avslag ung ufør' from dual union all
    select 'AVSLAG_UT', 'Avslag uføretrygd' from dual union all
    select 'BARN_DOD', 'Dødsfall barn' from dual union all
    select 'BARN_ENDRET_INNTEKT', 'Inntekt til barn er endret' from dual union all
    select 'BARN_UTDS_FLYTTEUTG', 'Barnetilsyn/utd.stønad/flytteutg.' from dual union all
    select 'BARNETILLEGG', 'Barnetillegg' from dual union all
    select 'BEGGE_FOR_END_IN', 'Begge forsørgerne har endret inntekt' from dual union all
    select 'BODD_ARB_UTL', 'Bodd/arbeidet i avtaleland' from dual union all
    select 'EKSPORT', 'Eksport' from dual union all
    select 'EKTEFELLETILLEGG', 'Ektefelletillegg' from dual union all
    select 'ENDR_ANNEN_SAK', 'Bruker har annen sak som er endret' from dual union all
    select 'ENDRET_INNTEKT', 'Inntekt til bruker er endret' from dual union all
    select 'ENDRET_OPPTJENING', 'Opptjeningsgrunnlag er endret' from dual union all
    select 'ENDRING_IFU', 'Endring av IFU' from dual union all
    select 'EPS_ENDRET_INNTEKT', 'Inntekt til tilstøtende er endret' from dual union all
    select 'EPS_NY_YTELSE', 'Tilstøtende har fått innvilget pensjon' from dual union all
    select 'EPS_NY_YTELSE_UT', 'Tilstøtende har fått innvilget uføretrygd' from dual union all
    select 'EPS_OPPH_YTELSE_UT', 'Tilstøtende sin uføretrygd er opphørt' from dual union all
    select 'ETTEROPPGJOR', 'Etteroppgjør' from dual union all
    select 'GJENLEVENDERETT', 'Gjenlevenderett' from dual union all
    select 'GJENLEVENDETILLEGG', 'Gjenlevendetillegg' from dual union all
    select 'GJNL_SKAL_VURD', 'Gjenlevendetillegg skal vurderes' from dual union all
    select 'GODSKR_OVERF', 'Godskriving/overføring' from dual union all
    select 'GOMR', 'G-omregning' from dual union all
    select 'GRADSENDRINGER', 'Gradsendringer' from dual union all
    select 'HJEMMEBER_FOR_UT', 'Hjemmeberegning for uføretrygd' from dual union all
    select 'HVIL_STONADSRETT', 'Hvilende stønadsrett' from dual union all
    select 'INNT_KONTROLL', 'Inntektskontroll' from dual union all
    select 'INNVANDRET', 'Innvandring' from dual union all
    select 'INSTOPPHOLD', 'Institusjonsopphold på bruker/tilstøtende' from dual union all
    select 'KLAGE_ANKE', 'Klage/anke' from dual union all
    select 'KONTR_3_17', 'Kontroll 3-17 a' from dual union all
    select 'MEDISINSK_STONAD', 'Medisinsk stønad' from dual union all
    select 'MEDL_TRYGDETID', 'Medlemskap/trygdetid' from dual union all
    select 'MEDL_TRYGDETID_OPPTJ', 'Medlemskap/trygdetid/opptjening' from dual union all
    select 'NY_SOKNAD', 'Ny søknad' from dual union all
    select 'NYE_OPPLYSNINGER', 'Nye opplysninger på bruker' from dual union all
    select 'OMGJ_ETTER_ANKE', 'Omgjøring etter anke' from dual union all
    select 'OMGJ_ETTER_KLAGE', 'Omgjøring etter klage' from dual union all
    select 'OMREGN_UFORETRYGD', 'Omregning til uføretrygd' from dual union all
    select 'OMREGNING', 'Omregning pga regelendring' from dual union all
    select 'OMSORG_FOR_SMA_BARN', 'Omsorg for små barn' from dual union all
    select 'OPPHOR', 'Opphør av brukers ytelse' from dual union all
    select 'OPPHOR_HJELPELOS', 'Opphør av hjelpeløshetsbidrag' from dual union all
    select 'OPPHOR_RED_UFG', 'Opphør/reduksjon uføregrad' from dual union all
    select 'OPPHOR_REDUKSJON', 'Opphør/reduksjon' from dual union all
    select 'OPPHOR_TLG_HJELP_HUS', 'Opphør av tillegg til hjelp i huset' from dual union all
    select 'OPPL_UTLAND', 'Opplysninger fra utlandet' from dual union all
    select 'OPPTJENINGSGRUNNLAG', 'Opptjeningsgrunnlag' from dual union all
    select 'OVERGANGSSTONAD', 'Overgangsstønad' from dual union all
    select 'OVRFR_OMSORGSPOENG', 'Overføring av omsorgsopptjening' from dual union all
    select 'PENSJONSVILKAR', 'Pensjonsvilkår' from dual union all
    select 'PLEIE_ELDR_SYK_FUNK', 'Pleie eldre/syke/ funksjonshemmede' from dual union all
    select 'REGULERING', 'Regulering av brukers ytelse' from dual union all
    select 'OMR_FEILRETTING', 'Omregning etter feilretting' from dual union all
    select 'REKONSTRUKSJON', 'Rekonstruksjon' from dual union all
    select 'SAMORDNING', 'Samordning' from dual union all
    select 'SIVILSTANDSENDRING', 'Sivilstandsendring' from dual union all
    select 'SOKNAD_BT', 'Søknad om barnetillegg' from dual union all
    select 'TIDLIGUTTAK', 'Tidliguttak' from dual union all
    select 'TILBAKEKREVING', 'Tilbakekreving' from dual union all
    select 'TILST_DOD', 'Dødsfall tilstøtende' from dual union all
    select 'TILSTOT_ENDR_YTELSE', 'Tilstøtende sin sak er endret' from dual union all
    select 'TILSTOT_OPPHORT', 'Opphør av tilstøtendes ytelse' from dual union all
    select 'UFG_IFU_OG_IEU', 'Uføregrad/IFU og IEU' from dual union all
    select 'UFOREBER_V_GTGRD', 'Uføreberegning ved garantigrad' from dual union all
    select 'UFOREBER_V_TIDUFT', 'Uføreber. ved tidl. Uføretidspunkt' from dual union all
    select 'UFOREOVERGANG', 'Uføretrygdovergang' from dual union all
    select 'UFR_GRAD', 'Uføregrad' from dual union all
    select 'UFR_PENSJON_GRAD', 'Uføre-/pensjonsgrad' from dual union all
    select 'UFR_TIDSPUNKT', 'Uføretidspunkt' from dual union all
    select 'UT_EO', 'Etteroppgjør' from dual union all
    select 'UT_OMGJ_ANKE_EO', 'Omgjøring etter anke' from dual union all
    select 'UT_OMGJ_KLAGE_EO', 'Omgjøring etter klage' from dual union all
    select 'UT_VURDERING_EO', 'Vurdering Etteroppgjør' from dual union all
    select 'UTBET_AVKORT', 'Utbetaling/avkortning' from dual union all
    select 'UTTAKSGRAD', 'Uttaksgrad er endret eller oppdatert' from dual union all
    select 'UTVANDRET', 'Utvandring/eksport' from dual union all
    select 'VILKAR', 'Vilkår' from dual union all
    select 'VIRK_TIDSPUNKT', 'Virkningstidspunkt' from dual union all
    select 'VURD_SIVILST', 'Avvik i vurdert sivilstand for berørte saker' from dual union all
    select 'VURDER_FORSORG', 'Forsørgingstillegg skal vurderes' from dual union all
    select 'YRK_SKADE_SYK', 'Yrkesskade/-sykdom' from dual union all
    select 'YRKESSKADE', 'Yrkesskade' from dual union all
    select 'BEREGNING', 'Beregning' from dual union all
    select 'LOVVALG', 'Lovvalg' from dual union all
    select 'VURDER_SERSKILT_SATS', 'Særskilt sats for forsørger skal vurderes' from dual union all
    select 'OMGJ_ETTER_FVL_P35_C', 'Omgjøring fvl. § 35 første ledd bokstav c' from dual union all
    select 'TVUNGEN_FORVALTNING', 'Tvungen forvaltning' from dual union all
    select 'OMGJ_ETTER_FVL_P35_A', 'Omgjøring fvl. § 35 første ledd bokstav a' from dual union all
    select 'OMGJ_ETTER_FVL_P35_B', 'Omgjøring fvl. § 35 første ledd bokstav b' from dual
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
    coalesce(dim_sak_t.dekode, s.k_sak_t) as sakstype,
    coalesce(dim_krav_gjelder.dekode, kh.k_krav_gjelder) as kravtype,
    coalesce(dim_krav_s.dekode, kh.k_krav_s) as kravstatus,
    coalesce(kam.dekode, arsak.k_krav_arsak_t) as kravarsak,
    count(*) antall
from pen.t_kravhode kh
left join krav_s_map dim_krav_s on dim_krav_s.k_krav_s = kh.k_krav_s
left join krav_gjelder_map dim_krav_gjelder on dim_krav_gjelder.k_krav_gjelder = kh.k_krav_gjelder
inner join pen.t_sak s on s.sak_id = kh.sak_id
left join sak_t_map dim_sak_t on dim_sak_t.k_sak_t = s.k_sak_t
inner join pen.t_krav_arsak arsak on arsak.kravhode_id = kh.kravhode_id
left join kravarsak_map kam on kam.k_krav_arsak_t = arsak.k_krav_arsak_t
where kh.k_krav_s not like 'AVBRUTT'
and kh.k_krav_s not like 'FERDIG'
group by coalesce(dim_sak_t.dekode, s.k_sak_t),
         coalesce(dim_krav_gjelder.dekode, kh.k_krav_gjelder),
         coalesce(dim_krav_s.dekode, kh.k_krav_s),
         coalesce(kam.dekode, arsak.k_krav_arsak_t)
order by antall desc