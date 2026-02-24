-- kravstatus.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.saksstatistikk.kravstatus_med_kravarsak

-- Kravårsak-mapping som erstatter pen.t_k_krav_arsak_t
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
)
select
    dim_sak_t.dekode as sakstype,
    dim_krav_gjelder.dekode as kravtype,
    dim_krav_s.dekode as kravstatus,
    coalesce(kam.dekode, arsak.k_krav_arsak_t) as kravarsak,
    count(*) antall
from pen.t_kravhode kh
inner join pen.t_k_krav_s dim_krav_s on dim_krav_s.k_krav_s = kh.k_krav_s
inner join pen.t_k_krav_gjelder dim_krav_gjelder on dim_krav_gjelder.k_krav_gjelder = kh.k_krav_gjelder
inner join pen.t_sak s on s.sak_id = kh.sak_id
inner join pen.t_k_sak_t dim_sak_t on dim_sak_t.k_sak_t = s.k_sak_t
inner join pen.t_krav_arsak arsak on arsak.kravhode_id = kh.kravhode_id
left join kravarsak_map kam on kam.k_krav_arsak_t = arsak.k_krav_arsak_t
where kh.k_krav_s not like 'AVBRUTT'
and kh.k_krav_s not like 'FERDIG'
group by dim_sak_t.dekode, dim_krav_gjelder.dekode, dim_krav_s.dekode, coalesce(kam.dekode, arsak.k_krav_arsak_t)
order by antall desc