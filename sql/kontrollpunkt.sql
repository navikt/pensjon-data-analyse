-- kontrollpunkt.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.kontrollpunkt.kontrollpunkt_daglig

with kontrollpnkt_map as (
    select 'AFP_AP_PA_VENT' as k_kontrollpnkt_t, 'AFP i privat sektor kan ikke behandles automatisk fordi bruker ikke har løpende alderspensjon. Kravet må behandles manuelt.' as dekode_tekst from dual union all
    select 'AFP_FOR_TIDLIG_VIRK', 'Dersom en AFP skal revurderes tilbake i tid til et virkningstidspunkt som er før eller lik eksisterende virkningstidspunkt, må først eksisterende AFP-historikk fjernes ved å opphøre kravet fra samme tidspunkt. Deretter må det opprettes et nytt førstegangskrav fra det nye virkningstidspunktet.' from dual union all
    select 'AFP_OFF_MANGLER_API', 'AFP Offentlig livsvarig kan ikke behandles automatisk. Klarte ikke å avklare status på livsvarig AFP offentlig. Kontakt tjenestepensjonleverandør for å avklare status. Informasjon om AFP offentlig må legges til manuelt.' from dual union all
    select 'AFP_OFF_UKJENT', 'AFP Offentlig livsvarig kan ikke behandles automatisk. Klarte ikke å avklare status på livsvarig AFP offentlig. Kontakt tjenestepensjonleverandør for å avklare status. Informasjon om AFP offentlig må legges til manuelt.' from dual union all
    select 'AFP_RESULT_MANGLER', 'Brukers krav om AFP i privat sektor mangler resultat fra Fellesordningen. Resultatet av vilkårsprøvingen fra Fellesordningen må innhentes manuelt og kravet ferdigbehandles.' from dual union all
    select 'ANB_VILKAR', 'Automatisk vilkårsprøving kunne ikke gjennomføres og saken må ferdigbehandles manuelt.' from dual union all
    select 'ANNET_VEDTAK', 'Det finnes vedtak under iverksettelse på samme sak. Sjekk om vedtaket avventer samordning.' from dual union all
    select 'AO_APENTKRAV_KONV', 'Alderskonvertering: Brukers ytelse skal konverteres til alderspensjon men dette kan ikke gjøres av automatisk prosess ettersom bruker har et åpent krav.' from dual union all
    select 'AO_APENTKRAV_OMR', 'Aldersovergang: Brukers ytelse skal omregnes, men dette kan ikke gjøres av automatisk prosess ettersom bruker har et åpent krav.' from dual union all
    select 'AO_APENTKRAV_OPPH', 'Aldersovergang: Brukers ytelse skal opphøres, men dette kan ikke gjøres av automatisk prosess ettersom bruker har et åpent krav.' from dual union all
    select 'AO_FEIL_AFPPRIVAT', 'Aldersovergang: Omregningen av brukers AFP Privat for opphør av kronetillegg ved 67 år feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_BT_ULOVLIG', 'Aldersovergang: opphør av barnetillegg feilet grunnet annen aldersovergang i samme måned med annen virkningsdato, barnetilleget må opphøres manuellt' from dual union all
    select 'AO_FEIL_KONV_ALDER', 'Alderskonvertering: Konverteringen til alderspensjon feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_OMR70', 'Aldersovergang: Omregning av brukers alderspensjon ved 70 år feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_OMR_BT', 'Aldersovergang: Omregning av brukers ytelse grunnet opphør av barnetillegg feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_OMR_ET60', 'Aldersovergang: Omregning av brukers ytelse grunnet ektefelletillegg for ektefelle som fyller 60 år feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_OMR_ET67', 'Aldersovergang: Omregning av brukers ytelse grunnet ektefelletillegg for ektefelle som fyller 67 år feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_OPPH_AFPSTAT', 'Aldersovergang: Opphør av statlig AFP  feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_OPPH_BP18', 'Aldersovergang: Opphør av barnepensjon  feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_OPPH_GJP67', 'Aldersovergang: Opphør av full gjenlevendepensjon ved 67 år feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_OPPH_INTAV67', 'Aldersovergang: Opphør av inntektsavkortet gjenlevendepensjon ved 67 år feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_OPPH_UP67', 'Aldersovergang: Opphør av uførepensjon med uttaksgrad 0 ved 67 år feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_REVURD67', 'Aldersovergang: Omregningen av brukers alderspensjon ved 67 år grunnet garantitillegg feilet. Behandlingen må gjøres manuelt.' from dual union all
    select 'AO_FEIL_REVURD75', 'Aldersovergang: Revurdering av brukers alderspensjon til 100 % ved 75 år feilet. Kravet må behandles manuelt.' from dual union all
    select 'AO_FEIL_UNGUFOR', 'Aldersovergang: Omregning av brukers uførepensjon for ung uførefordel feilet. Kravet må behandles manuelt.' from dual union all
    select 'AP_PA_VENT_AVSL', 'Bruker har fått henlagt krav om AFP i privat sektor, men har et krav om alderspensjon til behandling.' from dual union all
    select 'AP_TIL_BEHANDLING', 'Bruker har et alderspensjonskrav til behandling som må iverksettes før AFP i privat sektor kan iverksettes' from dual union all
    select 'AP_UTEN_UTTAK', 'Bruker søker AFP i privat sektor og har alderspensjon med 0% uttaksgrad ved kravets virkningstidspunkt. Bruker må informeres om at uttaksgrad på alderspensjonen må være større enn 0% for å få innvilget AFP i privat sektor.' from dual union all
    select 'AP_VENTER_UTLAND', 'Bruker har et alderspensjonskrav med en eller flere kravlinjer som har status "Venter på utland". Dette kan medføre at AFP kravet i privat sektor ikke kan iverksettes. Alle kravlinjer på AFP i privat sektor må i slike tilfeller også settes til "Venter på utland".' from dual union all
    select 'ATTESTERE', 'Det er feil eller mangler i opplysninger registrert på bruker. Attestering kan ikke gjennomføres' from dual union all
    select 'AUTO_PROS_FEILER', 'Den automatiske prosessen feiler. Behandlingen må fullføres manuelt.' from dual union all
    select 'AVVIK_EPS', 'Det er avvik mellom sivilstanden oppgitt av bruker og den som er registrert i TPS for ektefelle/partner/samboer.' from dual union all
    select 'AVVIK_SIVILSTAND', 'Det er avvik mellom vurdert sivilstand og sivilstand oppgitt av bruker.' from dual union all
    select 'AVVIK_UTENFOR_TOL', 'Avvik utenfor toleransegrense' from dual union all
    select 'AVVIK_VUR_SIVILST', 'Automatisk saksbehandling kunne ikke vurdere sivilstand. Dette skyldes at det er avvik mellom boforhold bruker har angitt i selvbetjeningsløsningen og vurdert sivilstand som allerede ligger registrert på personen i PESYS.' from dual union all
    select 'BAKSYS_UTILGJENGELIG', 'Teknisk feil oppsto under automatisk behandling. Kravet må behandles manuelt.' from dual union all
    select 'BARNET', 'Det er søkt om barnetillegg og dette kan ikke behandles automatisk. Saksbehandler må behandle saken manuelt.' from dual union all
    select 'BARN_MULIG_BP', 'Dødsfall over et år tilbake i tid og denne personen har muligens rett på barnepensjon' from dual union all
    select 'BEHANDE_VEDTAK', 'Brukers tilstøtende har et åpent vedtak som ikke er et sammenstøtsvedtak, og det er ingen sivilstandsendring mellom bruker og tilstøtende i virkningsperioden til Bs krav.' from dual union all
    select 'BEHAND_KODE', 'Det er feil eller mangler i opplysninger registrert på bruker. Behandlingstypekode kan ikke settes eller mangler' from dual union all
    select 'BEREGN', 'Entydig beregningsresultat kunne ikke gis. Dette kan være fordi en av ytelsene som inngår i beregningen har blitt manuelt overstyrt.' from dual union all
    select 'BERORT_SAK_AAPENT_KR', 'Brukers sak blir berørt av endring i saken til ektefelle/partner/samboer og status på brukers sak har blitt satt tilbake til behandling.' from dual union all
    select 'BER_MAN_OVERSTYRT', 'Forrige beregning er manuelt overstyrt (kan gjelde både bruker og tilstøtende). Sjekk ny beregning nøye.' from dual union all
    select 'BER_MINKONV_FAKTOMR', 'Løpende vedtak er faktoromregnet (kan gjelde både bruker og tilstøtende). Valgt virkningstidspunkt må endres slik at saken kan beregnes. Saksbehandler må selv vurdere  hvor langt tilbake i tid virkningstidspunktet må settes  Sjekk ny beregning nøye.' from dual union all
    select 'BORMED', 'Det er ikke samsvar mellom opplysninger oppgitt av bruker og TPS. Opplysninger må kontrolleres manuelt.' from dual union all
    select 'BREVKODE_FEILET', 'Bestilling av brev feilet i automatisk saksbehandling. Saken må behandles ferdig manuelt, og brev må sendes til bruker.' from dual union all
    select 'BRUKER_DOD_APENT_KRV', 'Bruker har avgått ved døden etter at dette kravet ble opprettet. Kravet må ferdigbehandles og deretter opphøres manuelt, eventuelt feilregistreres. Eventuelle løpende ytelser er allerede automatisk opphørt.' from dual union all
    select 'BRUKER_ENDR_MAN', 'Forrige beregning er manuelt overstyrt (kan gjelde både bruker og tilstøtende). Sjekk ny beregning nøye.' from dual union all
    select 'BRUKER_FLERE_KRAV', 'Bruker har flere krav til behandling. Den automatiske saksbehandlingen kunne ikke gjennomføres fordi kravene kan påvirke hverandre.' from dual union all
    select 'BRUKER_HAR_OPPHOR', 'Bruker har et opphørskrav som ikke er ferdigbehandlet, dette må feilregistreres eller iverksettes før vurdering av sammenstøt kan gjennomføres siden det ikke tillattes to åpne krav på samme sak samtidig.' from dual union all
    select 'BRUKER_MAN', 'Brukers krav kunne ikke beregnes automatisk. Saken må behandles manuelt.' from dual union all
    select 'BRUKER_OVERSTYRT_BER', 'Bruker har fått manuelt overstyrt sin beregning' from dual union all
    select 'BRUKER_UT_GJT', 'Manuell behandling pga. tidligere gjenlevenderett. Gjenlevenderett i alderspensjon må vurderes av saksbehandler.' from dual union all
    select 'BT_KRAV_VIRK_ENDRET', 'Virkningsdato på kravet er endret. Dette har medført at det ikke er samsvar mellom virkningsdato på kravet og periodene i Barnetillegg. Virkningsdato på kravet må derfor rettes tilbake til den opprinnelige datoen.' from dual union all
    select 'BT_SAMSVAR_FORELDRE', 'Det er registrert at barnet bor med begge foreldre. Det samsvarer ikke med opplysningene i Familieforhold. Registreringen må endres før videre behandling.' from dual union all
    select 'DATO_TILBAKE_TID', 'Antatt virkningsdato er tilbake i tid.  Automatisk saksbehandling kan ikke gjennomføres. Kravet må behandles manuelt.' from dual union all
    select 'DOD_ANNULLERT', 'Dødsdato er annullert' from dual union all
    select 'DOD_DATO_FEIL', 'Dødsdato på en annen dag er allerede registrert' from dual union all
    select 'DOD_DATO_FREM_I_TID', 'Dødsdato satt frem i tid' from dual union all
    select 'DOD_KRG_GYRK', 'Hvis avdøde hadde Krig og/eller gammel yrkesskade, så kan ikke ytelsen opphøres automatisk.' from dual union all
    select 'DOD_UTLAND_PENSJON', 'Avdøde har pensjon i utland' from dual union all
    select 'EKTEF_FLERE_KRAV', 'Ektefelle har flere krav til behandling. Den automatiske saksbehandlingen kunne ikke gjennomføres fordi kravene kan påvirke hverandre og/eller brukers krav.' from dual union all
    select 'EPS_INNTEKT_2G', 'Registrert inntekt på EPS er under 2G. Inntektene må undersøkes manuelt.' from dual union all
    select 'EPS_MULIG_GLP', 'Dødsfall over et år tilbake i tid og denne personen har muligens gjenlevenderett' from dual union all
    select 'ESTIMERT_TT_KAP19', 'Det er benyttet en estimert (ikke faktisk) trygdetid for kapittel 19 i beregningen av garantitilleggsbeholdning. Trygdetidsperioder for kapittel 19 må derfor registreres i skjermbildet Registrere trygdetid for at beregningen skal bli riktig.' from dual union all
    select 'FAMOPP', 'Det er mangler eller avvik i opplysninger om brukers familieforhold' from dual union all
    select 'FAMOPP_AVDOD', 'Det er mangler eller avvik i opplysninger om avdød' from dual union all
    select 'FAMOPP_BARN', 'Det er mangler eller avvik i opplysninger om barn' from dual union all
    select 'FAMOPP_EKTEF', 'Det er mangler eller avvik i opplysninger om ektefelle' from dual union all
    select 'FAMOPP_FAR', 'Det er mangler eller avvik i opplysninger om far' from dual union all
    select 'FAMOPP_FBARN', 'Det er mangler eller avvik i opplysninger om fosterbarn' from dual union all
    select 'FAMOPP_MANGLER', 'Et eller flere familieforhold strekker seg ikke over hele kravets periode. Familieforhold må oppdateres, vedtaksperioden må avgrenses eller delytelsen må avslås i perioden uten familierelasjon.' from dual union all
    select 'FAMOPP_MOR', 'Det er mangler eller avvik i opplysninger om mor' from dual union all
    select 'FAMOPP_PARTNER', 'Det er mangler eller avvik i opplysninger om partner' from dual union all
    select 'FAMOPP_SAMBO', 'Det er mangler eller avvik i opplysninger om samboer' from dual union all
    select 'FAMOPP_SOKER', 'Det er mangler eller avvik i opplysninger om bruker' from dual union all
    select 'FAMOPP_SOSKEN', 'Det er mangler eller avvik i opplysninger om søsken' from dual union all
    select 'FASTS_IFU_FEIL', 'Fastsettelse av IFU feilet, kontroller at Vilkårsvurdering del 2 er fylt ut.' from dual union all
    select 'FEIL_BORM_TILST', 'Bruker er registrert med en ektefelle/partner/samboer som har samme relasjon i et annet krav. Vurder om aktuell sak/krav  må revurderes.' from dual union all
    select 'FLERE_PERIODER_M_UT', 'Kravet dekker en periode som tidligere har vært opphørt. Periodene med uføretrygd må behandles i flere kravbehandlinger. Vilkårsperiodene etter opphøret må først opphøres med virkningsdato lik virkningsdato for innvilgelse. Deretter kan perioden før opphøret revurderes. Senere vilkårsperiode(r) med uføretrygd må rekonstrueres.' from dual union all
    select 'FLER_KRV_BERO_PA_SAK', 'Saksbehandler må gjøre flere revurderinger for å kunne behandle ulike uføregrader på ektefelle/partner/samboer sin sak.' from dual union all
    select 'FOR_TIDLIG_VIRK', 'Kravet gjelder nytt regelverk for alderspensjon, men virkningsdato er satt til en dato før 01.01.2011.' from dual union all
    select 'FT_KRAV', 'Det er søkt om barnetillegg og dette kan ikke behandles automatisk. Saken må behandles manuelt.' from dual union all
    select 'FULLMAKT', 'Iverksettelsen av vedtaket kunne ikke gjennomføres. Forsøk å iverksette vedtaket på nytt ved å trykke på iverksettelsesknappen.' from dual union all
    select 'GJENLEV_MED_100_PROSENT_UT', 'Vurder om bruker skal informeres om gjenlevendetillegg. Se rundskriv med overgangsreglene.' from dual union all
    select 'GRUNNLAG', 'Det er feil eller mangler i grunnlag' from dual union all
    select 'GR_AVDOD_UTV_DNR', 'Bruker med d-nummer eller status utvandret.' from dual union all
    select 'GR_BREV_ADR', 'Vi kan ikke sende brev fordi vi mangler adresse til mottaker.' from dual union all
    select 'GR_BREV_ANNET', 'Vi kan ikke sende brev av ukjent årsak.' from dual union all
    select 'GR_BREV_BARN18', 'Barnet er under 18 år, uten gjenlevende foreldre og kan ha rett til barnepensjon.' from dual union all
    select 'GR_BREV_BARN20', 'Barnet er under 20 år, uten gjenlevende foreldre og kan ha rett til barnepensjon.' from dual union all
    select 'GR_BREV_BARN21', 'Barnet er mellom 18 og 21 år og kan ha rett til barnepensjon.' from dual union all
    select 'GR_BREV_SAMBO', 'Vurder om gjenlevende foreldre til avdødes barn kan være 1.5 samboer.' from dual union all
    select 'GT_BREV_EPS', 'Gjenlevende er over 67 år og har ikke tatt ut alderspensjon. Bruker kan ha rett på gjenlevendetillegg i alderspensjonen. Saksbehandler må vurdere å informere bruker om rettigheter.' from dual union all
    select 'HALVMP_BP_FORELDLOS', 'Automatisk vilkårsprøving av unntak fra tre års forutgående medlemskap grunnet opptjening minst svarende til halv minstepensjon (§18-2 sjuende ledd) støttes ikke for Barnepensjon foreldreløs. Kontroller dette manuelt.' from dual union all
    select 'IKKE_SAMST_IKKE_VILK', 'Kravet må behandles og vilkårsprøves før det kan vurderes om det finnes berørte saker' from dual union all
    select 'IKKE_VILKARSPROVD', 'Minst en kravlinje kunne ikke vilkårsprøves.' from dual union all
    select 'INFO_GRUNNLAG', 'Automatisk innhenting av grunnlag kunne ikke gjøres. Opplysninger må innhentes manuelt.' from dual union all
    select 'INGEN_BARN', 'Det finnes ingen barn som det kan overføres omsorgspoeng for i angitt opptjeningsår. Saksbehandler må ferdigbehandle kravet manuelt og vurdere om bruker skal kontaktes.' from dual union all
    select 'INGEN_OMSORGSPOENG', 'Avgiver har ingen omsorgspoeng å overføre i år det ønskes å overføre omsorgspoeng for. Saksbehandler må ferdigbehandle kravet manuelt og vurdere om bruker skal kontaktes.' from dual union all
    select 'INNTOPP_AVDOD', 'Det er mangler eller avvik i opplysninger om inntekter for avdød' from dual union all
    select 'INNTOPP_BARN', 'Det er mangler eller avvik i opplysninger om inntekter for barn' from dual union all
    select 'INNTOPP_EKTEF', 'Det er mangler eller avvik i opplysninger om forventet inntekter for ektefelle' from dual union all
    select 'INNTOPP_FAR', 'Det er mangler eller avvik i opplysninger om inntekter for far' from dual union all
    select 'INNTOPP_FBARN', 'Det er mangler eller avvik i opplysninger om inntekter for fosterbarn' from dual union all
    select 'INNTOPP_MOR', 'Det er mangler eller avvik i opplysninger om inntekter for mor' from dual union all
    select 'INNTOPP_PARTNER', 'Det er mangler eller avvik i opplysninger om inntekter for partner' from dual union all
    select 'INNTOPP_SAMBO', 'Det er mangler eller avvik i opplysninger om inntekter for samboer' from dual union all
    select 'INNTOPP_SOKER', 'Det er mangler eller avvik i opplysninger om inntekter for bruker' from dual union all
    select 'INNTOPP_SOSKEN', 'Det er mangler eller avvik i opplysninger om inntekter for søsken' from dual union all
    select 'INSTOPP', 'Bruker eller ektefelle/partner/samboer har institusjonsopphold. Institusjonsopphold må registreres på alle saker.' from dual union all
    select 'INSTOPP_AVDOD', 'Det er mangler eller avvik i institusjonsopphold for avdød' from dual union all
    select 'INSTOPP_BARN', 'Det er mangler eller avvik i institusjonsopphold for barn' from dual union all
    select 'INSTOPP_EKTEF', 'Det er mangler eller avvik i institusjonsopphold for ektefelle' from dual union all
    select 'INSTOPP_EPS', 'Det er avvik i institusjonsoppholdsopplysninger mellom brukers og tilstøtenes saker.' from dual union all
    select 'INSTOPP_FAR', 'Det er mangler eller avvik i institusjonsopphold for far' from dual union all
    select 'INSTOPP_FBARN', 'Det er mangler eller avvik i institusjonsopphold for fosterbarn' from dual union all
    select 'INSTOPP_MOR', 'Det er mangler eller avvik i institusjonsopphold for mor' from dual union all
    select 'INSTOPP_PARTNER', 'Det er mangler eller avvik i institusjonsopphold for partner' from dual union all
    select 'INSTOPP_SAMBO', 'Det er mangler eller avvik i institusjonsopphold for samboer' from dual union all
    select 'INSTOPP_SOKER', 'Det er mangler eller avvik i institusjonsopphold for bruker' from dual union all
    select 'INSTOPP_SOSKEN', 'Det er mangler eller avvik i institusjonsopphold for søsken' from dual union all
    select 'IVERKSETT_POPP', 'Vedtak kan ikke iverksettes fordi oppdateringen av pensjonspoeng feilet i opptjeningsregisteret. Forsøk å iverksette vedtaket på nytt.' from dual union all
    select 'I_VERK_VEDTAK', 'Det er feil eller mangler i opplysninger registrert på bruker. Vedtak kan ikke iverksettes' from dual union all
    select 'KONTO', 'Kontoopplysninger mangler eller er avvikende' from dual union all
    select 'KONTROLL_ET_AK', 'Kontroll av ektefelletillegg: Brukers ektefelletillegg skal kontrolleres, men dette kan ikke gjøres automatisk ettersom bruker har et åpent krav.' from dual union all
    select 'KONTROLL_ET_EPS_AK', 'Kontroll av ektefelletillegg: Brukers ektefelletillegg skal kontrolleres. Dette kan ikke gjøres automatisk. Vurder om ektefelle/partner/samboers grunnlag må oppdateres. Fullfør behandlingen manuelt.' from dual union all
    select 'KONTROLL_ET_FEIL', 'Kontroll av ektefelletillegg: Kontroll av brukers ektefelletillegg feilet. Behandlingen må gjøres manuelt.' from dual union all
    select 'KONTROLL_SS_AK', 'Kontroll av særskilt sats: Brukers særskilt sats skal kontrolleres, men dette kan ikke gjøres automatisk ettersom bruker har et åpent krav.' from dual union all
    select 'KONTROLL_SS_EPS_AK', 'Kontroll av særskilt sats: Brukers særskilt sats skal kontrolleres. Dette kan ikke gjøres automatisk. Vurder om ektefelle/partner/samboers grunnlag må oppdateres. Fullfør behandlingen manuelt.' from dual union all
    select 'KONTROLL_SS_FEIL', 'Kontroll av særskilt sats: Kontroll av brukers særskilt sats feilet. Behandlingen må gjøres manuelt.' from dual union all
    select 'KRAVKONTROLL', 'Det er avvik mellom opplysninger oppgitt av bruker og opplysninger i registre.' from dual union all
    select 'KRAVTYPE', 'Det er feil eller mangler på opplysninger knyttet til kravtype. Det må gjøres manuell kontroll av kravtype.' from dual union all
    select 'KRAV_GRLAG', 'Det finnes avvik i opplysningene registrert på kravet.' from dual union all
    select 'KRAV_IKKE_OK', 'Brukers krav har ikke nok informasjon til å kunne utføre beregning' from dual union all
    select 'KUN_UTL_KRAVLINJER', 'En av brukerne i berørte saker har et åpent krav med kun utenlandske kravlinjer og en løpende ytelse på samme sak. Dette kravet må enten ferdigbehandles eller feilregistreres.' from dual union all
    select 'MANGEL_I_BT_GRNL', 'Det er ikke fylt ut tilstrekkelig informasjon om barnet til å kunne vilkårsprøve barnetillegget. Gå tilbake til skjermbildet Barnetillegg og fullfør vurderingen.' from dual union all
    select 'MANGLER_BORMED', 'Angi vurdert sivilstand for ektefelle/partner/samboer.' from dual union all
    select 'MANGLER_BOTIDOPP_EPS', 'Avkrysningsboks for "Det er vurdert om ektefelle/partner/samboer har utenlandsopphold" er ikke krysset av.' from dual union all
    select 'MANUELL_BEHANDLING', 'Det er ikke støtte for å behandle denne kravtypen automatisk. Krav må behandles manuelt.' from dual union all
    select 'MANUELL_VKP', 'Automatisk saksbehandling kunne ikke vilkårsprøve. Saken må ferdigstilles manuelt fra dette steget.' from dual union all
    select 'MAN_BEH_APENT_KRAV', 'Bruker har allerede et åpent krav til behandling. Hendelsen må derfor håndteres manuelt.' from dual union all
    select 'MAN_BEH_AP_HENDL_UTL', 'Krav om alderspensjon er automatisk opprettet uten funksjonelle eller tekniske feilsituasjoner. Kravet overføres til manuell behandling alderspensjon.' from dual union all
    select 'MAN_BEH_GENERELL_FEIL', 'Hendelsen ble ikke håndtert automatisk på grunn av en generell feil. Saksbehandler må derfor sjekke saken og vurdere videre behandling.' from dual union all
    select 'MAN_BEH_GNRL_MANGLER', 'Det er feil eller mangler i grunnlagsinformasjon mottatt fra utenlandskomponenten til Pensjon. Hendelsen må derfor håndteres manuelt.' from dual union all
    select 'MAN_BEH_HAR_AP', 'Bruker har allerede løpende alderspensjon eller et åpent krav om alderspensjon til behandling. Henvendelsen må derfor håndteres manuelt.' from dual union all
    select 'MAN_BEH_INFO_FRA_UTLAND', 'Utenlandskomponenten til Pensjon har mottatt informasjon fra utlandet. Saksbehandler må vurdere behov for videre behandling.' from dual union all
    select 'MAN_BEH_UKJENT_HENDELSE', 'Pesys mottok en ukjent hendelse som medførte at automatisk behandling feilet. Saken må derfor håndteres manuelt.' from dual union all
    select 'MAN_BEH_UT_HENDL_UTL', 'Krav om uføretrygd er automatisk opprettet uten funksjonelle eller tekniske feilsituasjoner. Kravet overføres til manuell behandling uføretrygd.' from dual union all
    select 'MEDL', 'Brukers medlemskap i folketrygden må kontrolleres manuelt.' from dual union all
    select 'MOTREGN_FLER_PERIOD', 'Det har tidligere vært motregnet sykepenger/AAP i saken. Saken må revurderes pr. periode med motregning.' from dual union all
    select 'NULLSTILL_BEHOLDNING', 'Brukers pensjonsbeholdning har blitt regulert. Status på kravet har blitt satt tilbake til "Til behandling". Nye registeropplysninger må hentes inn i skjermbildet Opptjening.' from dual union all
    select 'OMR_AFP_FEILET', 'Omregning feilet fordi AFP ikke skal omregnes.' from dual union all
    select 'OMR_APENT_KRAV', 'Omregning ved ny/korrigert opptjening: Omregning kunne ikke gjøres fordi bruker har et åpent krav. Kravet må behandles manuelt.' from dual union all
    select 'OMR_APENT_KRAV_GJEN', 'Omregning ved ny/korrigert opptjening: Omregning kunne ikke gjøres fordi gjenlevende har et åpent krav. Kravet må behandles manuelt.' from dual union all
    select 'OMR_EPS_MOTTAR_AFP', 'Omregning går til manuell behandling fordi EPS mottar AFP' from dual union all
    select 'OMR_FEILET', 'Omregning ved ny/korrigert opptjening: Omregning feilet. Kravet må behandles manuelt.' from dual union all
    select 'OMR_FEILET_TT_ANV', 'Omregning ved tilvekst i opptjening: Omregning feilet på grunn av ugyldig endring i trygdetid. Kravet må behandles manuelt.' from dual union all
    select 'OMR_IKKE_REKKEFOLGE', 'Omregning går til manuelt fordi rekkefølgen på behandlingen av sakene til familien ikke kunne bestemmes' from dual union all
    select 'OMR_UTLAND_PENSJON', 'Bruker har pensjon i utlandet' from dual union all
    select 'OMR_UTV_VED_OPPTJ', 'Omregning går til manuell behandling fordi bruker ikke bor i Norge i opptjeningsåret' from dual union all
    select 'OMSORGGR_GODSKR', 'Grunnlaget for godskriving av omsorgspoeng finnes ikke. Dette må vurderes.' from dual union all
    select 'OMSORGGR_OVERFOR', 'Grunnlaget for overføring av omsorgspoeng finnes ikke. Dette må vurderes.' from dual union all
    select 'OMSORGKRAV', 'Bruker har et krav om omsorgsopptjening. Hvis det gjelder omsorgsopptjening før 1992 må det behandles før AFP kravet i privat sektor kan behandles.' from dual union all
    select 'OMSORG_APEN', 'Bruker har en åpen sak av typen omsorgspoeng' from dual union all
    select 'OMSORG_FOR_1992', 'Bruker forsøker å overføre omsorgsopptjening som er opptjent før 1992 til en person som på grunn av alder/fødselsår ikke har rett på denne type omsorgsopptjening. Kravet om overføring skal avslås' from dual union all
    select 'OPPDRAG', 'Det er feil eller mangler i opplysninger registrert på bruker. Overføring av vedtak til Oppdrag kan ikke gjennomføres' from dual union all
    select 'OPPHOR_UT_AAPENT_KRAV', 'Opphør av brukers uføretrygd kunne ikke gjennomføres grunnet åpent krav i uføretrygden' from dual union all
    select 'OPPHOR_UT_AVKORT_TT', 'Opphør av brukers uføretrygd kunne ikke gjennomføres da bruker har avkortet fremtidig trygdetid' from dual union all
    select 'OPPHOR_UT_FAKT', 'Opphør av brukers uføretrygd kunne ikke gjennomføres da ytelsen er faktoromregnet' from dual union all
    select 'OPPHOR_UT_MAN_BER', 'Opphør av brukers uføretrygd kunne ikke gjennomføres da ytelsen er manuelt overstyrt' from dual union all
    select 'OPPHOR_UT_NORDISK_GP', 'Opphør av brukers konverterte uføretrygd må følges opp av saksbehandler da ytelsen har nordisk avkortet grunnpensjon' from dual union all
    select 'OPPHOR_UT_NORDISK_TP', 'Opphør av brukers konverterte uføretrygd kunne ikke gjennomføres da ytelsen har nordisk avkortet tillegspensjon' from dual union all
    select 'OPPOPP_AVDOD', 'Det er mangler eller avvik i opptjeningsopplysninger for avdød' from dual union all
    select 'OPPOPP_BARN', 'Det er mangler eller avvik i opptjeningsopplysninger for barn' from dual union all
    select 'OPPOPP_EKTEF', 'Det er mangler eller avvik i opptjeningsopplysninger for ektefelle' from dual union all
    select 'OPPOPP_FAR', 'Det er mangler eller avvik i opptjeningsopplysninger for far' from dual union all
    select 'OPPOPP_FBARN', 'Det er mangler eller avvik i opptjeningsopplysninger for fosterbarn' from dual union all
    select 'OPPOPP_MOR', 'Det er mangler eller avvik i opptjeningsopplysninger for mor' from dual union all
    select 'OPPOPP_PARTNER', 'Det er mangler eller avvik i opptjeningsopplysninger for partner' from dual union all
    select 'OPPOPP_SAMBO', 'Det er mangler eller avvik i opptjeningsopplysninger for samboer' from dual union all
    select 'OPPOPP_SOKER', 'Det er mangler eller avvik i opptjeningsopplysninger for bruker.' from dual union all
    select 'OPPOPP_SOSKEN', 'Det er mangler eller avvik i opptjeningsopplysninger for søsken' from dual union all
    select 'OPPRETT_VEDTAK', 'Sivilstandsinformasjonen på brukers krav og kravet til ektefelle/partner/samboer stemmer ikke overens, og/eller kravene er revurderingskrav og har ulik ønsket virkningsdato.' from dual union all
    select 'OVERLAPP_VERGEINFO', 'Automatisk saksbehandling kunne ikke bestemme verge. Dette skyldes sannsynligvis at det er overlappende vergeinformasjon. Det må tas stilling til hvem som skal benyttes som verge. Kravet må ferdigstilles manuelt fra og med fatting av vedtak.' from dual union all
    select 'PENGEMOTTAKER', 'Det er registrert mer enn én pengemottaker for bruker' from dual union all
    select 'PERSOPP_AVDOD', 'Det er mangler eller avvik i personopplysninger for avdød' from dual union all
    select 'PERSOPP_BARN', 'Det er mangler eller avvik i personopplysninger for barn' from dual union all
    select 'PERSOPP_EKTEF', 'Det er mangler eller avvik i personopplysninger for ektefelle' from dual union all
    select 'PERSOPP_FAR', 'Det er mangler eller avvik i personopplysninger for far' from dual union all
    select 'PERSOPP_FBARN', 'Det er mangler eller avvik i personopplysninger for fosterbarn' from dual union all
    select 'PERSOPP_MOR', 'Det er mangler eller avvik i personopplysninger for mor' from dual union all
    select 'PERSOPP_PARTNER', 'Det er mangler eller avvik i personopplysninger for partner' from dual union all
    select 'PERSOPP_SAMBO', 'Det er mangler eller avvik i personopplysninger for samboer' from dual union all
    select 'PERSOPP_SOKER', 'Det er mangler eller avvik i personopplysninger for bruker.' from dual union all
    select 'PERSOPP_SOSKEN', 'Det er mangler eller avvik i personopplysninger for søsken' from dual union all
    select 'RETUR_SAMMENSTOT', 'Attesterer har returnert kravet til saksbehandler og status har blitt satt tilbake til behandling.' from dual union all
    select 'REVURD_NY_GML_SATS', 'Kravet kan ikke behandles videre, da bruker eller EPS har en løpende ytelse som ikke er regulert i henhold til nye satser tilgjengelig i PESYS. Kravet må feilregistreres og tas opp til behandling etter regulering.' from dual union all
    select 'REVURD_REDUK', 'Brukeren er registrert med opplysninger i opptjeningsgrunnlaget som indikerer opphør av alderspensjon. Gradsendring må behandles manuelt.' from dual union all
    select 'SAMBOER', 'Det er registrert en samboer på kravet. Dette må verifiseres manuelt.' from dual union all
    select 'SAMBO_TID_EKTE_UKJENT_LENGDE', 'Tidligere ektefelle som bor på samme adresse er død, men lengden på ekteskapet er ikke mulig å utlede' from dual union all
    select 'SAML_UTG_OVER100', 'Bruker har søkt om alderspensjon og har nå en pensjonsgrad over 100%. Saksbehandler må opphøre uførepensjonen fram i tid og innvilge ønsket uttaksgrad på alderspensjon.' from dual union all
    select 'SAMMENST_BLOKK_TILST', 'Status på et eller flere berørte krav har blitt satt til behandling fordi den/de må sammenstøtsberegnes med kravet som er til behandling. Disse må vilkårsprøves på nytt.' from dual union all
    select 'SAMMENST_FEIL_OVERLP', 'Sammenstøt kan ikke vurderes. Antatt virkningsdato på de åpne kravene må tilsvare hverandre. Saksbehandler må feilregistrere et av de åpne kravene. Dette kravet kan igjen opprettes når det andre kravet har blitt iverksatt.' from dual union all
    select 'SAMMENST_FORSKJ_INST', 'Avvik i institusjonsoppholdinformasjon.' from dual union all
    select 'SAMMENST_IKKE_VILK', 'Et eller flere berørte krav er ikke vilkårsprøvd.' from dual union all
    select 'SAMMENST_KRAV_STATUS', 'Ektefelle/partner/samboer har et krav med status som gjør at dette kravet ikke kan sammestøtbehandles.' from dual union all
    select 'SAMMENST_MINKONV', 'Bruker har en minimalkonvertert ytelse.' from dual union all
    select 'SAMMENST_OVERLAP', 'Bruker har overlappende krav. Vurdere hvilke som skal fortsette å løpe.' from dual union all
    select 'SAMMENST_PG_OVER100', 'Bruker har samlet pensjonsgrad over 100% fremover i tid. Saksbehandler må sette ned uttaksgraden på alderspensjonen slik at samlet pensjonsgrad blir 100 % fra første dag i neste måned.' from dual union all
    select 'SAMMENST_SIVST_ULIK', 'Det er avvik i sivilstand på bruker og/eller ektefelle/partner/samboer sin sak.' from dual union all
    select 'SAMMENST_TIL_IVERKS', 'Bruker eller ektefelle/partner/samboer har et vedtak som er til iverksetting. Denne saken må iverksettes før videre behandling kan utføres.' from dual union all
    select 'SAMORDNING', 'Det er feil eller mangler i opplysninger registrert på bruker. Samordning kan ikke iverksettes' from dual union all
    select 'SIVILST', 'Det er mangler eller avvik i sivilstandsopplysninger' from dual union all
    select 'SIVILST_AVDOD', 'Det er mangler eller avvik i sivilstandsopplysninger for avdød' from dual union all
    select 'SIVILST_EKTEF', 'Det er mangler eller avvik i sivilstandsopplysninger for ektefelle' from dual union all
    select 'SIVILST_PARTNER', 'Det er mangler eller avvik i sivilstandsopplysninger for partner' from dual union all
    select 'SIVILST_SAMBO', 'Det er mangler eller avvik i sivilstandsopplysninger for samboer' from dual union all
    select 'SIVILST_SOKER', 'Det er mangler eller avvik i sivilstandsopplysninger for bruker.' from dual union all
    select 'SOKER_INNV_OMST', 'Det er registrert en omstillingsstønad på søker. Kravet må kontrolleres.' from dual union all
    select 'SOKT_AFP', 'Brukeren har søkt om AFP Privat. Varsel må sendes til ordning.' from dual union all
    select 'STOPPET_VEDTAK', 'Omregning kunne ikke gjøres fordi bruker har et vedtak med status "stoppes", "stoppet" eller "reaktiviseres".' from dual union all
    select 'TA_AVDOD', 'Det er mangler eller avvik i trygdeavtaleopplysninger for avdød' from dual union all
    select 'TA_BARN', 'Det er mangler eller avvik i trygdeavtaleopplysninger for barn' from dual union all
    select 'TA_EKTEF', 'Det er mangler eller avvik i trygdeavtaleopplysninger for ektefelle' from dual union all
    select 'TA_FAR', 'Det er mangler eller avvik i trygdeavtaleopplysninger for far' from dual union all
    select 'TA_FBARN', 'Det er mangler eller avvik i trygdeavtaleopplysninger for fosterbarn' from dual union all
    select 'TA_MOR', 'Det er mangler eller avvik i trygdeavtaleopplysninger for mor' from dual union all
    select 'TA_PARTNER', 'Det er mangler eller avvik i trygdeavtaleopplysninger for partner' from dual union all
    select 'TA_SAMBO', 'Det er mangler eller avvik i trygdeavtaleopplysninger for samboer' from dual union all
    select 'TA_SOKER', 'Det er mangler eller avvik i trygdeavtaleopplysninger for bruker' from dual union all
    select 'TA_SOSKEN', 'Det er mangler eller avvik i trygdeavtaleopplysninger for søsken' from dual union all
    select 'TD_ALDER_GJR', 'Gjenlevende har alderspensjon og kan ha rett på gjenlevendetillegg.' from dual union all
    select 'TD_BARN', 'Bruker har barnetillegg for barn som har avgått med døden.' from dual union all
    select 'TD_BT', 'Gjenlevende har ytelse med barnetillegg for felles barn.' from dual union all
    select 'TD_EPS', 'Ukjent feil ved omregning av tilstøtende e/p/s ved dødsfall.' from dual union all
    select 'TD_IFU', 'Automatisk saksbehandling feilet da bruker mangler angitt IFU i vilkårsvurderingen. Saksbehandler må behandle saken og sørge for at det settes riktig angitt IFU for bruker.' from dual union all
    select 'TD_INST', 'Automatisk saksbehandling feilet på grunn av institusjonsopphold på bruker. Behandlingen må gjøres manuelt.' from dual union all
    select 'TD_IVERKS', 'Det finnes allerede et vedtak under iverksettelse.' from dual union all
    select 'TD_OMR', 'Sak kan ikke omregnes automatisk.' from dual union all
    select 'TD_OS_MELD', 'Simulering feiler mot oppdragssystemet.' from dual union all
    select 'TD_OS_NEDE', 'Iverksettes feiler mot oppdragssystemet.' from dual union all
    select 'TD_OVERLAPP', 'Bruker har to løpende folketrygdytelser.' from dual union all
    select 'TD_POPP', 'Bruker finnes ikke i POPP.' from dual union all
    select 'TD_SAMBO', 'Bruker er vurdert som ikke samboer. Saksbehandler må revurdere saken manuelt, og vurdere om bruker må informeres om gjenlevenderettigheter.' from dual union all
    select 'TD_SAMBO_MED_FELLES_BARN', 'Tidligere sambo som har felles barn med avdøde. Vurder om det skal sendes ut informasjon om gjenlevenderettigheter.' from dual union all
    select 'TD_SAMT_BP', 'Samtidig dødsfall: Tilstøtende må informeres om mulig rett til barnepensjon.' from dual union all
    select 'TD_SAMT_GJENL', 'Samtidig dødsfall: Tilstøtende må informeres om mulig rett til gjenlevendepensjon.' from dual union all
    select 'TD_SAMT_OMR', 'Samtidig dødsfall: Berørt sak må omregnes manuelt og bruker må informeres om mulige rett til barnepensjon eller gjenlevendepensjon.' from dual union all
    select 'TD_SJEKK_IEU', 'Bruker har en IFU som er mindre enn 3,5 G og IEU større enn 0.' from dual union all
    select 'TD_SOSKEN', 'Ukjent feil ved omregning av søsken ved dødsfall.' from dual union all
    select 'TD_STOPPET', 'Det finnes stoppet vedtak på saken.' from dual union all
    select 'TD_UNDER_BEHANDLING', 'Informasjonsbrev til gjenlevende er ikke sendt automatisk da krav i forbindelse med dødsfallet allerede er under behandling.' from dual union all
    select 'TD_UTEN_AVDOD', 'Brukers registrerte ektefelle i TPS er død, men avdøde finnes ikke på brukers sak. Vurder omregning og gjenlevenderett.' from dual union all
    select 'TD_UTGJT', 'Bruker var § 3-2 samboer og har gjenlevendetillegg i uføretrygden.' from dual union all
    select 'TD_VERGE', 'Det er registrert mer enn én verge for bruker.' from dual union all
    select 'TIDLIGERE_FORELOP_UP', 'Bruker har eller har hatt en foreløpig uførepensjon i løpet av de siste tre årene. Fellesordningen må informeres om dette.' from dual union all
    select 'TIDLIGERE_OMS', 'Søker har tidligere hatt omstillingsstønad. Søker kan ha rett på gjenlevenderett.' from dual union all
    select 'TID_EKTE', 'Tidligere ektefelle er død og bruker kan ha gjenlevenderettigheter.' from dual union all
    select 'TID_EKTE_UKJENT_LENGDE', 'Tidligere ektefelle er død, men lengden på ekteskapet er ikke mulig å utlede' from dual union all
    select 'TILBAKE_KRAV_ENDR', 'Kravet inngikk i et sammenstøt da det ble gjort endringer i grunnlaget. Vurder om krav på berørte saker må endres' from dual union all
    select 'TILBAKE_KRAV_FEIL', 'Kravet inngikk i et sammenstøt da det ble feilregistrert. Vurder om berørte saker også skal feilregistreres' from dual union all
    select 'TILBAKE_SAMST_ENDR', 'Kravet har fått endret status til Til behandling fordi status på kravet til en berørt sak ble endret' from dual union all
    select 'TILBAKE_SAMST_FEIL', 'Kravet har inngått i et sammenstøt hvor et av de berørte kravene har blitt feilregistrert. Vurder om kravet skal feilregistreres' from dual union all
    select 'TILSTOT_DOD', 'Brukers sak må revurderes fordi brukers ektefelle/partner/samboer som er død.' from dual union all
    select 'TILSTOT_ENDR', 'Ektefelle/partner/samboer sin ytelse har blitt endret på grunn av en sammenstøtsberegning med bruker sak. Det må manuelt fattes vedtak.' from dual union all
    select 'TILSTOT_ENDR_MAN', 'Ektefelle/partner/samboer sin ytelse har blitt endret på grunn av en sammenstøtsberegning med brukers sak. Ektefelle/partner/samboer har tidligere fått sin beregning manuelt overstyrt. Vurdere om beregningen skal endres.' from dual union all
    select 'TILSTOT_MAN', 'Ektefelle/partner/samboer sitt krav kunne ikke beregnes automatisk. Saken må behandles manuelt fra dette steget.' from dual union all
    select 'TILSTOT_TILBAKEKREV', 'Ektefelle/partner/samboer får potensiell tilbakekreving grunnet endringer tilbake i tid for bruker.' from dual union all
    select 'TILST_KRAV_U_VEDT_BT', 'Kravet til ektefelle/partner/samboer må vilkårsprøves før brukers krav kan beregnes.' from dual union all
    select 'TILST_KRAV_U_VEDT_KT', 'Tilstøtende person har ingen sivilstandsendring, men har et åpent krav uten tilhørende vedtak' from dual union all
    select 'TILST_KRAV_V_INK', 'Tilstøtende person har lukket krav men åpent vedtak.' from dual union all
    select 'TRYGDETIDGRNL_BLANKT', 'Trygdetidsgrunnlag er blankt' from dual union all
    select 'TT_AVDOD', 'Trygdetid kunne ikke fastsettes for avdød' from dual union all
    select 'TT_BARN', 'Trygdetid kunne ikke fastsettes for barn' from dual union all
    select 'TT_EKTEF', 'Trygdetid kunne ikke fastsettes for ektefelle' from dual union all
    select 'TT_FAR', 'Trygdetid kunne ikke fastsettes for far' from dual union all
    select 'TT_FBARN', 'Trygdetid kunne ikke fastsettes for fosterbarn' from dual union all
    select 'TT_MOR', 'Trygdetid kunne ikke fastsettes for mor' from dual union all
    select 'TT_PARTNER', 'Trygdetid kunne ikke fastsettes for partner' from dual union all
    select 'TT_PERIODE_AVDOD', 'Det er mangler eller avvik i trygdetidsgrunnlag for avdød' from dual union all
    select 'TT_PERIODE_BARN', 'Det er mangler eller avvik i trygdetidsgrunnlag for barn' from dual union all
    select 'TT_PERIODE_EKTEF', 'Det er mangler eller avvik i trygdetidsgrunnlag for ektefelle' from dual union all
    select 'TT_PERIODE_FAR', 'Det er mangler eller avvik i trygdetidsgrunnlag for far' from dual union all
    select 'TT_PERIODE_FBARN', 'Det er mangler eller avvik i trygdetidsgrunnlag for fosterbarn' from dual union all
    select 'TT_PERIODE_MOR', 'Det er mangler eller avvik i trygdetidsgrunnlag for mor' from dual union all
    select 'TT_PERIODE_PARTNER', 'Det er mangler eller avvik i trygdetidsgrunnlag for partner' from dual union all
    select 'TT_PERIODE_SAMBO', 'Det er mangler eller avvik i trygdetidsgrunnlag for samboer' from dual union all
    select 'TT_PERIODE_SOKER', 'Det er mangler eller avvik i trygdetidsgrunnlag for bruker' from dual union all
    select 'TT_PERIODE_SOSKEN', 'Det er mangler eller avvik i trygdetidsgrunnlag for søsken' from dual union all
    select 'TT_SAMBO', 'Trygdetid kunne ikke fastsettes for samboer' from dual union all
    select 'TT_SOKER', 'Trygdetid kunne ikke fastsettes for bruker' from dual union all
    select 'TT_SOSKEN', 'Trygdetid kunne ikke fastsettes for søsken' from dual union all
    select 'UFOREGR_AVDOD', 'Det er mangler eller avvik i uføredetaljer for avdød.' from dual union all
    select 'UFOREGR_BARN', 'Det er mangler eller avvik i uføredetaljer for barn.' from dual union all
    select 'UFOREGR_EKTEF', 'Det er mangler eller avvik i uføredetaljer for ektefelle.' from dual union all
    select 'UFOREGR_FAR', 'Det er mangler eller avvik i uføredetaljer for far.' from dual union all
    select 'UFOREGR_FBARN', 'Det er mangler eller avvik i uføredetaljer for fosterbarn.' from dual union all
    select 'UFOREGR_MOR', 'Det er mangler eller avvik i uføredetaljer for mor.' from dual union all
    select 'UFOREGR_PARTNER', 'Det er mangler eller avvik i uføredetaljer for partner.' from dual union all
    select 'UFOREGR_SAMBO', 'Det er mangler eller avvik i uføredetaljer for samboer.' from dual union all
    select 'UFOREGR_SOKER', 'Det er mangler eller avvik i uføredetaljer for bruker.' from dual union all
    select 'UFOREGR_SOSKEN', 'Det er mangler eller avvik i uføredetaljer for søsken.' from dual union all
    select 'UTLAND', 'Bruker har hatt perioder med opphold i utland. Utlandsopplysninger skal kontrolleres manuelt' from dual union all
    select 'UTLAND_AVDOD', 'En avdød tilknyttet kravet har hatt perioder med opphold i utland. Utlandsopplysninger skal kontrolleres manuelt.' from dual union all
    select 'UTLAND_F_BH_MED_UTL', 'Bruker har hatt perioder med opphold i avtaleland. Utlandsopplysninger skal kontrolleres manuelt og krav til avtaleland må opprettes.' from dual union all
    select 'UTLAND_UTG', 'Bruker har forsøkt å endre uttaksgrad, men har hatt perioder med opphold i utlandet. Gradsendring må behandles manuelt.' from dual union all
    select 'VARIG_ADSKILT', 'Bruker har angitt at han er varig adskilt fra ektefelle/partner. Sjekk feltet "Vurdert sivilstand" i familieforhold.' from dual union all
    select 'VEDTAKSBREV', 'Det er feil eller mangler i opplysninger registrert på bruker. Vedtaksbrev kan ikke opprettes' from dual union all
    select 'VEDTAKSSTATUS', 'Det er feil eller mangler i opplysninger registrert på bruker. Vedtaksstatus kan ikke oppdateres.' from dual union all
    select 'VEDTAKSTYPE', 'Det er feil eller mangler på opplysninger registrert på bruker. Vedtakstype må manuelt settes til ''ytelse'' eller ''opptjening''' from dual union all
    select 'VEDTAK_TIDL_FAKTOMR', 'Beregningen på forrige vedtak var faktoromregnet eller manuelt overstyrt (kan gjelde både bruker og tilstøtende). Kontroller beregningen.' from dual union all
    select 'VILKAR', 'Automatisk vilkårsprøving kunne ikke gjennomføres og saken må ferdigbehandles manuelt.' from dual union all
    select 'VIRK_FRA_FO_ULIK', 'AFP i privat sektor kan ikke innvilges fordi resultatet fra Fellesordningen er innvilget fra en dato som er ulik kravets ønsket virkdato. Kontakt Fellesordningen eller bruker for avklaring.' from dual union all
    select 'VURDERE_SS', 'Automatisk saksbehandling kunne ikke beregne vedtaket. Saken, samt tilstøtendes saker som inngår i sammenstøtet må ferdigstilles manuelt fra dette steget.' from dual union all
    select 'VURDER_SERSKILT_SATS', 'Automatisk saksbehandling kunne ikke ferdigstille vedtaket. Vurder hvorvidt ytelsen fortsatt skal beregnes med særskilt sats for forsørger.' from dual union all
    select 'VURDSIVILST_EPS', 'Det er mangler eller avvik i vurdert sivilstandsopplysninger for ektefelle/partner/samboer.' from dual union all
    select 'VURDSIVILST_SOKER', 'Det er avvik i vurdert sivilstand mellom brukers saker.' from dual union all
    select 'YTELSE_ANNEN', 'Bruker mottar annen NAV-ytelse som ikke kan løpe samtidig som kravet som er til behandling. Det må vurderes hvilke ytelser som skal fortsette å løpe.' from dual union all
    select 'YTELSE_PEN', 'Bruker eller ektefelle/partner/samboer mottar andre pensjonsytelser som ikke kan løpe samtidig med kravet som er til behandling. Det må vurderes hvilke ytelser som skal fortsette å løpe.' from dual union all
    select 'YTELSE_PEN_MOTTAKER', 'Mottaker av omsorgspoeng har en løpende pensjonsytelse. Det må vurderes om omsorgspoengene påvirker størrelsen på denne ytelsen.' from dual
),
-- Kravårsak-mapping definert i kravarsak_dim.sql (gjenbrukbar)
kravarsak_map as (
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
behandling_t_map as (
    select 'AUTO' as k_behandling_t, 'Automatisk' as dekode from dual union all
    select 'DEL_AUTO', 'Del-automatisk' from dual union all
    select 'MAN', 'Manuell' from dual
),
base as (
    select
        trunc(kp.dato_opprettet) as dato,
        coalesce(dkh.dekode, kh.k_krav_gjelder) as kravtype,
        (case when substr(kh.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
        case when substr(kh.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(kh.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
        kh.opprettet_av end end ) as behandler,
        coalesce(ds.dekode, s.k_sak_t) as sakstype,
        coalesce(dks.dekode, kh.k_krav_s) as kravstatus,
        dkb.dekode as behandlingstype,
        kp.k_kontrollpnkt_t as kontrollpunkt,
        coalesce(kam.dekode, arsak.k_krav_arsak_t) as kravarsak,
        coalesce(dkp.dekode_tekst, kp.k_kontrollpnkt_t) as kontrollpunkt_forklaring

    from pen.t_kontrollpunkt kp
    inner join pen.t_kravhode kh on kh.kravhode_id = kp.kravhode_id
    inner join pen.t_sak s on s.sak_id = kp.sak_id
    left join krav_gjelder_map dkh on dkh.k_krav_gjelder = kh.k_krav_gjelder
    left join sak_t_map ds on ds.k_sak_t = s.k_sak_t
    left join kontrollpnkt_map dkp on dkp.k_kontrollpnkt_t = kp.k_kontrollpnkt_t
    left join krav_s_map dks on dks.k_krav_s = kh.k_krav_s
    left join behandling_t_map dkb on dkb.k_behandling_t = kh.k_behandling_t
    inner join pen.t_krav_arsak arsak on arsak.kravhode_id = kh.kravhode_id
    left join kravarsak_map kam on kam.k_krav_arsak_t = arsak.k_krav_arsak_t

    where trunc(kp.dato_opprettet) < trunc(current_date)
)
select
    dato,
    kravtype,
    behandler,
    sakstype,
    kravstatus,
    behandlingstype,
    kontrollpunkt,
    kravarsak,
    kontrollpunkt_forklaring,
    count(*) as antall
from base

group by
    dato,
    kravtype,
    behandler,
    sakstype,
    kravstatus,
    behandlingstype,
    kontrollpunkt,
    kravarsak,
    kontrollpunkt_forklaring
