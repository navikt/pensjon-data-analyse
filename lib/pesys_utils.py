import cx_Oracle
import os


stonads_mapper = {
    "AFP": "AFP",
    "AFP_PRIVAT": "AFP Privat",
    "ALDER": "Alderspensjon",
    "BARNEP": "Barnepensjon",
    "FAM_PL": "Familiepleierytelse",
    "GAM_YRK": "Gammel yrkesskade",
    "GENRL": "Generell",
    "GJENLEV": "Gjenlevendeytelse",
    "GRBL": "Grunnblanketter",
    "KRIGSP": "Krigspensjon",
    "OMSORG": "Omsorgsopptjening",
    "UFOREP": "Uføretrygd"
}

forstegang = {"Førstegangsbehandling",
             "Førstegangsbehandling Norge/utland", 
             "Førstegangsbehandling bosatt utland",
             "Førstegangsbehandling kun utland"}

# Kravtyper hvor ledetid er interessant - kan være noen som mangler her
fra_bruker = {"Førstegangsbehandling", # Ta bort aldersovergang?
             "Førstegangsbehandling Norge/utland", 
             "Førstegangsbehandling bosatt utland",
             "Førstegangsbehandling kun utland",
             "Endring uttaksgrad",
             "Søknad om økning av uføregrad",
             "Søknad om reduksjon av uføregrad",
             "Klage",
             "Søknad om yrkesskade",
             "Søknad ung ufør"}


def open_pen_connection():
    ORACLE_HOST = '10.53.136.15'
    ORACLE_PORT = '1521'
    ORACLE_SERVICE = 'pen'
    dsnStr = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
    con = cx_Oracle.connect(user=os.environ["PEN_USER"], password=os.environ["PEN_PASSWORD"], dsn=dsnStr)
    return con


def open_popp_connection():
    ORACLE_HOST = 'dm09-scan.adeo.no'
    ORACLE_PORT = '1521'
    ORACLE_SERVICE = 'popp'
    dsnStr = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
    con = cx_Oracle.connect(user=os.environ["POPP_USER"], password=os.environ["POPP_PASSWORD"], dsn=dsnStr)
    return con


def map_stonad(kode):
    return stonads_mapper[kode]


def fjern_sjeldne_stonader(row):
    sjeldne_stonader = {"Krigspensjon", "Familiepleierytelse", "Gammel yrkesskade"}
    if row["STØNADSOMRÅDE"] in sjeldne_stonader:
        return 
    else:
        return row


def add_zero_to_mnd(x: str):
    if len(x) == 2:
        return x
    elif len(x) == 1:
        return '0' + x
    else:
        raise Exception(f"Wrong format on 'MAANED': {x}")
        
        
def add_zero_to_aar_mnd(x: str):
    if len(x) == 7:
        return x
    elif len(x) == 6:
        return x[:5] + '0' + x[5:]
    else:
        raise Exception(f"Wrong format on 'AAR_MAANED': {x}")


oppgavetekst_lang = {
    "SOKNAD_AP_PREFIKS": "Har ingen verdi, men det trengs et innslag for å unngå at default prefiks benyttes",
    "OPPG_DEL_AUTO_UTLAND": "Del-automatisk krav Utlandsopphold: Bruker er registrert med utlandsopphold, dette må kontrolleres.<br>Kravet har derfor gått til del-automatisk behandling.<br>Kontroller øvrige kontrollpunkt.",
    "OPPG_DEL_AUTO_UTLAND_F_BH_MED_UTL": "Del-automatisk krav Avtaleland: Bruker har bodd/arbeidet i avtaleland, det må opprettes krav med tilhørende kravblanketter til avtaleland.<br>Kravet har derfor gått til del-automatisk behandling.<br>Kontroller øvrige kontrollpunkt.",
    "OPPG_DEL_AUTO_SAMBOER": "Del-automatisk krav samboer: Bruker oppgir sivilstand samboer, dette må kontrolleres manuelt.<br>Kravet har derfor gått til del-automatisk behandling.",
    "OPPG_EPS_INNTEKT_2G": "Registrert inntekt på EPS er under 2G.<br>Inntektene må undersøkes manuelt.",
    "OPPG_ALDER_GJR": "Sjekk om bruker har søkt om gjenlevenderett.",
    "OPPG_APENT_KRAV_OMR": "Dersom bruker har et åpent krav oppdateres kravoppgaven med informasjon om at bruker har vært forsøkt behandlet av en automatisk jobb.",
    "OPPG_DOD_DATO_FREM_I_TID": "Dødsdato satt frem i tid.<br>Vurder om ytelsen skal midlertidig stanses.<br>Saken må meldes brukerstøtte umiddelbart.",
    "OPPG_DOD_IVERKS": "Bruker har krav til iverksettelse som ikke kan avsluttes automatisk.<br>Kravet må iverksettes før det opphøres manuelt.",
    "OPPG_DOD_KRG_GYRK": "Bruker er meldt død.<br>Krav må revurderes og opphøres manuelt.",
    "OPPG_DOD_STOPP": "Brukers ytelse er midlertidig stanset og lar seg ikke opphøre automatisk.<br>Saksbehandler må sørge for manuelt opphør av ytelsen.",
    "OPPG_DOD_TIL_BEH": "Bruker har krav til behandling som ikke kan avsluttes automatisk.<br>Kravet må ferdigbehandles, eventuelt feilregistreres, før det opphøres manuelt.",
    "OPPG_BARN_DOD": "Bruker har barnetillegg for barn som har gått bort.<br>Kravet må revurderes og opphøres manuelt.",
    "OPPG_BARN_DOD_I_EO_AR_ELLER_ARET_ETTER": "Forsørget barn døde i løpet av etteroppgjørsåret eller året etter.<br>Etteroppgjøret må behandles manuelt.",
    "OPPG_DEFAULT_APENT_KRAV": "Manuell behandling pga åpent krav.<br>Brukers ytelse ble forsøkt omregnet fra: <DATO VIRK FOM OPPGAVE>.<br>Vurder om virkningstidspunktet på det åpne kravet skal endres, eventuelt behandle ferdig det åpne kravet og foreta omregning manuelt.",
    "OPPG_DEFAULT_FAKTOR": "Manuell behandling pga faktoromregnet vedtak.<br>Brukers ytelse ble forsøkt omregnet fra: <DATO VIRK FOM OPPGAVE>.<br>Kompletter grunnlaget og foreta omregning manuelt.",
    "OPPG_DEFAULT_FEIL": "Manuell behandling pga automatisk omregning feilet.<br>Brukers ytelse ble forsøkt omregnet fra: <DATO VIRK FOM OPPGAVE>.<br>Foreta omregning manuelt.",
    "OPPG_DEFAULT_IKKE_REKKEFOLGE": "Manuell behandling pga rekkefølgen av omregningen av brukers og E/P/S ytelse ikke kunne fastsettes.<br>Brukers ytelse ble forsøkt omregnet fra: <DATO VIRK FOM OPPGAVE>.<br>Foreta omregningene manuelt.",
    "OPPG_DEFAULT_MAN_BER": "Manuell behandling pga manuelt overstyrt beregning.<br>Brukers ytelse ble forsøkt omregnet fra: <DATO VIRK FOM OPPGAVE>.<br>Kontroller først om manuell overstyring er påkrevet.<br>Foreta deretter omregning manuelt og kontroller beregning nøye.",
    "OPPG_DEFAULT_MINKONV": "Manuell behandling pga minimalkonvertert ytelse.<br>Brukers ytelse ble forsøkt omregnet fra: <DATO VIRK FOM OPPGAVE>.<br>Kompletter grunnlaget og foreta omregning manuelt.",
    "OPPG_DEFAULT_NY_GML_SATS": "Manuell behandling pga at ytelsen ikke er regulert.<br>Brukers ytelse ble forsøkt omregnet fra: <DATO VIRK FOM OPPGAVE>.<br>Reguler ytelsen og kontroller om beregningen er korrekt etter regulering.<br>Foreta omregning manuelt dersom nødvendig.",
    "OPPG_DEFAULT_STOPP": "Manuell behandling pga et vedtak med status stoppes/stoppet/reaktiviseres.<br>Brukers ytelse ble forsøkt omregnet fra: <DATO VIRK FOM OPPGAVE>.<br>Reaktiver vedtak og omregn ytelsen manuelt.",
    "OPPG_DEFAULT_TIL_IVERKS": "Manuell behandling pga et vedtak til samordning eller iverksettelse.<br>Brukers ytelse ble forsøkt omregnet fra: <DATO VIRK FOM OPPGAVE>.<br>Revurder vedtaket når status er endret til iverksatt.",
    "OPPG_DEFAULT_TOLERANSEGRENSE": "Manuell behandling pga endring etter omregning er utenfor fastsatt beløp- eller prosentgrense.<br>Brukers ytelse ble forsøkt omregnet fra: <DATO VIRK FOM OPPGAVE>.<br>Kontroller grunnlaget og omregn ytelsen manuelt.",
    "OPPG_EPS_DOD_I_EO_AR_ELLER_ARET_ETTER": "Annen forelder døde i løpet av etteroppgjørsåret eller året etter.<br>Etteroppgjøret må behandles manuelt.",
    "OPPG_IE_BRUKER_MA_BEH_ETTER_EPS": "Inntektsendring.<br>Automatisk behandling av berørt sak feilet.<br>Kravet må behandles manuelt.",
    "OPPG_IE_KONTROLLPUNKT": "Inntektsendring.<br>Automatisk saksbehandling feilet.<br>Kravet må behandles manuelt.",
    "OPPG_BEH_INNT_ENDR": "Inntekt endret i Inntektsplanleggeren som medfører nettoutbetaling endret, samtidig som bruker har åpent og potensielt løpende krav på uføresak.",
    "OPPG_BEH_INNT_OPP": "Forventet inntekt må oppdateres med virkningsdato 1.<br>januar neste år.<br>Det må registreres et beløp eller null på alle inntektstyper hvor det er registrert et beløp i inneværende år.",
    "OPPG_KONTROLLPUNKT": "Automatisk saksbehandling avbrutt: Kontrollpunkt er knyttet til kravet.<br>Gå til kravkontroll.",
    "OPPG_OMR_MA_REBEREGNES": "Denne oppgaven opprettes dersom vedtaket har status ”Til iverksettelse”, ”Til samordning” eller ”Samordnet”",
    "OPPG_OMR_MAN_BER": "Dersom et vedtak har en manuelt overstyrt beregning",
    "OPPG_OMR_FAKTOR": "Dersom brukers vedtak har vært faktoromregnet",
    "OPPG_OMR_KONVMIN": "Dersom det løpende vedtaket har et minimalkonvertert krav",
    "OPPG_OMR_UP_LOP": "Dersom opptjeningsendringen påvirker en løpende uførepensjonsytelse som ikke kan omregnes automatisk",
    "OPPG_OMR_UP_AVSL": "Dersom opptjeningsendringen påvirker en tidligere uførepensjonsytelse som nå er avsluttet",
    "OPPG_OMR_AFP_AVSL": "Dersom opptjeningsendringen påvirker en tidligere AFP-ytelse som nå er avsluttet",
    "OPPG_OMR_MGLR_TT": "Dersom brukeren mangler trygdetidsgrunnlag",
    "OPPG_OMR_TT_FEIL": "Dersom trygdetiden ble redusert eller økt med mer enn ett år.",
    "OPPG_OKNING_INNTEKTENDR": "Dersom bruker får utbetalt et høyere beløp etter omregning.<br>Oppgaven opprettes for at saksbehandles skal vurdere etterbetaling.",
    "OPPG_OMR_FEIL": "Omregning av bruker feiler",
    "OPPG_OMR_FLR_VEDTAK": "Bruker har flere vedtak på samme sak",
    "OPPG_OMR_KONV_1967": "",
    "OPPG_OMR_VURD_UTG": "Saksbehandler må sende ut varselbrev til bruker og finne nærmeste lovlige uttaksgrad.",
    "OPPG_OMR_VARS_BRV": "Saksbehandler må sende ut varselbrev til bruker.",
    "OPPG_OMR_BEH_RED": "Den første av to kravbehandleringer resulterte i et åpent krav og derfor må begge kravbehandlingene gjøres manuelt.",
    "OPPG_OMR_UTVANDRET": "Bruker har utvandret til utlandet",
    "OPPG_OMR_PP_UTVANDRET": "Bruker har bodd i utlandet i hele opptjeningsaret og har inntekt > 1G eller omsorg.",
    "OPPG_OMR_OPPH_UP": "Bruker får en endring i opptjening som påvirker en avsluttet UP.",
    "OPPG_OMR_OPPH_AFP": "Bruker får en endring i opptjening som påvirker en avsluttet AFP.",
    "OPPG_OMR_UP_TILBAKE": "Brukers uførepensjon skal reberegnes fra på grunn av endring i registrert opptjening.<br>Dette kunne ikke gjøres automatisk da endringen gjelder før 01.01.2009 og/eller det var vært gradsendringer på brukers sak.<br>Brukers uførepensjon må reberegnes manuelt.",
    "OPPG_OMR_MOTREGN": "Brukers uføreytelse skal reberegnes fra på grunn av endring i registerert opptjening.<br>Saksbehandler må reberegne brukers uføreytelse manuelt og legge inn postering for å ta høyde for motregninger mot sykepenger eller arbeidsavklaringspenger.",
    "OPPG_OMR_UFOREP_REDUSERT_BELOP": "Oppgave dersom man finner at et krav med virkfom tilbake i tid innebærer en reduksjon i ytelse sammenlignet med gjeldende vedtak.",
    "OPPG_OMR_EPS_AFP": "Manuell behandling da eps har afp kommunalt.",
    "OPPG_SJEKK_IEU": "Bruker har en IFU som er mindre enn 3,5 G, og EPS har avgått med døden.<br>Saksbehandler må revurdere saken og sjekke at IEU blir riktig.",
    "OPPG_SJEKK_IFU": "Automatisk saksbehandling feilet da bruker mangler angitt IFU i vilkårsvurderingen.<br>Saksbehandler må behandle saken og sørge for at det settes riktig angitt IFU for bruker.",
    "OPPG_TD_BT": "Bruker har ytelse med barnetillegg for felles barn og E/P/S har avgått med døden.<br>Saksbehandler må revurdere saken.",
    "OPPG_TD_IVERKS": "Automatisk saksbehandling feilet da det allerede finnes et vedtak knyttet til bruker som er under iverksettelse.<br>Saksbehandler må kontrollere og iverksette dette vedtaket.",
    "OPPG_TD_OMR": "Automatisk saksbehandling feilet ved omregning av brukers ytelse.<br>Behandling må gjøres manuelt.<br>Gå til kravkontroll for å se kontrollpunkter.",
    "OPPG_TD_OS_MELD": "Automatisk saksbehandling feilet på grunn av manglende kommunikasjon med Oppdragssytemet.<br>Saksbehandler må revurdere og iverksette saken på nytt.",
    "OPPG_TD_OS_NEDE": "Automatisk saksbehandling feilet på grunn av manglende kommunikasjon med Oppdragssytemet.<br>Saksbehandler må revurdere og iverksette saken på nytt.",
    "OPPG_TD_OVERLAPP": "Automatisk saksbehandling feilet ved attestering fordi bruker allerede har en løpende ytelse i samme periode.<br>Saksbehandler må behandle saken og sørge for at ytelsene ikke overlapper.",
    "OPPG_TD_POPP": "Automatisk saksbehandling feilet fordi bruker ikke finnes i Opptjeningsregisteret.<br>Saksbehandler må sjekke om fødselsnummeret har blitt endret.",
    "OPPG_TD_SAMT_BP": "Samtidig dødsfall i familierelasjon.<br>Saksbehandler må vurdere om bruker skal informeres om barnepensjon.",
    "OPPG_TD_SAMT_GJENL": "Samtidig dødsfall i familierelasjon.<br>Saksbehandler må vurdere om bruker skal informeres om gjenlevendepensjon.",
    "OPPG_TD_SAMT_OMR": "Samtidig dødsfall i familierelasjon.<br>Saksbehandler må sjekke om ytelsen må omregnes, og vurdere om det skal informeres om gjenlevenderettigheter.",
    "OPPG_TD_VERGE": "Automatisk saksbehandling feilet på grunn av overlappende perioder i vergeinformasjon.<br>Behandlingen må gjøres manuelt og det må tas stilling til hvem som er verge til bruker.",
    "OPPG_TD_UTGJT": "Brukers § 3-2 samboer har avgått med døden og bruker har gjenlevendetillegg i uføretrygden.<br>Saksbehandler må revurdere saken.",
    "OPPG_TRYGDETID_GAMMEL": "Dersom bruker mottar en gjenlevendeytelse og det mangler trygdetidsgrunnlag på kravet",
    "OPPG_UTEO_VURDERING": "PK-11043 Eventuelle åpne krav må ferdigstilles",
    "OPPG_UTEO_MAN_OVER_FAKTOR": "Uføretrygden er faktoromregnet eller manuelt beregnet.<br>Etteroppgjøret må behandles manuelt.",
    "OPPG_UTEO_APENT_KRAV": "Etteroppgjørsbehandlingen kan ikke utføres på grunn av at saken har et åpent krav.",
    "OPPG_UTEO_AUTO_FEIL": "Automatisk etteroppgjør feilet.<br>Etteroppgjøret må behandles manuelt.",
    "OPPG_UTEO_APENT_TILBKR": "Etteroppgjørsbehandling kan ikke utføres på grunn av at saken har et åpent tilbakekrevingskrav.<br>Vurder om kravet får innvirkning på etteroppgjøret.",
    "OPPG_UTEO_HVILENDE_RETT": "Bruker har hvilende rett.<br>Avklar om hvilende rett skal videreføres.",
    "OPPG_UTEO_INNT_OVER_TAK": "Brukers inntekt har oversteget 80 % av oppjustert IFU i etteroppgjørsåret.<br>Arbeidsforsøk må vurderes.",
    "OPPG_UTEO_SVAR_MOTTATT": "Tilsvar på etteroppgjøret mottatt.<br>Etteroppgjøret må behandles manuelt.",
    "OPPG_UTEO_FLERE_AR": "Etteroppgjøret kunne ikke behandles automatisk fordi det er mottatt lignede inntekter for flere år på bruker.<br>Etteroppgjørskravene må behandles i kronologisk rekkefølge hvor det eldste året tas først.",
    "OPPG_UTEO_EPS_IKKE_FULL_MAN": "Etteroppgjøret kan ikke utføres før etteroppgjøret til e/p/s er fullført.<br>Kravet må behandles manuelt.",
    "OPPG_UTEO_EPS_IKKE_I_POPP": "Etteroppgjøret må behandles manuelt på grunn av mangelfulle opplysninger om e/p/s.",
    "OPPG_UTEO_IKKE_SISTE_AAR": "Brukers pensjonsgivende inntekt er endret.<br>Endringer gjelder år som er ulikt siste etteroppgjørsår.<br>Nytt etteroppgjør må behandles manuelt.",
    "OPPG_UTEO_FLERE_EPS": "Brukeren har mottatt barnetillegg for fellesbarn med ulike e/p/s i løpet av året.<br>Etteroppgjøret må behandles manuelt.",
    "OPPG_UTEO_EPS_PGI_IKKE_KLAR": "Det er ikke mottatt opplysninger om e/p/s sin pensjonsgivende inntekt.<br>Brukers etteroppgjør må behandles manuelt.",
    "OPPG_UTEO_AVBRYT_VENTEPERIODE": "Automatisk etteroppgjørsbehandling er avbrutt.<br>Gå til Pensjonsoversikt for å ferdigbehandle etteroppgjøret manuelt.",
    "OPPG_UTEO_MANUELT_JOURNALFORT": "Tilsvar på etteroppgjøret mottatt.<br>Etteroppgjøret må behandles manuelt.",
    "OPPG_UTEO_ETTERBET_IKKE_RED_I_EO_AR": "Brukeren har mottatt en etterbetaling fra NAV i etteroppgjørsåret, og det er allerede registrert et fradrag på saken.<br>Vurder om etterbetalingen fra NAV skal registreres som et nytt fradrag.",
    "OPPG_UTEO_ETTERBET_OPPTJENT_I_EO_AR": "Brukeren er innvilget uføretrygd i etteroppgjørsåret, og har mottatt en etterbetaling fra NAV.<br>Merk at deler av etterbetalingen allerede kan være registrert som et fradrag av typen 'Innrapportert arbeidsinntekt' med årsak 'Opptjent før innvilgelse UT'.<br>Vurder hvilket beløp som skal registreres som 'Inntekt til fradrag' med årsak 'Etterbetaling fra NAV'.",
    "OPPG_UTEO_MANUELLE_FRATREKK": "Etteroppgjør er tidligere foretatt med manuelt registrerte fradrag.<br>Nytt etteroppgjør må behandles manuelt med kontroll av fradrag.",
    "OPPG_YT_OPPH": "Dersom det er satt en TOM dato på vedtaket der TOM datoen er i behandlingsmåneden",
    "OPPG_DEFAULT_UTLAND_PENSJON": "Ytelse fra avtaleland.<br>Folketrygdytelsen har blitt omregnet.<br>Informer trygdemyndighetene i avtalelandet om nytt beløp",
    "OPPG_DOD_DEFAULT": "Det har skjedd en feil i den automatiske prosessen.<br>Saksbehandler må sjekke saken, eventuelt sørge for manuelt opphør av ytelsen.",
    "OPPG_DOD_DATO_FEIL": "Dødsdato er endret.<br>Saksbehandler må sjekke opphørsvedtaket mot dødsdato, eventuelt sørge for manuelt opphør av ytelsen.<br>Sjekk eventuelle tilstøtende vedtak.",
    "OPPG_DOD_ANNULLERT": "Dødsdato er annullert fordi bruker var feilaktig meldt død.<br>Saksbehandler må sjekke om bruker hadde ytelser som må iverksettes på nytt, og sørge for at adressen er registrert i Pesys.<br>Sjekk eventuelle tilstøtende vedtak.",
    "OPPG_DOD_UTLAND": "Avdød bruker er registrert som mottaker av pensjon fra utlandet.<br>Utenlandske trygdemyndigheter må orienteres om dødsfallet (ikke nødvendig hvis Norge har elektronisk utveksling av dødsmeldinger med utbetalingslandet).",
    "OPPG_BREV_ADR": "Informasjonsbrev om gjenlevenderettigheter er ikke sendt ut på grunn av manglende adresse.<br>Saksbehandler må sørge for å sende dette manuelt.",
    "OPPG_BREV_ANNET": "Automatisk utsending av informasjonsbrev om gjenlevenderettigheter feiler.<br>Saksbehandler må sørge for å sende dette manuelt.",
    "OPPG_BREV_BARN18": "Barn uten foreldre.<br>Saksbehandler må sørge for å sende informasjonsbrev manuelt.",
    "OPPG_BREV_BARN21": "Barn mellom 18 og 21 år.<br>Saksbehandler må vurdere om barn har utvidet rett til barnepensjon og eventuelt sende informasjonsbrev.",
    "OPPG_BREV_SAMBO": "Bruker er vurdert som ikke samboer.<br>Saksbehandler må vurdere om det skal informeres om gjenlevenderettigheter.",
    "OPPG_BRUKER_UT_GJT": "Manuell behandling pga.<br>tidligere gjenlevenderett.<br>Gjenlevenderett i alderspensjon må vurderes av saksbehandler.",
    "OPPG_GT_BREV_EPS": "Gjenlevende er over 67 år og har ikke tatt ut alderspensjon.<br>Bruker kan ha rett på gjenlevendetillegg i alderspensjonen.<br>Saksbehandler må vurdere å informere bruker om rettigheter.",
    "OPPG_AVDOD_UTV_DNR": "Avdød er utvandret eller har d-nummer.<br>Sjekk familierelasjoner i Pesys og send eventuelt informasjonsbrev til gjenlevende/barn manuelt.",
    "OPPG_TD_INST": "Automatisk saksbehandling feilet på grunn av institusjonsopphold på bruker.<br>Saksbehandler må revurdere saken manuelt.",
    "OPPG_TD_SAMBO": "Bruker er vurdert som ikke samboer.<br>Saksbehandler må revurdere saken manuelt, og vurdere om bruker må informeres om gjenlevenderettigheter.",
    "OPPG_TD_UTEN_AVDOD": "Brukers registrerte ektefelle i TPS er død, men avdøde finnes ikke på brukers sak.<br>Vurder omregning og gjenlevenderett.",
    "OPPG_TD_UNDER_BEHANDLING": "Informasjonsbrev til gjenlevende er ikke sendt automatisk da krav i forbindelse med dødsfallet allerede er under behandling.",
    "OPPG_BARN_MULIG_BP": "(fnr) døde (dødsdato).<br>Saksbehandler må vurdere om barn har rett til barnepensjon.",
    "OPPG_EPS_MULIG_GLP": "(fnr) døde (dødsdato).<br>Saksbehandler må vurdere om gjenlevende har rettigheter.",
    "OPPG_TID_EKTE": "Tidligere ektefelle er død og bruker kan ha gjenlevenderettigheter.",
    "OPPG_TID_EKTE_USIKKER_LENGDE": "Tidligere ektefelle er død, men lengden på ekteskapet er ikke mulig å utlede.<br>Saksbehandler må vurdere lengden på ekteskapet og eventuelt sende informasjonsbrev til gjenlevende.",
    "OPPG_SAMBO_TID_EKTE_USIKKER_LENGDE": "Tidligere ektefelle som bor på samme adresse er død, men lengden på ekteskapet er ikke mulig å utlede.<br>Saksbehandler må vurdere lengden på ekteskapet og eventuelt sende informasjonsbrev til gjenlevende.",
    "OPPG_OPPH_AO_FAKTOR": "Brukers ytelse kunne ikke omregnes eller konverteres automatisk fordi brukers vedtak er faktoromregnet Kompletter grunnlaget og fullfør behandlingen manuelt.",
    "OPPG_OPPH_APENT_KRAV_KONV": "Brukers ytelse skal konverteres til alderspensjon men dette kan ikke gjøres av automatisk prosess ettersom bruker har et åpent krav.",
    "OPPG_OPPH_AO_MAN_BER": "Omregning eller konvertering av brukers ytelse kan ikke gjøres fordi bruker har en manuelt overstyrt beregning.<br>Behandlingen må gjøres manuelt.",
    "OPPG_AO_KONVERTERT_UP_MED_FREMTIDIG_TRYGDETID": "Vurder trygdetid.<br>Bruker hadde konvertert UP med avkortet fremtidig trygdetid.<br>Vurder om alderspensjon skal beregnes med annen trygdetid.",
    "OPPG_AO_KONVERTERT_UFT_MED_AVKORTET_TILLEGGSPENSJON": "Uføre må opphøres.<br>Bruker hadde UP beregnet med avkortet tilleggspensjon.<br>Vurder om alderspensjon skal beregnes med avkortet TP.",
    "OPPG_AO_AVKORTET_FREMTIDIG_TRYGDETID": "Uføre må opphøres.<br>Bruker hadde UT beregnet med avkortet fremtidig trygdetid.<br>Vurder om det er grunnpensjon eller tilleggspensjon som skal avkortes i alderspensjonen.<br>Kontrollerer om bruker har opparbeidet seg mer trygdetid.",
    "OPPG_UTL_APENT_KRAV": "Kravet kunne ikke opprettes automatisk fordi bruker har krav til behandling.",
    "OPPG_UTL_HAR_AP": "Kravet kunne ikke opprettes automatisk.<br>Bruker har løpende alderspensjon eller åpent krav om alderspensjon til behandling.",
    "OPPG_UTL_GNRL_MANGLER": "Automatisk opprettelse av krav feilet som følge av mangler i grunnlagsinformasjon.<br>Søknaden må behandles manuelt.",
    "OPPG_UTL_AP": "Søknad om alderspensjon.",
    "OPPG_UTL_UT": "Søknad om uføretrygd.",
    "OPPG_UTL_GENERELL": "Pesys kunne ikke håndtere hendelsen automatisk.<br>Saksbehandler må vurdere behov for videre behandling.",
    "OPPG_INFO_FRA_UTLAND": "",
    "OPPG_UKJENT_HENDELSE": ""
}