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
    return cx_Oracle.connect(user=os.environ["PEN_USER"], password=os.environ["PEN_PASSWORD"], dsn=dsnStr)

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