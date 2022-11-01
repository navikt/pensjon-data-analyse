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