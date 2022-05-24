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
