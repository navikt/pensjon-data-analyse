import pandas as pd
from google.cloud import bigquery


# hent ut to tabeller fra BiqQuery:
# 1. identer, område (alle PO)
# 2. identer, team, område (kun PO pensjon)

sql_base = """
with

po as (
    select 
        concat('PO ', substr(name, 15)) as po_navn,
        id as po_id
    from org-prod-1016.teamkatalogen_federated_query_updated_dataset.Produktomraader
    where status = 'ACTIVE'
    and substr(name, 1, 13) = 'Produktområde'
),

-- har duplikater innad i PO der det er en person er i flere team
team_i_po as (
    select 
        name as team_navn,
        -- productareaid as po_id,
        po.po_navn as po_navn,
        json_extract_scalar(member, '$.navIdent') as nav_id,
        lower(json_extract_scalar(member, '$.roles[0]')) as rolle,
        lower(json_extract_scalar(member, '$.roles[1]')) as rolle_ekstra,
    from
        org-prod-1016.teamkatalogen_federated_query_updated_dataset.Teams as teams,
        unnest(json_extract_array(teams.members)) as member
        inner join po on teams.productareaid = po.po_id
        where teams.status = 'ACTIVE'
),

-- én rad per person i PO. Velger første rolle, som er litt tilfeldig
unike_identer_i_po as (
    select
        nav_id,
        po_navn,
        rolle
    from (
        select
            nav_id,
            po_navn,
            rolle,
            row_number() over (partition by nav_id order by po_navn, rolle) as rn
        from team_i_po
        )
    where rn = 1
)

"""

sql_pensjon_teams = sql_base + """select * from team_i_po where po_navn = 'PO pensjon'"""
sql_alle_po = sql_base + """select * from unike_identer_i_po"""

# henter data fra BigQuery
client = bigquery.Client(project='pensjon-saksbehandli-prod-1f83')
df_teams = client.query(sql_pensjon_teams).to_dataframe()
df_alle_po = client.query(sql_alle_po).to_dataframe()


# send 1 og 2 inn i en sqlspørring mot oracle  dt_hr.hrres_ressurs_lav, som aggregerer

# select
#     remedy_enhet, -- IT-avdelingen, IT-avdelingen
#     orgenhet_paa_ressurs_beskrivelse, -- Data 3, IT utvikling 3
#     -- den over kan byttes med ORGANISASJONSKNYTNING_BESK, hvis den gir bedre verdier
#     stillingsnavn, -- Rådgiver, Ekstern
#     ansettelsestype_besk, -- Fast, Ekstern
#     lederniva, -- usikker på om denne er interessant
#     --fodselsdato, -- for alder
#     kjonn, -- Mann
#     case 
#         when floor(months_between(sysdate, fodselsdato) / 12) < 35 then 'under 35'
#         when floor(months_between(sysdate, fodselsdato) / 12) between 35 and 50 then '35-50'
#         else 'over 50'
#     end as aldersgruppe
# from dt_hr.hrres_ressurs_lav