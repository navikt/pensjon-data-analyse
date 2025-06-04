import pandas as pd
from google.cloud import bigquery


# hent ut to tabeller fra BiqQuery:
# 1. identer, team, område, rolle, rolle2, (kun PO pensjon)
# 2. identer, område, rolle (alle PO)

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
        name as team,
        -- productareaid as po_id,
        po.po_navn as po_navn,
        json_extract_scalar(member, '$.navIdent') as nav_id,
        lower(json_extract_scalar(member, '$.roles[0]')) as rolle,
        lower(json_extract_scalar(member, '$.roles[1]')) as rolle2
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
),

po_pensjon as (
    select
        nav_id,
        team,
        po_navn,
        rolle,
        rolle2
    from team_i_po
    where po_navn = 'PO pensjon'
)

"""

sql_pensjon_teams = sql_base + """select * from po_pensjon"""
sql_alle_po = sql_base + """select * from unike_identer_i_po"""

# henter data fra BigQuery
client = bigquery.Client(project="pensjon-saksbehandli-prod-1f83")
df_pensjon_teams = client.query(sql_pensjon_teams).to_dataframe()
df_alle_po = client.query(sql_alle_po).to_dataframe()


# send 1 og 2 inn i en sqlspørring mot oracle  dt_hr.hrres_ressurs_lav, som aggregerer
# select
#     remedy_enhet, -- IT-avdelingen, IT-avdelingen
#     orgenhet_paa_ressurs_beskrivelse, -- Data 3, IT utvikling 3
#     -- den over kan byttes med ORGANISASJONSKNYTNING_BESK, hvis den gir bedre verdier
#     stillingsnavn, -- Rådgiver, Ekstern
#     ansettelsestype_besk, -- Fast, Ekstern
#     --lederniva, -- usikker på om denne er interessant
#     --fodselsdato, -- for alder
#     kjonn, -- Mann
#     case
#         when floor(months_between(sysdate, fodselsdato) / 12) < 35 then 'under 35'
#         when floor(months_between(sysdate, fodselsdato) / 12) between 35 and 50 then '35-50'
#         else 'over 50'
#     end as aldersgruppe
# from dt_hr.hrres_ressurs_lav


#
# # oracle

oracle_sql_pensjon = f"""
with

hr_data as (
    select
        nav_id, -- for join mot data fra bq
        remedy_enhet as avdeling,
        orgenhet_paa_ressurs_beskrivelse as omrade,
        stillingsnavn,
        case
            when resource_typ = 'E' then 'Ekstern'
            else 'Intern' -- neglisjerer typ lærling og innlån/utlån, som er veldig få
        end as ansettelsestype,
        kjonn,
        case 
            when floor(months_between(sysdate, fodselsdato) / 12) < 35 then 'under 35'
            when floor(months_between(sysdate, fodselsdato) / 12) between 35 and 50 then '35-50'
            else 'over 50'
        end as aldersgruppe
    from dt_hr.hrres_ressurs_lav
),

pensjon_teams as (
    select
        nav_id,
        team,
        po_navn,
        rolle,
        rolle2
    from (
        { ' UNION ALL '.join([f"SELECT '{row.nav_id}' AS nav_id, '{row.team}' AS team, '{row.po_navn}' AS po_navn, '{row.rolle}' AS rolle, '{row.rolle2}' AS rolle2 from dual" for row in df_pensjon_teams.itertuples(index=False)]) }
    )
),

joined as (
    select
        pensjon_teams.nav_id,
        pensjon_teams.team,
        pensjon_teams.po_navn,
        pensjon_teams.rolle,
        pensjon_teams.rolle2,
        hr_data.avdeling,
        hr_data.omrade,
        hr_data.stillingsnavn,
        hr_data.ansettelsestype,
        hr_data.kjonn,
        hr_data.aldersgruppe
    from pensjon_teams
    left join hr_data on pensjon_teams.nav_id = hr_data.nav_id
),

aggregert as (
    select
        team,
        po_navn as po,
        rolle,
        rolle2,
        avdeling,
        omrade,
        stillingsnavn as stilling,
        ansettelsestype,
        kjonn,
        aldersgruppe,
        count(*) as antall,
        trunc(sysdate) as dato_hentet
    from joined
    group by
        team,
        po_navn,
        rolle,
        rolle2,
        avdeling,
        omrade,
        stillingsnavn,
        ansettelsestype,
        kjonn,
        aldersgruppe,
        trunc(sysdate)
)

select * from aggregert
"""


oracle_sql_alle_po = f"""
with

hr_data as (
    select
        nav_id, -- for join mot data fra bq
        remedy_enhet as avdeling,
        orgenhet_paa_ressurs_beskrivelse as omrade,
        stillingsnavn,
        case
            when resource_typ = 'E' then 'Ekstern'
            else 'Intern' -- neglisjerer typ lærling og innlån/utlån, som er veldig få
        end as ansettelsestype,
        kjonn,
        case 
            when floor(months_between(sysdate, fodselsdato) / 12) < 35 then 'under 35'
            when floor(months_between(sysdate, fodselsdato) / 12) between 35 and 50 then '35-50'
            else 'over 50'
        end as aldersgruppe
    from dt_hr.hrres_ressurs_lav
),

alle_po as (
    select
        nav_id,
        po_navn,
        rolle
    from (
        { ' UNION ALL '.join([f"SELECT '{row.nav_id}' AS nav_id, '{row.po_navn}' AS po_navn, '{row.rolle}' AS rolle from dual" for row in df_alle_po.itertuples(index=False)]) }
    )
),

joined as (
    select
        alle_po.nav_id,
        alle_po.po_navn,
        -- alle_po.rolle,
        -- hr_data.avdeling,
        -- hr_data.omrade,
        -- hr_data.stillingsnavn,
        hr_data.ansettelsestype,
        hr_data.kjonn,
        hr_data.aldersgruppe
    from alle_po
    left join hr_data on alle_po.nav_id = hr_data.nav_id
),

aggregert as (
    select
        po_navn as po,
        ansettelsestype,
        kjonn,
        aldersgruppe,
        count(*) as antall,
        trunc(sysdate) as dato_hentet
    from joined
    group by
        po_navn,
        ansettelsestype,
        kjonn,
        aldersgruppe,
        trunc(sysdate)
)

select * from aggregert
"""


with open("bemanningsoversikt_alle_po.sql", "w") as f:
    f.write(oracle_sql_alle_po)

with open("bemanningsoversikt_pensjon_teams.sql", "w") as f:
    f.write(oracle_sql_pensjon)

# kjører disse manuelt i dvhp og kopierer inn resultatet i en csv-fil
# it is what it is, and it is not great
