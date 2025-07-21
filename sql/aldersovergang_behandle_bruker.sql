-- aldersovergang.py (Metabase for team Alder)
-- pensjon-saksbehandli-prod-1f83.aldersovergang.aldersovergang_behandle_bruker

select
    opp.behandlingsmaned as behandlingsmaned,
    beh.hovedytelse as hovedytelse,
    beh.delytelse as delytelse,
    beh.oppg_beskr_koder as oppg_beskr_koder,
    count(beh.oppg_beskr_koder) as antall
from pen.t_aldersovergang_oppsummering opp
    join pen.t_aldersovergang_oppsummering_behandling beh on opp.id = beh.aldersovergang_oppsummering_id
where beh.oppg_beskr_koder is not null
group by
    opp.behandlingsmaned,
    beh.hovedytelse,
    beh.delytelse,
    beh.oppg_beskr_koder
