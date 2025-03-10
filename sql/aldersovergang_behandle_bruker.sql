select
    opp.behandlingsmaned,
    beh.hovedytelse,
    beh.delytelse,
    beh.oppg_beskr_koder,
    count(beh.oppg_beskr_koder)
from pen.t_aldersovergang_oppsummering opp
    join pen.t_aldersovergang_oppsummering_behandling beh on opp.id = beh.aldersovergang_oppsummering_id
where beh.oppg_beskr_koder is not null
group by
    opp.behandlingsmaned,
    beh.hovedytelse,
    beh.delytelse,
    beh.oppg_beskr_koder
