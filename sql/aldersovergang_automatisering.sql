select
    opp.behandlingsmaned,
    beh.hovedytelse,
    beh.delytelse,
    count(beh.oppg_beskr_koder) as oppgave, -- falt ut til manuell behandling/oppgave
    count(*) as total
from pen.t_aldersovergang_oppsummering opp
join pen.t_aldersovergang_oppsummering_behandling beh on opp.id = beh.aldersovergang_oppsummering_id
group by opp.behandlingsmaned, beh.hovedytelse, beh.delytelse
order by opp.behandlingsmaned desc, beh.hovedytelse asc, beh.delytelse asc
