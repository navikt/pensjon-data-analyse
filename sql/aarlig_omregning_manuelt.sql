--- manuell vs automatisk prosessert per ytelse per aar
select opp.opptjeningsaar + 1                              as aar,
       opp.k_sak_t                                         as ytelse,
       count(case when opp.oppgave is not null then 1 end) as manuell,
       count(case when opp.oppgave is null then 1 end)     as auto
from pen.t_opptjeningsendring_aarlig_oppsummering opp
group by opp.opptjeningsaar, opp.k_sak_t
order by aar desc, ytelse