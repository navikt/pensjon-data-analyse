--- antall oppgavetyper per år
select opp.opptjeningsaar as aar,
       opp.oppgave        as oppgavetype,
       count(*)           as antall_oppgaver
from pen.t_opptjeningsendring_aarlig_oppsummering opp
where opp.oppgave is not null
group by opp.opptjeningsaar, opp.oppgave
order by aar desc, antall_oppgaver desc, oppgavetype