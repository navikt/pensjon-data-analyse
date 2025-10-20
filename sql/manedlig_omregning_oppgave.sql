--- oppgavebeskrivelse per ytelse per mnd
select to_char(opp.dato_opprettet, 'yyyy-mm') as kjoremaned,
       opp.ytelse,
       opp.oppg_beskr_koder,
       count(opp.oppg_beskr_koder)            as oppgave
from pen.t_opptjeningsendring_bruker opp
where opp.oppg_beskr_koder is not null
group by to_char(opp.dato_opprettet, 'yyyy-mm'), opp.ytelse, opp.oppg_beskr_koder
order by kjoremaned desc, opp.ytelse, oppgave desc;