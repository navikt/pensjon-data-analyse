--- automatisk per ytelse per mnd
select to_char(opp.dato_opprettet, 'yyyy-mm') as kjoremaned,
       opp.ytelse,
       count(opp.manuell)                     as manuell
from pen.t_opptjeningsendring_bruker opp
where opp.manuell = '0'
group by to_char(opp.dato_opprettet, 'yyyy-mm'), opp.ytelse, opp.manuell
order by kjoremaned desc, opp.ytelse, manuell desc