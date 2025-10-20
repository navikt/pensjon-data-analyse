--- manuell og auto per ytelse per mnd
select to_char(opp.dato_opprettet, 'yyyy-mm')        as kjoremaned,
       opp.ytelse,
       count(case when opp.manuell = '1' then 1 end) as manuell,
       count(case when opp.manuell = '0' then 0 end) as auto
from pen.t_opptjeningsendring_bruker opp
group by to_char(opp.dato_opprettet, 'yyyy-mm'), opp.ytelse
order by kjoremaned desc, opp.ytelse, manuell desc