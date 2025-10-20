--- reduksjon per ytelse per mnd
select to_char(opp.dato_opprettet, 'yyyy-mm') as kjoremaned,
       opp.ytelse,
       count(opp.reduksjon)                   as reduksjon
from pen.t_opptjeningsendring_bruker opp
where opp.reduksjon = '1'
group by to_char(opp.dato_opprettet, 'yyyy-mm'), opp.ytelse, opp.reduksjon
order by kjoremaned desc, opp.ytelse, reduksjon desc;