--- belop_redusert per ytelse per mnd
select to_char(opp.dato_opprettet, 'yyyy-mm') as kjoremaned,
       opp.ytelse,
       count(opp.belop_redusert)                   as reduksjon
from pen.t_opptjeningsendring_bruker opp
where opp.belop_redusert = '1'
group by to_char(opp.dato_opprettet, 'yyyy-mm'), opp.ytelse, opp.belop_redusert
order by kjoremaned desc, opp.ytelse, belop_redusert desc