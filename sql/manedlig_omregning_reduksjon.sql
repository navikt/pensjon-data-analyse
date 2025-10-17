--- reduksjon per ytelse per mnd
select to_char(opp.dato_opprettet, 'yyyy-mm') as opprettet_maned,
       opp.ytelse,
       count(opp.reduksjon)                   as reduksjon
from pen.t_opptjeningsendring_bruker opp
where opp.reduksjon = '1'
group by to_char(opp.dato_opprettet, 'yyyy-mm'), opp.ytelse, opp.reduksjon
order by opprettet_maned desc, opp.ytelse, reduksjon desc;