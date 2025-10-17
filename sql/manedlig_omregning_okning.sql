--- okning per ytelse per mnd
select to_char(opp.dato_opprettet, 'yyyy-mm') as opprettet_maned,
       opp.ytelse,
       count(opp.okning)                      as okning
from pen.t_opptjeningsendring_bruker opp
where opp.okning = '1'
group by to_char(opp.dato_opprettet, 'yyyy-mm'), opp.ytelse, opp.okning
order by opprettet_maned desc, opp.ytelse, okning desc;