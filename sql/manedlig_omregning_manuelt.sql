--- manuell per ytelse per mnd
select to_char(opp.dato_opprettet, 'yyyy-mm') as opprettet_maned,
       opp.ytelse,
       count(opp.manuell)                     as auto
from pen.t_opptjeningsendring_bruker opp
where opp.manuell = '1'
group by to_char(opp.dato_opprettet, 'yyyy-mm'), opp.ytelse, opp.manuell
order by opprettet_maned desc, opp.ytelse, manuell desc;