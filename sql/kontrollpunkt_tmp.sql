select extract(year from kp.dato_opprettet) ar, kp.k_kontrollpnkt_t, kp.k_kontrollpnkt_s, 
(case when substr(kp.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
   case when substr(kp.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(kp.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
   kp.opprettet_av end end ) as behandler,
   count(*) antall,
   d_kp.dekode_tekst
from pen.t_kontrollpunkt kp
left join pen.t_k_kontrollpnkt_t d_kp on d_kp.k_kontrollpnkt_t = kp.k_kontrollpnkt_t
group by kp.k_kontrollpnkt_t, kp.k_kontrollpnkt_s, extract(year from kp.dato_opprettet),
(case when substr(kp.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
   case when substr(kp.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(kp.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
    kp.opprettet_av end end ),
    d_kp.dekode_tekst
order by ar desc, antall desc