select count(*) as antall, o.oppgavesett, o.oppgavetekst, b.kontrollpunkt_liste, trunc(b.dato_opprettet) as dato 
from PEN.t_bpen002_oppgave b
join PEN.t_k_oppgave_kontrollpunkt_map o on instr(',' || b.kontrollpunkt_liste || ',', ',' || o.k_kontrollpnkt_t || ',') > 0
group by o.oppgavesett, o.oppgavetekst, b.kontrollpunkt_liste, trunc(b.dato_opprettet)
order by dato desc;