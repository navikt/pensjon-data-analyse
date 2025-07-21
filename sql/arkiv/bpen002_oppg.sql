-- bpen002_oppg.qmd (arkivert datafortelling)

select trunc(b.dato_opprettet) as dato, d_s.dekode as sakstype, o.oppgavesett, o.oppgavetekst, b.kontrollpunkt_liste, o.prioritet, count(*) as antall
from PEN.t_bpen002_oppgave b
join PEN.t_k_oppgave_kontrollpunkt_map o on instr(',' || b.kontrollpunkt_liste || ',', ',' || o.k_kontrollpnkt_t || ',') > 0
left join pen.t_sak s on b.sak_id = s.sak_id
left join pen.t_k_sak_t d_s on d_s.k_sak_t = s.k_sak_t
where extract(year from b.dato_opprettet) >= 2023
group by trunc(b.dato_opprettet), o.oppgavesett, o.oppgavetekst, b.kontrollpunkt_liste, o.prioritet, d_s.dekode
order by dato desc
