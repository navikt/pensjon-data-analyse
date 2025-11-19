select b.behandling_code  as behandlingstype,
       trunc(b.opprettet) as opprettet,
       (extract(day from (b.siste_kjoring - b.opprettet)) * 86400 -- sekunder per dag
           + extract(hour from (b.siste_kjoring - b.opprettet)) * 3600 -- per time
           + extract(minute from (b.siste_kjoring - b.opprettet)) * 60 -- per minutt
           + extract(second from (b.siste_kjoring - b.opprettet))
           )              as behandlingstid,
       ansvarlig_team
from pen.t_behandling b
where b.status = 'FULLFORT'
  and trunc(b.opprettet) < -- Initsiell last, bytt til = etter forste kjoring
      trunc(sysdate) - 1