select trunc(sysdate) - 1      as dato,
       b.behandling_code       as behandlingstype,
       bm.underkategori_kode   as underkategori,
       bm.oppgave_kode         as oppgavekode,
       bm.fagomrade,
       min(bm.kategori_decode) keep (dense_rank last order by bm.opprettet) as kategori,
       count(*)                as antall
from pen.t_behandling b
         join pen.t_aktivitet a
              on b.behandling_id = a.behandling_id
         join pen.t_behandling_manuell bm
              on a.aktivitet_id = bm.aktivitet_id
where trunc(bm.opprettet) = trunc(sysdate) - 1
group by b.behandling_code, bm.underkategori_kode, bm.oppgave_kode, bm.fagomrade, bm.kategori
order by antall desc