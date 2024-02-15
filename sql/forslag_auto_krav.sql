select sakstype, behandler, selvbetjening, k_behandling_t as automatisering, kravtype, dato_opprettet, batch, bodd_arb_utl
from (
select
   d_s.dekode sakstype
   , (case when substr(kh.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
      case when substr(kh.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(kh.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
      kh.opprettet_av end end ) as behandler
   , (case when kh.kravkilde is not null then 1 else
      case when (select 1 from pen.t_skjema s where s.kravhode_id = kh.kravhode_id) > 0 then 1 else
      case when (select 1 from PEN.t_skjema_afp_priv s where s.kravhode_id = kh.kravhode_id) > 0 then 1 else 0 end end end) as Selvbetjening
   , kh.k_behandling_t
   , d_k.dekode kravtype
   , kh.dato_opprettet
   , case when substr(kh.opprettet_av,1,4) = 'BPEN' then 'Batch' else 'Ikke batch' end as batch
   , kh.bodd_arb_utl

from pen.t_kravhode kh
   inner join pen.t_k_krav_gjelder d_k on d_k.k_krav_gjelder = kh.k_krav_gjelder
   inner join pen.t_sak s on kh.sak_id = s.sak_id
   inner join pen.t_k_sak_t d_s on d_s.k_sak_t = s.k_sak_t

where kh.k_krav_s != 'AVBRUTT'
   and kh.dato_opprettet >= to_Date('01.01.x_year','DD.MM.YYYY') -- x_year byttes ut med årstall man vil hente
   and kh.dato_opprettet < to_Date('01.01.y_year','DD.MM.YYYY') -- y_year byttes ut med året etter det man vil hente
   and kh.dato_opprettet < trunc(current_date, 'mm') --Ekskluderer nåværende måned
)
