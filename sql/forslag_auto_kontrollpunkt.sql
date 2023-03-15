select sakstype, behandler, k_behandling_t as automatisering, kravtype, dato_virk_fom, batch, kontrollpunkt_kode, kontrollpunkt, status_kontrollpunkt
--, bodd_Arb_utl
--, count(1) antall
from (
select
d_s.dekode sakstype
, (case when substr(kh.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
   case when substr(kh.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(kh.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
   kh.opprettet_av end end ) as behandler
, (case when kh.kravkilde is not null then 'Ja' else
   case when (select 1 from pen.t_skjema s where s.kravhode_id = kh.kravhode_id) > 0 then 'Ja' else
   case when (select 1 from PEN.t_skjema_afp_priv s where s.kravhode_id = kh.kravhode_id) > 0 then 'Ja' else 'Nei' end end end) as Selvbetjening
, kh.k_behandling_t
, d_k.dekode kravtype  
, v.dato_virk_fom
, case when substr(kh.opprettet_av,1,4) = 'BPEN' then 'Batch' else 'Ikke batch' end as batch
, kp.k_kontrollpnkt_t kontrollpunkt_kode
, d_kp.dekode_tekst kontrollpunkt
, kp.k_kontrollpnkt_s status_kontrollpunkt
--, kh.bodd_arb_utl
from pen.t_vedtak v
inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
inner join pen.t_k_krav_gjelder d_k on d_k.k_krav_gjelder = kh.k_krav_gjelder
inner join pen.t_k_sak_t d_s on d_s.k_sak_t = v.k_sak_t
left join pen.t_kontrollpunkt kp on kp.kravhode_id =  kh.kravhode_id
left join pen.t_k_kontrollpnkt_t d_kp on d_kp.k_kontrollpnkt_t = kp.k_kontrollpnkt_t
where v.k_vedtak_s = 'IVERKS'
  and v.dato_virk_fom >= to_Date('01.01.x_year','DD.MM.YYYY')
  and v.dato_virk_fom < to_Date('01.01.y_year','DD.MM.YYYY')
  and v.k_vedtak_t != 'REGULERING'
  --and kh.k_krav_gjelder in ('REGULERING','TILBAKEKR','OVERF_OMSGSP','ENDR_UTTAKSGRAD','INNT_E','FORSTEG_BH','REVURD', 'UT_EO')

union all 

select
d_s.dekode sakstype
, (case when substr(v.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
   case when substr(v.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(v.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
   v.opprettet_av end end ) as behandler
, 'Nei' as Selvbetjening
, v.k_behandling_t  
, 'Regulering' as kravtype  
, v.dato_virk_fom
, case when substr(v.opprettet_av,1,4) = 'BPEN' then 'Batch' else 'Ikke batch' end as batch
--, kh.bodd_arb_utl
, null as kontrollpunkt_kode
, null as kontrollpunkt
, null as status_kontrollpunkt
from pen.t_vedtak v
inner join pen.t_kravhode kh on kh.kravhode_id = v.kravhode_id
inner join pen.t_k_krav_gjelder d_k on d_k.k_krav_gjelder = kh.k_krav_gjelder
inner join pen.t_k_sak_t d_s on d_s.k_sak_t = v.k_sak_t
where v.k_vedtak_s = 'IVERKS'
  and v.dato_virk_fom >= to_Date('01.01.x_year','DD.MM.YYYY')
  and v.dato_virk_fom < to_Date('01.01.y_year','DD.MM.YYYY')
  and v.k_vedtak_t = 'REGULERING'
) A
--group by k_sak_t, Behandler, k_behandling_t , k_krav_gjelder, dato_virk_fom  --, bodd_Arb_utl
--order by sakstype, kravtype, dato_virk_fom, automatisering, behandler  --, bodd_arb_utl
