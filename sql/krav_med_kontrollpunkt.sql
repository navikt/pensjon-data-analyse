select
    extract(month from kh.dato_opprettet) as mnd,
    extract(year from kh.dato_opprettet) as yr,
    dkh.dekode as kravtype,
    (case when substr(kh.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
    case when substr(kh.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(kh.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
    kh.opprettet_av end end ) as behandler,
    ds.dekode as sakstype,
    dks.dekode as kravstatus,
    dkb.dekode as behandlingstype,
    kp.k_kontrollpnkt_t as kontrollpunkt,
    dkp.dekode_tekst as kontrollpunkt_forklaring,
    count(*) as antall

from pen.t_kravhode kh
    inner join pen.t_k_krav_gjelder dkh on dkh.k_krav_gjelder = kh.k_krav_gjelder
    inner join pen.t_k_behandling_t dkb on dkb.k_behandling_t = kh.k_behandling_t
    inner join pen.t_k_krav_s dks on dks.k_krav_s = kh.k_krav_s
inner join pen.t_sak s on s.sak_id = kh.sak_id
    inner join pen.t_k_sak_t ds on ds.k_sak_t = s.k_sak_t
left join pen.t_kontrollpunkt kp on kp.kravhode_id = kh.kravhode_id
    left join pen.t_k_kontrollpnkt_t dkp on dkp.k_kontrollpnkt_t = kp.k_kontrollpnkt_t

where s.k_sak_t = 'ALDER'
--and kh.k_behandling_t = 'DEL_AUTO'
--and kp.dato_opprettet < trunc(current_date, 'MM')
and kh.k_krav_gjelder in ('FORSTEG_BH', 'F_BH_MED_UTL')
and kh.k_krav_s != 'AVBRUTT'

group by
    extract(month from kh.dato_opprettet),
    extract(year from kh.dato_opprettet),
    dkh.dekode,
    (case when substr(kh.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
    case when substr(kh.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(kh.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
    kh.opprettet_av end end ),
    ds.dekode,
    dks.dekode,
    dkb.dekode,
    kp.k_kontrollpnkt_t,
    dkp.dekode_tekst