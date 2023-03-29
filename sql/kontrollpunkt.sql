with staging as (select
    trunc(kp.dato_opprettet) as dato,
    dkh.dekode as kravtype,
    (case when substr(kh.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
    case when substr(kh.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(kh.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
    kh.opprettet_av end end ) as behandler,
    ds.dekode as sakstype,
    kp.k_kontrollpnkt_t as kontrollpunkt,
    dkp.dekode_tekst as kontrollpunkt_forklaring,
    count(*) as antall

from pen.t_kontrollpunkt kp
inner join pen.t_kravhode kh on kh.kravhode_id = kp.kravhode_id
inner join pen.t_sak s on s.sak_id = kp.sak_id
inner join pen.t_k_krav_gjelder dkh on dkh.k_krav_gjelder = kh.k_krav_gjelder
inner join pen.t_k_sak_t ds on ds.k_sak_t = s.k_sak_t
inner join PEN.t_k_kontrollpnkt_t dkp on dkp.k_kontrollpnkt_t = kp.k_kontrollpnkt_t

where trunc(kp.dato_opprettet) < trunc(current_date)

group by
    trunc(kp.dato_opprettet),
    dkh.dekode,
    (case when substr(kh.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
    case when substr(kh.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(kh.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
    kh.opprettet_av end end ),
    ds.dekode,
    kp.k_kontrollpnkt_t,
    dkp.dekode_tekst
)

select * from staging
where antall >= 5
--order by dato desc, antall desc

union all

select 
    dato,
    '-' as kravtype,
    '-' as behandler,
    '-' as sakstype,
    '-' as kontrollpunkt,
    '-' as kontrollpunkt_forklaring,
    count(*) as antall
from staging
where antall < 5
group by dato, '-', '-', '-', '-', '-'