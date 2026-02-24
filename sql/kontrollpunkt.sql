-- kontrollpunkt.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.kontrollpunkt.kontrollpunkt_daglig

-- Kravårsak-mapping definert i kravarsak_dim.sql (gjenbrukbar)
with kravarsak_map as (
    @@kravarsak_dim.sql
),
base as (
    select
        trunc(kp.dato_opprettet) as dato,
        dkh.dekode as kravtype,
        (case when substr(kh.opprettet_av,1,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Bruker' else
        case when substr(kh.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(kh.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') then 'Saksbehandler' else
        kh.opprettet_av end end ) as behandler,
        ds.dekode as sakstype,
        dks.dekode as kravstatus,
        dkb.dekode as behandlingstype,
        kp.k_kontrollpnkt_t as kontrollpunkt,
        coalesce(kam.dekode, arsak.k_krav_arsak_t) as kravarsak,
        dkp.dekode_tekst as kontrollpunkt_forklaring

    from pen.t_kontrollpunkt kp
    inner join pen.t_kravhode kh on kh.kravhode_id = kp.kravhode_id
    inner join pen.t_sak s on s.sak_id = kp.sak_id
    inner join pen.t_k_krav_gjelder dkh on dkh.k_krav_gjelder = kh.k_krav_gjelder
    inner join pen.t_k_sak_t ds on ds.k_sak_t = s.k_sak_t
    inner join pen.t_k_kontrollpnkt_t dkp on dkp.k_kontrollpnkt_t = kp.k_kontrollpnkt_t
    inner join pen.t_k_krav_s dks on dks.k_krav_s = kh.k_krav_s
    inner join pen.t_k_behandling_t dkb on dkb.k_behandling_t = kh.k_behandling_t
    inner join pen.t_krav_arsak arsak on arsak.kravhode_id = kh.kravhode_id
    left join kravarsak_map kam on kam.k_krav_arsak_t = arsak.k_krav_arsak_t

    where trunc(kp.dato_opprettet) < trunc(current_date)
)
select
    dato,
    kravtype,
    behandler,
    sakstype,
    kravstatus,
    behandlingstype,
    kontrollpunkt,
    kravarsak,
    kontrollpunkt_forklaring,
    count(*) as antall
from base

group by
    dato,
    kravtype,
    behandler,
    sakstype,
    kravstatus,
    behandlingstype,
    kontrollpunkt,
    kravarsak,
    kontrollpunkt_forklaring
