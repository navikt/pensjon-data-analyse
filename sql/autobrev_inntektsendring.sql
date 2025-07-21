-- brukt i autobrev_inntektsendring.py (Metabase)
-- pensjon-saksbehandli-prod-1f83.brev.autobrev_inntektsendring

select
    extract(year from kh.dato_onsket_virk) as ar,
    extract(month from kh.dato_onsket_virk) as maned,
    concat(
        concat(extract(year from kh.dato_onsket_virk), '-'),
        lpad(extract(month from kh.dato_onsket_virk), 2, '0')
    ) as armaned,
    (case
        when substr(v.opprettet_av, 1, 1) in ('0','1','2','3','4','5','6','7','8','9')
            then 'Bruker'
        when
            substr(v.opprettet_av, 1, 1) not in ('0','1','2','3','4','5','6','7','8','9')
            and substr(v.opprettet_av, 2, 1) in ('0','1','2','3','4','5','6','7','8','9')
            then 'Saksbehandler'
        else v.opprettet_av
    end) as opprettet_av,
    (case when ab.k_batchbrev_id is not null then 'Autobrev' else 'Manuelt brev eller uten brev' end) as brevtype,
    lower(kh.k_behandling_t) as behandlingstype,
    count(1) as antall
from pen.t_kravhode kh
inner join pen.t_vedtak v
    on
        v.kravhode_id = kh.kravhode_id
        and v.k_vedtak_t != 'REGULERING'
        and v.dato_iverksatt is not null
left outer join pen.t_autobrev ab
    on ab.vedtak_id = v.vedtak_id
where
    kh.k_krav_gjelder = 'INNT_E' -- inntektsendring
    and kh.k_krav_s = 'FERDIG'
    and extract(year from kh.dato_onsket_virk) >= 2022
group by
    extract(year from kh.dato_onsket_virk),
    extract(month from kh.dato_onsket_virk),
    concat(
        concat(extract(year from kh.dato_onsket_virk), '-'),
        lpad(extract(month from kh.dato_onsket_virk), 2, '0')),
    (case
        when substr(v.opprettet_av, 1, 1) in ('0','1','2','3','4','5','6','7','8','9')
            then 'Bruker'
        when
            substr(v.opprettet_av, 1, 1) not in ('0','1','2','3','4','5','6','7','8','9')
            and substr(v.opprettet_av, 2, 1) in ('0','1','2','3','4','5','6','7','8','9')
            then 'Saksbehandler'
        else v.opprettet_av
    end),
    (case when ab.k_batchbrev_id is not null then 'Autobrev' else 'Manuelt brev eller uten brev' end),
    kh.k_behandling_t
order by
    extract(year from kh.dato_onsket_virk) desc,
    extract(month from kh.dato_onsket_virk) desc,
    count(1) desc
