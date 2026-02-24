-- dataproduct_laaste_vedtak.py
-- pensjon-saksbehandli-prod-1f83.vedtak.laast_data_handling

-- oversikt over krav og vedtak som har blitt "låst opp" ved å endre status eller behandlingstype
-- lagt til mapping av k_laast_data_handling til beskrivelse navn

with laast_data_handling_map as (
    select 'ENDR_KRAV_BEH_TYPE' as kode, 'Endre behandlingstype' as dekode from dual
    union all
    select 'ENDR_STATUS_TYPE' as kode, 'Endre status' as dekode from dual
),
base as (
        select l.dato_opprettet as dato_opprettet,
                d_s.dekode as sakstype,
                d_k.dekode as kravtype,
                coalesce(kam.dekode, l.k_laast_data_handling) as handling,
                le.elementtype as elementtype,
                le.gammel_verdi as gammel_verdi,
                le.ny_verdi as ny_verdi
         from pen.t_laast_data_logg l
                  inner join pen.t_laast_data_logg_element le on le.laast_data_logg_id = l.laast_data_logg_id
                  inner join pen.t_kravhode kh on kh.kravhode_id = l.krav_id
                  inner join pen.t_k_krav_gjelder d_k on d_k.k_krav_gjelder = kh.k_krav_gjelder
                  inner join pen.t_sak s on s.sak_id = l.sak_id
                  inner join pen.t_k_sak_t d_s on d_s.k_sak_t = s.k_sak_t
                  left join laast_data_handling_map kam on kam.kode = l.k_laast_data_handling
         where le.elementtype != 'KRAVLINJE'
           and not (le.elementtype = 'KRAVHODE' and l.k_laast_data_handling = 'ENDR_STATUS_TYPE'))
select dato_opprettet,
       sakstype,
       kravtype,
       handling,
       elementtype,
       gammel_verdi,
       ny_verdi
from base