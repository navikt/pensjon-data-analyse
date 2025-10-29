select
    count(distinct autobrev_id) as antall_autobrev,
    extract(year from ab.dato_opprettet) as ar
from pen.t_autobrev ab
left join pen.t_person p on p.fnr_fk = ab.fnr_brev_mottaker
inner join pen.t_sak s on p.person_id = s.person_id and s.k_sak_t = 'UFOREP'
left join pen.t_vedtak v on v.sak_id = s.sak_id -- sjekker at de hadde løpende ytelse i 2023
    and extract(year from v.dato_lopende_fom) <= 2023
    and (extract(year from v.dato_lopende_tom) >= 2023 or v.dato_lopende_tom is not null)
group by extract(year from ab.dato_opprettet)
order by extract(year from ab.dato_opprettet) desc

-- evt denne joinen under, men da mister vi brev som ikke er koblet til vedtak, feks varselbrev EO tilbakekreving
-- where ab.vedtak_id in (select vedtak_id from pen.t_vedtak where k_sak_t = 'UFOREP')
-- left join pen.t_vedtak v on v.vedtak_id = ab.vedtak_id
