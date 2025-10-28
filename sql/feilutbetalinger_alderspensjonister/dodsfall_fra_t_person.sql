select
    extract(year from p.dato_dod) as dod_ar,
    count(*) as antall_dodsfall_alder
from pen.t_person p
left join pen.t_sak s on p.person_id = s.person_id
where
    p.dato_dod is not null
    and extract(year from p.dato_dod) >= 2012
    and s.k_sak_t = 'ALDER'
group by extract(year from p.dato_dod)
order by dod_ar desc
