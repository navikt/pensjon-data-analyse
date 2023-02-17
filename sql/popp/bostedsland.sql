select distinct p.fnr_fk, l.dekode bostedsland
from pen.t_person p
left join pen.t_k_land_3_tegn l on l.k_land_3_tegn_id = p.bostedsland
where p.bostedsland is not null