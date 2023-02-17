select distinct p.fnr_fk, l.dekode bostedsland, a.dekode avtaletype, ak.dekode avtalekriterie
from pen.t_person p
left join pen.t_person_grunnlag pg on pg.person_id = p.person_id
left join pen.t_trygdeavtale t on t.person_grunnlag_id = pg.person_grunnlag_id
left join pen.t_k_land_3_tegn l on l.k_land_3_tegn_id = p.bostedsland
left join pen.t_k_avtale_t a on a.K_AVTALE_T = t.K_AVTALE_T
left join pen.t_k_avtale_krit_t ak on ak.k_avtale_krit_t = t.k_avtale_krit_t
where p.bostedsland is not null or a.dekode is not null or ak.dekode is not null
