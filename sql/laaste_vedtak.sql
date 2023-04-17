select l.dato_opprettet, d_l.dekode as handling, le.elementtype, le.gammel_verdi, le.ny_verdi
from pen.t_laast_data_logg l
inner join pen.t_laast_data_logg_element le on le.laast_data_logg_id = l.laast_data_logg_id
inner join pen.t_k_laast_data_handling d_l on d_l.k_laast_data_handling = l.k_laast_data_handling
