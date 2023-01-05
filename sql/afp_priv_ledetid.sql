select vedtaksdato, dato_opprettet, extract(day from dato_opprettet - vedtaksdato) dager, k_fk_resultat resultat
from PEN.T_AFP_PRIV_RES_FK afp
--left join pen.t_kravhode kh on kh.AFP_PRIV_RES_FK_ID = afp.AFP_PRIV_RES_FK_ID
order by vedtaksdato desc