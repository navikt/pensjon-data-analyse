-- afp_privat_resultater.qmd (arkivert datafortelling)

select vedtaksdato, k_fk_resultat resultat, count(*) antall from PEN.T_AFP_PRIV_RES_FK
group by vedtaksdato, k_fk_resultat
order by vedtaksdato desc