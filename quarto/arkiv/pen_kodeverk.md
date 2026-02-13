# Publisere pen_kodeverk quarto fortelling


- `DB_USER`
- `DB_ENV_SECRET_PASS`
- `DB_HOST`

```bash
quarto render pen_kodeverk.qmd 
```

miljøvariableer:
- `QUARTO_ID`: ID-en for Quarto-prosjektet i Teamet
- `TEAM_TOKEN`: Token for autentisering mot Teamet
- `ENV` f.eks 'datamarkedsplassen.intern.nav.no'


```bash
curl -X PUT -F index.html=@index.html \                 
    "https://${ENV}/quarto/update/${QUARTO_ID}" \
    -H "Authorization:Bearer ${TEAM_TOKEN}"
```