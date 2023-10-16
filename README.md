# pensjon-data-analyse
Dette repoet brukes til versjonskontroll av airflowjobber og en mengde ad-hoc innsiktsarbeid i PO Pensjon. Sistnevnte består hovedsakelig av jupyter notebooks og sql-spørringer mot PEN-databasen. Koden er beregnet på å kjøre i KNADA (se [docs.knada.io](docs.knada.io)).

## Ordliste
- KNADA: En samling verktøy med tilgang til onprem-ressurser. Hovedbestanddelene er jupyterhub/VM og airflow.
- Dataprodukt: En tabell i BigQuery som er publisert på datamarkedsplassen
- Datafortelling: En rapport som er laget med quarto eller datastory og publisert på datamarkedsplassen
- Quarto: Verktøy for å kompilere en .html-fil (datafortelling) eller andre typer filer fra en jupyter notebook
- Datastory: Deprecated verktøy for å publisere datafortellinger. 


Under følger en kort beskrivelse av innholdet i hver mappe.

## common
Inneholder funksjoner som brukes av airflowjobbene som oppdaterer datafortellinger laget med quarto