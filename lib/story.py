from datastory import DataStory


def make_datastory(ytelse, figs, innledning):
    ds = DataStory(f"Automatiserings- og selvbetjeningsgrad for {ytelse}")
    
    ds.markdown("**OBS! Rapporten som figurene er basert på oppdateres ikke lenger. Ny versjon kommer!**")
    ds.markdown(innledning)
    dims_tekst = "## Dimensjoner \n- Selvbetjent/ikke selvbetjent: Angir hvorvidt saken er opprettet av bruker eller av NAV. \n- Selvbetjent: Saker hvor bruker har opprettet saken gjennom selvbetjeningsløsning Din Pensjon eller Ditt NAV. Skjema sendt inn gjennom skjemaveilederen telles ikke som selvbetjent. \n- Ikke selvbetjent: Saker opprettet av saksbehandler i Pesys eller i selvbetjeningsløsning. \n- Automatiseringsgrad: Angir hvorvidt saken behandlet helt automatisk, delvis automatisk eller manuelt. \n- Automatisk: Saker som er behandlet automatisk av Pesys. \n- Del-automatisk: Saker som er behandlet delautomatisk, det vil si at enkelte steg er utført av saksbehandler og enkelte steg er utført av Pesys. \n- Manuell: Saker som i sin helhet er behandlet av saksbehandler."
    ds.markdown(dims_tekst)
    
    if "auto_total" in figs:
        ds.header("Automatiseringsgrad alle førstegangsbehandlinger", level=1)
        ds.plotly(figs["auto_total"].to_json())
        
        ds.header("Automatiseringsgrad selvbetjening", level=2)
        ds.plotly(figs["auto_selv"].to_json())

        ds.header("Automatiseringsgrad utlandstilsnitt", level=2)
        ds.plotly(figs["auto_utland"].to_json())

    else:
        print(f"Ingen figurer for automatiseringsgrad, {ytelse}.")
        ds.markdown(f"**Ingen saker automatiseres for {ytelse}.**")


    if "selv_total" in figs:
        figs["selv_total"]
        ds.header("Selvbetjeningsgrad alle førstegangsbehandlinger", level=1)
        ds.plotly(figs["selv_total"].to_json())

        ds.header("Selvbetjeningsgrad utlandstilsnitt", level=2)
        ds.plotly(figs["selv_utland"].to_json())
    else:
        print(f"Ingen figurer for selvbetjeningsgrad, {ytelse}.")
        ds.markdown(f"**Saker for {ytelse} går ikke gjennom selvbetjeningsløsningen.**")

    return ds