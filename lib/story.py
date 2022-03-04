from datastory import DataStory


def make_datastory(ytelse, figs):
    ds = DataStory(f"Automatiserings- og selvbetjeningsgrad for {ytelse}")
    
    innledning = f"Statistikk på automatiserings- og selvbetjeningsgrad for førstegangsbehandling av {ytelse}. Data er basert på rapporten PSAK120. Saker opprettet av batch er eksludert."
    ds.markdown(innledning)

    ds.header("Automatiseringsgrad", level=1)
    ds.markdown("Automatiseringsgrad på alle nye saker. Del-automatisk henviser til saker hvor deler av saksbehandlingen ble gjort automatisk.")
    ds.plotly(figs["auto_total"].to_json())

    ds.header("Automatiseringsgrad selvbetjening", level=2)
    ds.plotly(figs["auto_selv"].to_json())

    ds.header("Automatiseringsgrad utlandstilsnitt", level=2)
    ds.plotly(figs["auto_utland"].to_json())

    ds.header("Selvbetjeningsgrad", level=1)
    ds.markdown("Selvbetjeningsgrad på alle nye saker.")
    ds.plotly(figs["selv_total"].to_json())

    ds.header("Selvbetjeningsgrad utlandstilsnitt", level=2)
    ds.plotly(figs["selv_utland"].to_json())
    
    return ds