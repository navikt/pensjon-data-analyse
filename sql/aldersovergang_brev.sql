select
    opp.behandlingsmaned,
    brev.kategori_kode,
    brev.brev_kode,
    count(brev_kode)
from pen.t_aldersovergang_oppsummering opp
join pen.t_aldersovergang_oppsummering_brev brev on opp.id = brev.aldersovergang_oppsummering_id
group by
    opp.behandlingsmaned,
    brev.kategori_kode,
    brev.brev_kode
