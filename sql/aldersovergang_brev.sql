select
    opp.behandlingsmaned as behandlingsmaned,
    brev.kategori_kode as kategori_kode,
    brev.brev_kode as brev_kode,
    count(brev_kode) as antall
from pen.t_aldersovergang_oppsummering opp
join pen.t_aldersovergang_oppsummering_brev brev on opp.id = brev.aldersovergang_oppsummering_id
group by
    opp.behandlingsmaned,
    brev.kategori_kode,
    brev.brev_kode
