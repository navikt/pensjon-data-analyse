-- feilutbetalinger
-- finner antall måneder med feilutbetalinger ved å telle tilbakekrevinger
-- ser på diff mellom fom og tom på tilbakekrevingskravet etter dødsfallet
-- skjuler alle saker med mindre enn 10 årlige dødsfallopphør

-- Lagt til mapping mellom ID og landkode
with land_map as (
    select 1 as k_land_3_tegn_id, 'ABW' as land_3_tegn from dual union all
    select 2, 'AFG' from dual union all
    select 3, 'AGO' from dual union all
    select 4, 'AIA' from dual union all
    select 5, 'ALB' from dual union all
    select 6, 'AND' from dual union all
    select 7, 'ANT' from dual union all
    select 8, 'ARE' from dual union all
    select 9, 'ARG' from dual union all
    select 10, 'ARM' from dual union all
    select 11, 'ASM' from dual union all
    select 12, 'ATF' from dual union all
    select 13, 'ATG' from dual union all
    select 14, 'AUS' from dual union all
    select 15, 'AUT' from dual union all
    select 16, 'AZE' from dual union all
    select 17, 'BDI' from dual union all
    select 18, 'BEL' from dual union all
    select 19, 'BEN' from dual union all
    select 20, 'BFA' from dual union all
    select 21, 'BGD' from dual union all
    select 22, 'BGR' from dual union all
    select 23, 'BHR' from dual union all
    select 24, 'BHS' from dual union all
    select 25, 'BIH' from dual union all
    select 26, 'BLR' from dual union all
    select 27, 'BLZ' from dual union all
    select 28, 'BMU' from dual union all
    select 29, 'BOL' from dual union all
    select 30, 'BRA' from dual union all
    select 31, 'BRB' from dual union all
    select 32, 'BRN' from dual union all
    select 33, 'BTN' from dual union all
    select 34, 'BWA' from dual union all
    select 35, 'CAF' from dual union all
    select 36, 'CAN' from dual union all
    select 37, 'CCK' from dual union all
    select 38, 'CHE' from dual union all
    select 39, 'CHL' from dual union all
    select 40, 'CHN' from dual union all
    select 41, 'CIV' from dual union all
    select 42, 'CMR' from dual union all
    select 43, 'COD' from dual union all
    select 44, 'COG' from dual union all
    select 45, 'COK' from dual union all
    select 46, 'COL' from dual union all
    select 47, 'COM' from dual union all
    select 48, 'CPV' from dual union all
    select 49, 'CRI' from dual union all
    select 50, 'CSK' from dual union all
    select 51, 'CUB' from dual union all
    select 52, 'CXR' from dual union all
    select 53, 'CYM' from dual union all
    select 54, 'CYP' from dual union all
    select 55, 'CZE' from dual union all
    select 56, 'DDR' from dual union all
    select 57, 'DEU' from dual union all
    select 58, 'DJI' from dual union all
    select 59, 'DMA' from dual union all
    select 60, 'DNK' from dual union all
    select 61, 'DOM' from dual union all
    select 62, 'DZA' from dual union all
    select 63, 'ECU' from dual union all
    select 64, 'EGY' from dual union all
    select 65, '???' from dual union all
    select 66, 'PSE' from dual union all
    select 67, 'ERI' from dual union all
    select 68, 'ESH' from dual union all
    select 69, 'ESP' from dual union all
    select 70, 'EST' from dual union all
    select 71, 'ETH' from dual union all
    select 72, 'FIN' from dual union all
    select 73, 'FJI' from dual union all
    select 74, 'FLK' from dual union all
    select 75, 'FRA' from dual union all
    select 76, 'FRO' from dual union all
    select 77, 'FSM' from dual union all
    select 78, 'GAB' from dual union all
    select 79, 'GBR' from dual union all
    select 80, 'GEO' from dual union all
    select 81, 'GHA' from dual union all
    select 82, 'GIB' from dual union all
    select 83, 'GIN' from dual union all
    select 84, 'GLP' from dual union all
    select 85, 'GMB' from dual union all
    select 86, 'GNB' from dual union all
    select 87, 'GNQ' from dual union all
    select 88, 'GRC' from dual union all
    select 89, 'GRD' from dual union all
    select 90, 'GRL' from dual union all
    select 91, 'GTM' from dual union all
    select 92, 'GUF' from dual union all
    select 93, 'GUM' from dual union all
    select 94, 'GUY' from dual union all
    select 95, 'HKG' from dual union all
    select 96, 'HND' from dual union all
    select 97, 'HRV' from dual union all
    select 98, 'HTI' from dual union all
    select 99, 'HUN' from dual union all
    select 100, 'IDN' from dual union all
    select 101, 'IND' from dual union all
    select 102, 'IOT' from dual union all
    select 103, 'IRL' from dual union all
    select 104, 'IRN' from dual union all
    select 105, 'IRQ' from dual union all
    select 106, 'ISL' from dual union all
    select 107, 'ISR' from dual union all
    select 108, 'ITA' from dual union all
    select 109, 'JAM' from dual union all
    select 110, 'JOR' from dual union all
    select 111, 'JPN' from dual union all
    select 112, 'KAZ' from dual union all
    select 113, 'KEN' from dual union all
    select 114, 'KGZ' from dual union all
    select 115, 'KHM' from dual union all
    select 116, 'KIR' from dual union all
    select 117, 'KNA' from dual union all
    select 118, 'KOR' from dual union all
    select 119, 'KWT' from dual union all
    select 120, 'LAO' from dual union all
    select 121, 'LBN' from dual union all
    select 122, 'LBR' from dual union all
    select 123, 'LBY' from dual union all
    select 124, 'LCA' from dual union all
    select 125, 'LIE' from dual union all
    select 126, 'LKA' from dual union all
    select 127, 'LSO' from dual union all
    select 128, 'LTU' from dual union all
    select 129, 'LUX' from dual union all
    select 130, 'LVA' from dual union all
    select 131, 'MAC' from dual union all
    select 132, 'MAR' from dual union all
    select 133, 'MCO' from dual union all
    select 134, 'MDA' from dual union all
    select 135, 'MDG' from dual union all
    select 136, 'MDV' from dual union all
    select 137, 'MEX' from dual union all
    select 138, 'MHL' from dual union all
    select 139, 'MKD' from dual union all
    select 140, 'MLI' from dual union all
    select 141, 'MLT' from dual union all
    select 142, 'MMR' from dual union all
    select 143, 'MNG' from dual union all
    select 144, 'MNP' from dual union all
    select 145, 'MOZ' from dual union all
    select 146, 'MRT' from dual union all
    select 147, 'MSR' from dual union all
    select 148, 'MTQ' from dual union all
    select 149, 'MUS' from dual union all
    select 150, 'MWI' from dual union all
    select 151, 'MYS' from dual union all
    select 152, 'MYT' from dual union all
    select 153, 'NAM' from dual union all
    select 154, 'NCL' from dual union all
    select 155, 'NER' from dual union all
    select 156, 'NFK' from dual union all
    select 157, 'NGA' from dual union all
    select 158, 'NIC' from dual union all
    select 159, 'NIU' from dual union all
    select 160, 'NLD' from dual union all
    select 161, 'NOR' from dual union all
    select 162, 'NPL' from dual union all
    select 163, 'NRU' from dual union all
    select 164, 'NZL' from dual union all
    select 165, 'OMN' from dual union all
    select 166, 'PAK' from dual union all
    select 167, 'PAN' from dual union all
    select 168, 'PCN' from dual union all
    select 169, 'PER' from dual union all
    select 170, 'PHL' from dual union all
    select 171, 'PLW' from dual union all
    select 172, 'PNG' from dual union all
    select 173, 'POL' from dual union all
    select 174, 'PRI' from dual union all
    select 175, 'PRK' from dual union all
    select 176, 'PRT' from dual union all
    select 177, 'PRY' from dual union all
    select 178, 'PYF' from dual union all
    select 179, 'QAT' from dual union all
    select 180, 'REU' from dual union all
    select 181, 'ROU' from dual union all
    select 182, 'RUS' from dual union all
    select 183, 'RWA' from dual union all
    select 184, 'SAU' from dual union all
    select 185, 'SCG' from dual union all
    select 186, 'SDN' from dual union all
    select 187, 'SEN' from dual union all
    select 188, 'SGP' from dual union all
    select 189, 'SHN' from dual union all
    select 190, 'SJM' from dual union all
    select 191, 'SLB' from dual union all
    select 192, 'SLE' from dual union all
    select 193, 'SLV' from dual union all
    select 194, 'SMR' from dual union all
    select 195, 'SOM' from dual union all
    select 196, 'SPM' from dual union all
    select 197, 'STP' from dual union all
    select 198, 'SUN' from dual union all
    select 199, 'SUR' from dual union all
    select 200, 'SVK' from dual union all
    select 201, 'SVN' from dual union all
    select 202, 'SWE' from dual union all
    select 203, 'SWZ' from dual union all
    select 204, 'SYC' from dual union all
    select 205, 'SYR' from dual union all
    select 206, 'TCA' from dual union all
    select 207, 'TCD' from dual union all
    select 208, 'TGO' from dual union all
    select 209, 'THA' from dual union all
    select 210, 'TJK' from dual union all
    select 211, 'TKL' from dual union all
    select 212, 'TKM' from dual union all
    select 213, 'TLS' from dual union all
    select 214, 'TON' from dual union all
    select 215, 'TTO' from dual union all
    select 216, 'TUN' from dual union all
    select 217, 'TUR' from dual union all
    select 218, 'TUV' from dual union all
    select 219, 'TWN' from dual union all
    select 220, 'TZA' from dual union all
    select 221, 'UGA' from dual union all
    select 222, 'UKR' from dual union all
    select 223, 'UMI' from dual union all
    select 224, 'URY' from dual union all
    select 225, 'USA' from dual union all
    select 226, 'UZB' from dual union all
    select 227, 'VAT' from dual union all
    select 228, 'VCT' from dual union all
    select 229, 'VEN' from dual union all
    select 230, 'VGB' from dual union all
    select 231, 'VIR' from dual union all
    select 232, 'VNM' from dual union all
    select 233, 'VUT' from dual union all
    select 234, 'WAK' from dual union all
    select 235, 'WLF' from dual union all
    select 236, 'WSM' from dual union all
    select 237, 'XXX' from dual union all
    select 238, 'YEM' from dual union all
    select 239, 'YUG' from dual union all
    select 240, 'ZAF' from dual union all
    select 241, 'ZMB' from dual union all
    select 242, 'ZWE' from dual union all
    select 243, '349' from dual union all
    select 244, '546' from dual union all
    select 245, '556' from dual union all
    select 246, '669' from dual union all
    select 247, 'CZE' from dual union all
    select 248, 'SRB' from dual union all
    select 249, 'MNE' from dual union all
    select 250, 'GGY' from dual union all
    select 251, 'XXK' from dual union all
    select 252, 'SSD' from dual union all
    select 253, 'QEB' from dual union all
    select 254, 'BES' from dual union all
    select 256, 'CUW' from dual union all
    select 257, 'HMD' from dual union all
    select 258, 'IMN' from dual union all
    select 259, 'BVT' from dual union all
    select 260, 'JEY' from dual union all
    select 261, 'BLM' from dual union all
    select 262, 'MAF' from dual union all
    select 263, 'SXM' from dual union all
    select 264, 'ALA' from dual union all
    select 265, 'ATF' from dual union all
    select 266, 'ATA' from dual union all
    select 267, 'SJM' from dual union all
    select 268, 'REU' from dual union all
    select 269, 'SGS' from dual union all
    select 270, 'XUK' from dual
),

vedtak as (
    select
        p.dato_dod,
        to_char(p.dato_dod, 'YYYY') as dod_ar,
        land.land_3_tegn as bosatt,
        v_opph.dato_vedtak as opph_dato_vedtak,
        v_opph.dato_virk_fom as opph_dato_virk_fom,
        v_lop.dato_lopende_tom as lop_dato_lopende_tom,
        v_tilb.dato_virk_fom as tilb_dato_virk_fom,
        v_tilb.dato_virk_tom as tilb_dato_virk_tom,
        v_opph.sak_id,
        v_opph.vedtak_id as opph_vedtak_id,
        v_lop.vedtak_id as lop_vedtak_id,
        v_tilb.vedtak_id as tilb_vedtak_id
    from pen.t_vedtak v_opph
    left join pen.t_person p
        on
            p.person_id = v_opph.person_id
    left join pen.t_vedtak v_lop on v_opph.basert_pa_vedtak = v_lop.vedtak_id
    left join pen.t_vedtak v_tilb
        on
            v_tilb.sak_id = v_opph.sak_id
            and v_tilb.k_vedtak_t = 'TILBAKEKR'
            and v_tilb.k_sak_t = 'ALDER'
            and trunc(v_tilb.dato_virk_fom) >= p.dato_dod
    left join land_map land on land.k_land_3_tegn_id = coalesce(p.bostedsland, 161)
    where
        1 = 1
        and p.dato_dod is not null
        and p.dato_dod <= v_opph.dato_vedtak -- dødsfallet må ha skjedd før vedtaket. dette fjerner 12 vedtak fra 2024-2025
        and p.dato_dod >= to_date('01.01.2012', 'DD.MM.YYYY') -- fjerner 2009 og 2010
        and v_opph.k_sak_t = 'ALDER'
        and v_opph.k_vedtak_t = 'OPPHOR'
        and v_opph.basert_pa_vedtak in (select vedtak_id from pen.t_vedtak where dato_lopende_tom is not null) --noqa
        -- filteret over fjerner saker som aldri har hatt et løpende vedtak, feks planlagte pensjoner
),

finner_antall_mnd_feil as (
    select
        -- finner antall måneder tilbakekrevd, som tilsvarer antall mnd feil
        case
            when tilb_vedtak_id is not null and (tilb_dato_virk_tom + 1 > tilb_dato_virk_fom) -- fjerner 2 saker
                then months_between(tilb_dato_virk_tom + 1, tilb_dato_virk_fom)
        end as mnd_tilbakekrevd, -- ser ut som noen gamle tilbakekrevinger kun har virk fom, ikke virk tom ...
        case when tilb_vedtak_id is not null then 1 else 0 end as tilbakekrevd_flagg,
        dod_ar,
        bosatt
    from vedtak
),

aggregering as (
    select
        count(*) as antall_saker_totalt,
        sum(mnd_tilbakekrevd) as sum_tilbakekreving,
        sum(tilbakekrevd_flagg) as antall_saker_med_tilbakekreving,
        dod_ar,
        bosatt
    from finner_antall_mnd_feil
    group by
        dod_ar,
        bosatt
    having count(*) >= 4
    order by
        dod_ar desc,
        bosatt desc
),

final as (
    select
        dod_ar,
        bosatt,
        antall_saker_totalt,
        antall_saker_med_tilbakekreving,
        sum_tilbakekreving
    from aggregering
)

select * from final
order by
    dod_ar asc,
    antall_saker_totalt desc,
    bosatt desc
