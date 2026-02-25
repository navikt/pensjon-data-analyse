-- feilutbetalinger_dodsalder

-- viser gjennomsnittlig dødsalder for ulike land per år, for opphør av alderspensjonssaker
-- skjuler forekomster med færre enn 10 dødsfall

-- Lagt til mapping mellom ID og landnavn
with land_map as (
    select 1 as k_land_3_tegn_id, 'Aruba' as decode from dual union all
    select 2, 'Afghanistan' from dual union all
    select 3, 'Angola' from dual union all
    select 4, 'Anguilla' from dual union all
    select 5, 'Albania' from dual union all
    select 6, 'Andorra' from dual union all
    select 7, 'De Nederlandske Antillene' from dual union all
    select 8, 'De Forente Arabiske Emirater' from dual union all
    select 9, 'Argentina' from dual union all
    select 10, 'Armenia' from dual union all
    select 11, 'Am. Samoa' from dual union all
    select 12, 'Franske Antiller' from dual union all
    select 13, 'Antigua Og Barbuda' from dual union all
    select 14, 'Australia' from dual union all
    select 15, 'Østerrike' from dual union all
    select 16, 'Aserbajdsjan' from dual union all
    select 17, 'Burundi' from dual union all
    select 18, 'Belgia' from dual union all
    select 19, 'Benin' from dual union all
    select 20, 'Burkina Faso' from dual union all
    select 21, 'Bangladesh' from dual union all
    select 22, 'Bulgaria' from dual union all
    select 23, 'Bahrain' from dual union all
    select 24, 'Bahamas' from dual union all
    select 25, 'Bosnia-Hercegovina' from dual union all
    select 26, 'Belarus' from dual union all
    select 27, 'Belize' from dual union all
    select 28, 'Bermuda' from dual union all
    select 29, 'Bolivia' from dual union all
    select 30, 'Brasil' from dual union all
    select 31, 'Barbados' from dual union all
    select 32, 'Brunei' from dual union all
    select 33, 'Bhutan' from dual union all
    select 34, 'Botswana' from dual union all
    select 35, 'Den Sentralafrikanske Republikk' from dual union all
    select 36, 'Canada' from dual union all
    select 37, 'Kokosøyene (Keelingøyene)' from dual union all
    select 38, 'Sveits' from dual union all
    select 39, 'Chile' from dual union all
    select 40, 'Kina' from dual union all
    select 41, 'Elfenbenskysten' from dual union all
    select 42, 'Kamerun' from dual union all
    select 43, 'Kongo' from dual union all
    select 44, 'Kongo-Brazzaville' from dual union all
    select 45, 'Cookøyene' from dual union all
    select 46, 'Colombia' from dual union all
    select 47, 'Komorene' from dual union all
    select 48, 'Kapp Verde' from dual union all
    select 49, 'Costa Rica' from dual union all
    select 50, 'Tsjekkoslovakia' from dual union all
    select 51, 'Cuba' from dual union all
    select 52, 'Christmasøya' from dual union all
    select 53, 'Caymanøyene' from dual union all
    select 54, 'Kypros' from dual union all
    select 55, 'Den Tsjekkiske Rep.' from dual union all
    select 56, 'Tyskland (Øst)' from dual union all
    select 57, 'Tyskland' from dual union all
    select 58, 'Djibouti' from dual union all
    select 59, 'Dominica' from dual union all
    select 60, 'Danmark' from dual union all
    select 61, 'Den Dominikanske Republikk' from dual union all
    select 62, 'Algerie' from dual union all
    select 63, 'Ecuador' from dual union all
    select 64, 'Egypt' from dual union all
    select 65, 'Uoppgitt/Ukjent' from dual union all
    select 66, 'Palestina' from dual union all
    select 67, 'Eritrea' from dual union all
    select 68, 'Vest-Sahara' from dual union all
    select 69, 'Spania' from dual union all
    select 70, 'Estland' from dual union all
    select 71, 'Etiopia' from dual union all
    select 72, 'Finland' from dual union all
    select 73, 'Fiji' from dual union all
    select 74, 'Falklandsøyene' from dual union all
    select 75, 'Frankrike' from dual union all
    select 76, 'Færøyene' from dual union all
    select 77, 'Mikronesiaføderasjonen' from dual union all
    select 78, 'Gabon' from dual union all
    select 79, 'Storbritannia' from dual union all
    select 80, 'Georgia' from dual union all
    select 81, 'Ghana' from dual union all
    select 82, 'Gibraltar' from dual union all
    select 83, 'Guinea' from dual union all
    select 84, 'Guadeloupe' from dual union all
    select 85, 'Gambia' from dual union all
    select 86, 'Guinea-Bissau' from dual union all
    select 87, 'Ekvatorial-Guinea' from dual union all
    select 88, 'Hellas' from dual union all
    select 89, 'Grenada' from dual union all
    select 90, 'Grønland' from dual union all
    select 91, 'Guatemala' from dual union all
    select 92, 'Fransk Guyana' from dual union all
    select 93, 'Guam' from dual union all
    select 94, 'Guyana' from dual union all
    select 95, 'Hongkong' from dual union all
    select 96, 'Honduras' from dual union all
    select 97, 'Kroatia' from dual union all
    select 98, 'Haiti' from dual union all
    select 99, 'Ungarn' from dual union all
    select 100, 'Indonesia' from dual union all
    select 101, 'India' from dual union all
    select 102, 'Det britiske territoriet i Indiahavet' from dual union all
    select 103, 'Irland' from dual union all
    select 104, 'Iran' from dual union all
    select 105, 'Irak' from dual union all
    select 106, 'Island' from dual union all
    select 107, 'Israel' from dual union all
    select 108, 'Italia' from dual union all
    select 109, 'Jamaica' from dual union all
    select 110, 'Jordan' from dual union all
    select 111, 'Japan' from dual union all
    select 112, 'Kasakhstan' from dual union all
    select 113, 'Kenya' from dual union all
    select 114, 'Kirgisistan' from dual union all
    select 115, 'Kambodsja' from dual union all
    select 116, 'Kiribati' from dual union all
    select 117, 'Saint Kitts og Nevis' from dual union all
    select 118, 'Sør-Korea' from dual union all
    select 119, 'Kuwait' from dual union all
    select 120, 'Laos' from dual union all
    select 121, 'Libanon' from dual union all
    select 122, 'Liberia' from dual union all
    select 123, 'Libya' from dual union all
    select 124, 'Saint Lucia' from dual union all
    select 125, 'Liechtenstein' from dual union all
    select 126, 'Sri Lanka' from dual union all
    select 127, 'Lesotho' from dual union all
    select 128, 'Litauen' from dual union all
    select 129, 'Luxembourg' from dual union all
    select 130, 'Latvia' from dual union all
    select 131, 'Macao' from dual union all
    select 132, 'Marokko' from dual union all
    select 133, 'Monaco' from dual union all
    select 134, 'Moldova' from dual union all
    select 135, 'Madagaskar' from dual union all
    select 136, 'Maldivene' from dual union all
    select 137, 'Mexico' from dual union all
    select 138, 'Marshalløyene' from dual union all
    select 139, 'Nord-Makedonia' from dual union all
    select 140, 'Mali' from dual union all
    select 141, 'Malta' from dual union all
    select 142, 'Myanmar (Burma)' from dual union all
    select 143, 'Mongolia' from dual union all
    select 144, 'Nord-Marianene' from dual union all
    select 145, 'Mosambik' from dual union all
    select 146, 'Mauritania' from dual union all
    select 147, 'Montserrat' from dual union all
    select 148, 'Martinique' from dual union all
    select 149, 'Mauritius' from dual union all
    select 150, 'Malawi' from dual union all
    select 151, 'Malaysia' from dual union all
    select 152, 'Mayotte' from dual union all
    select 153, 'Namibia' from dual union all
    select 154, 'Ny-Caledonia' from dual union all
    select 155, 'Niger' from dual union all
    select 156, 'Norfolkøya' from dual union all
    select 157, 'Nigeria' from dual union all
    select 158, 'Nicaragua' from dual union all
    select 159, 'Niue' from dual union all
    select 160, 'Nederland' from dual union all
    select 161, 'Norge' from dual union all
    select 162, 'Nepal' from dual union all
    select 163, 'Nauru' from dual union all
    select 164, 'New Zealand' from dual union all
    select 165, 'Oman' from dual union all
    select 166, 'Pakistan' from dual union all
    select 167, 'Panama' from dual union all
    select 168, 'Pitcairn' from dual union all
    select 169, 'Peru' from dual union all
    select 170, 'Filippinene' from dual union all
    select 171, 'Palau' from dual union all
    select 172, 'Papua Ny-Guinea' from dual union all
    select 173, 'Polen' from dual union all
    select 174, 'Puerto Rico' from dual union all
    select 175, 'Nord-Korea' from dual union all
    select 176, 'Portugal' from dual union all
    select 177, 'Paraguay' from dual union all
    select 178, 'Fransk Polynesia' from dual union all
    select 179, 'Qatar' from dual union all
    select 180, 'Le Port' from dual union all
    select 181, 'Romania' from dual union all
    select 182, 'Russland' from dual union all
    select 183, 'Rwanda' from dual union all
    select 184, 'Saudi-Arabia' from dual union all
    select 185, 'Serbia Og Montenegro' from dual union all
    select 186, 'Sudan' from dual union all
    select 187, 'Senegal' from dual union all
    select 188, 'Singapore' from dual union all
    select 189, 'Sankt Helena, Ascension og Tristan da Cunha' from dual union all
    select 190, 'Svalbard Og Jan Mayen' from dual union all
    select 191, 'Salomonøyene' from dual union all
    select 192, 'Sierra Leone' from dual union all
    select 193, 'El Salvador' from dual union all
    select 194, 'San Marino' from dual union all
    select 195, 'Somalia' from dual union all
    select 196, 'Saint-Pierre og Miquelon' from dual union all
    select 197, 'Sao Tome Og Principe' from dual union all
    select 198, 'Sovjetunionen' from dual union all
    select 199, 'Suriname' from dual union all
    select 200, 'Slovakia' from dual union all
    select 201, 'Slovenia' from dual union all
    select 202, 'Sverige' from dual union all
    select 203, 'Eswatini' from dual union all
    select 204, 'Seychellene' from dual union all
    select 205, 'Syria' from dual union all
    select 206, 'Turks- og Caicosøyene' from dual union all
    select 207, 'Tchad' from dual union all
    select 208, 'Togo' from dual union all
    select 209, 'Thailand' from dual union all
    select 210, 'Tadsjikistan' from dual union all
    select 211, 'Tokelau' from dual union all
    select 212, 'Turkmenistan' from dual union all
    select 213, 'Øst-Timor' from dual union all
    select 214, 'Tonga' from dual union all
    select 215, 'Trinidad Og Tobago' from dual union all
    select 216, 'Tunisia' from dual union all
    select 217, 'Tyrkia' from dual union all
    select 218, 'Tuvalu' from dual union all
    select 219, 'Taiwan' from dual union all
    select 220, 'Tanzania' from dual union all
    select 221, 'Uganda' from dual union all
    select 222, 'Ukraina' from dual union all
    select 223, 'Usas ytre småøyer' from dual union all
    select 224, 'Uruguay' from dual union all
    select 225, 'USA' from dual union all
    select 226, 'Usbekistan' from dual union all
    select 227, 'Vatikanstaten' from dual union all
    select 228, 'Saint Vincent og Grenadinene' from dual union all
    select 229, 'Venezuela' from dual union all
    select 230, 'De britiske Jomfruøyer' from dual union all
    select 231, 'De amerikanske Jomfruøyer' from dual union all
    select 232, 'Vietnam' from dual union all
    select 233, 'Vanuatu' from dual union all
    select 234, 'Wakøya' from dual union all
    select 235, 'Wallis og Futuna' from dual union all
    select 236, 'Samoa' from dual union all
    select 237, 'Statløs' from dual union all
    select 238, 'Yemen' from dual union all
    select 239, 'Jugoslavia' from dual union all
    select 240, 'Sør-Afrika' from dual union all
    select 241, 'Zambia' from dual union all
    select 242, 'Zimbabwe' from dual union all
    select 243, 'Spanske Omr. Afrika' from dual union all
    select 244, 'Sikkim' from dual union all
    select 245, 'Yemen' from dual union all
    select 246, 'Panamakanalsonen' from dual union all
    select 247, 'Tsjekkia' from dual union all
    select 248, 'Serbia' from dual union all
    select 249, 'Montenegro' from dual union all
    select 250, 'Guernsey' from dual union all
    select 251, 'Kosovo' from dual union all
    select 252, 'Sør-Sudan' from dual union all
    select 253, 'Quebec' from dual union all
    select 254, 'Bonaire, Sint Eustatius og Saba' from dual union all
    select 256, 'Curacao' from dual union all
    select 257, 'Heard- og McDonaldøyene' from dual union all
    select 258, 'Isle of Man' from dual union all
    select 259, 'Bouvetøya' from dual union all
    select 260, 'Jersey' from dual union all
    select 261, 'Saint Barthelemy' from dual union all
    select 262, 'Saint Martin' from dual union all
    select 263, 'Sint Maarten' from dual union all
    select 264, 'Åland' from dual union all
    select 265, 'De franske territoriene i sør' from dual union all
    select 266, 'Antarktis' from dual union all
    select 267, 'Svalbard og Jan Mayen' from dual union all
    select 268, 'Reunion' from dual union all
    select 269, 'Sør-Georgia og Sør-Sandwichøyene' from dual union all
    select 270, 'Uoppgitt/Ukjent' from dual
),
vedtak as (
    select
        p.person_id,
        p.dato_dod,
        p.dato_fodsel,
        land.decode as bostedsland,
        floor(months_between(p.dato_dod, p.dato_fodsel) / 12) as dodsalder,
        -- Velg den nyeste saken per person
        max(v.sak_id) as sak_id,
        max(v.vedtak_id) as vedtak_id,
        max(v.dato_vedtak) as dato_vedtak,
        max(v.dato_virk_fom) as dato_virk_fom
    from pen.t_vedtak v
    inner join pen.t_person p on p.person_id = v.person_id
    left join land_map land on land.k_land_3_tegn_id = coalesce(p.bostedsland, 161)
    where
        1 = 1
        and v.k_sak_t = 'ALDER'
        and v.k_vedtak_t = 'OPPHOR'
        and p.dato_dod is not null
    group by
        p.person_id,
        p.dato_dod,
        p.dato_fodsel,
        land.decode
),

-- gjennomsnitt_dodsalder_per_ar as (
--     -- alternativ visning som viser per år
--     select
--         avg(dodsalder) as gjennomsnitt_dodsalder,
--         extract(year from dato_dod) as dodsar,
--         bostedsland
--     from vedtak
--     group by extract(year from dato_dod), bostedsland
-- ),

gjennomsnitt_dodsalder as (
    select
        round(avg(dodsalder), 2) as gjennomsnitt_dodsalder,
        count(*) as antall_personer,
        bostedsland
    from vedtak
    group by bostedsland
    having count(*) > 10
)

-- select * from gjennomsnitt_dodsalder_per_ar order by dodsar desc, bostedsland, gjennomsnitt_dodsalder desc;
select * from gjennomsnitt_dodsalder
order by gjennomsnitt_dodsalder desc
