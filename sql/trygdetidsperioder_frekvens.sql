WITH personer as(
    SELECT
        EXTRACT(YEAR FROM p.dato_fodsel) ar_fodsel,
        CASE
        WHEN t.k_land_3_tegn_id = 161 THEN
        'Norge'
        ELSE
        'Utland'
        END                              land,
        COALESCE(COUNT(t.trygdetid_grnl_id), 0) antall
    FROM
        pen.t_person_grunnlag p
        LEFT JOIN pen.t_trygdetid_grnl t ON t.person_grunnlag_id = p.person_grunnlag_id
    WHERE
        1936 <= EXTRACT(YEAR FROM p.dato_fodsel)
    AND
        EXTRACT(YEAR FROM p.dato_fodsel) <= to_char(sysdate, 'YYYY') - 68
    GROUP BY
        p.fnr_fk,
        EXTRACT(YEAR FROM p.dato_fodsel),
        CASE
        WHEN t.k_land_3_tegn_id = 161 THEN
            'Norge'
        ELSE
        'Utland'
        END
)
SELECT ar_fodsel, land, antall, count(*) frekvens
FROM personer
GROUP BY ar_fodsel, land, antall
order by ar_fodsel desc, land, antall asc