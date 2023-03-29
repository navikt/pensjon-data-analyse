with a as (
    SELECT
        count(*) as antall_grunnlag,
        extract(year from t.dato_opprettet) as aar,
        case 
            when t.k_land_3_tegn_id = 161 then 'Norge'
            when t.k_land_3_tegn_id is null then 'Ukjent'
            else 'Utland'
        end land
    FROM 
        pen.t_person_grunnlag p
        inner join pen.t_trygdetid_grnl t ON t.person_grunnlag_id = p.person_grunnlag_id
    WHERE
        --Bare innslag saksbehandler fører inn?
        t.k_grunnlag_kilde = 'SAKSB'
        AND 
        substr(t.opprettet_av,1,1) not in ('0','1','2','3','4','5','6','7','8','9') and substr(t.opprettet_av,2,1) in ('0','1','2','3','4','5','6','7','8','9') 
    GROUP BY
        extract(year from t.dato_opprettet),
        p.person_id,
        case 
            when t.k_land_3_tegn_id = 161 then 'Norge' -- 161 er basert at Norge er land 161 i alfabetisk rekkefølge. Kan potensielt endre seg 
            when t.k_land_3_tegn_id is null then 'Ukjent'
            else 'Utland'
        end
)

select antall_grunnlag, aar, land, count(*) frekvens
from a
group by antall_grunnlag, aar, land
order by aar, land, antall_grunnlag desc