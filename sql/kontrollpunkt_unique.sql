WITH UniquePeople AS (
    SELECT
        DISTINCT kh.kravhode_id,
        extract(month from kp.dato_opprettet) as mnd,
        extract(year from kp.dato_opprettet) as yr,
        dkh.dekode as kravtype,
        (CASE 
            WHEN substr(kh.opprettet_av,1,1) IN ('0','1','2','3','4','5','6','7','8','9') THEN 'Bruker'
            WHEN substr(kh.opprettet_av,1,1) NOT IN ('0','1','2','3','4','5','6','7','8','9') 
                AND substr(kh.opprettet_av,2,1) IN ('0','1','2','3','4','5','6','7','8','9') THEN 'Saksbehandler' 
            ELSE kh.opprettet_av 
        END) AS behandler,
        ds.dekode as sakstype,
        dks.dekode as kravstatus,
        dkb.dekode as behandlingstype,
        kp.k_kontrollpnkt_t as kontrollpunkt,
        dkp.dekode_tekst as kontrollpunkt_forklaring
    FROM
        pen.t_kontrollpunkt kp
    INNER JOIN
        pen.t_kravhode kh ON kh.kravhode_id = kp.kravhode_id
    INNER JOIN
        pen.t_sak s ON s.sak_id = kp.sak_id
    INNER JOIN
        pen.t_k_krav_gjelder dkh ON dkh.k_krav_gjelder = kh.k_krav_gjelder
    INNER JOIN
        pen.t_k_sak_t ds ON ds.k_sak_t = s.k_sak_t
    INNER JOIN
        pen.t_k_kontrollpnkt_t dkp ON dkp.k_kontrollpnkt_t = kp.k_kontrollpnkt_t
    INNER JOIN
        pen.t_k_krav_s dks ON dks.k_krav_s = kh.k_krav_s
    INNER JOIN
        pen.t_k_behandling_t dkb ON dkb.k_behandling_t = kh.k_behandling_t
    WHERE
        s.k_sak_t = 'ALDER'
        AND kh.k_krav_gjelder IN ('FORSTEG_BH', 'F_BH_MED_UTL')
        AND kh.k_krav_s != 'AVBRUTT'
)
SELECT
    mnd,
    yr,
    kravtype,
    behandler,
    sakstype,
    kravstatus,
    behandlingstype,
    kontrollpunkt,
    kontrollpunkt_forklaring,
    COUNT(*) AS antall
FROM
    UniquePeople
GROUP BY
    mnd,
    yr,
    kravtype,
    behandler,
    sakstype,
    kravstatus,
    behandlingstype,
    kontrollpunkt,
    kontrollpunkt_forklaring
