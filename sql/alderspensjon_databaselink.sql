CREATE DATABASE LINK "lesekopien_til_dvh_prod"
CONNECT TO "" IDENTIFIED BY ""
USING '(DESCRIPTION = (ADDRESS= (PROTOCOL=TCP) (HOST=dmv14-scan.adeo.no) (PORT=1521)) (CONNECT_DATA= (SERVICE_NAME=PEN_LES)))';



create table dataprodukt_alderspensjon_vedtak as 
select * from pen_dataprodukt.dataprodukt_alderspensjon_vedtak@lesekopien_til_dvh_prod;

--drop table dataprodukt_alderspensjon_vedtak purge;


create table dataprodukt_alderspensjon_beregning as 
select * from pen_dataprodukt.dataprodukt_alderspensjon_beregning@lesekopien_til_dvh_prod;

--drop table dataprodukt_alderspensjon_belop purge;


create table dataprodukt_alderspensjon_belop as 
select * from pen_dataprodukt.dataprodukt_alderspensjon_belop@lesekopien_til_dvh_prod;

--drop table dataprodukt_alderspensjon_belop purge;


select 1 from dual;

drop database link lesekopien_til_dvh_prod;



-- Sett inn kode for views 
-- create view wendelboe_pesys_alder_I_current

-- create view wendelboe_pesys_alder_II_current