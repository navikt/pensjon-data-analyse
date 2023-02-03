select pp.FNR_FK,oms.*
from POPP.T_OMSORG oms join popp.T_PERSON pp on pp.PERSON_ID = oms.PERSON_ID
where oms.K_OMSORG_S = 'G'
and oms.AR = 2021