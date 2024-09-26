select codigo_fiscal
,concat(concat(substring(codigo_fiscal,0,length(codigo_fiscal)-1),'-'),right(codigo_fiscal,1)) 
,substring(codigo_fiscal,length(codigo_fiscal)-1,1)
, case when substring(codigo_fiscal,length(codigo_fiscal)-1,1) != '-' then concat(concat(substring(codigo_fiscal,0,length(codigo_fiscal)-1),'-'),right(codigo_fiscal,1)) 
else codigo_fiscal end
from ext.proveedores_sapfi where pais_iso = 'CL'