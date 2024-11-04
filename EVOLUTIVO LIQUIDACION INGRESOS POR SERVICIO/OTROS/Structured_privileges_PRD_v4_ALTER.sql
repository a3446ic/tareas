ALTER STRUCTURED PRIVILEGE EXT.REP_SECURITY_EXT
FOR SELECT
ON EXT.CSA_RECIBOSFACTURAS_SVW,
	EXT.CSA_RECIBOSFACTURAS_V2_SVW,
	EXT.CSA_RECIBOSPRIMA_SVW,
	EXT.CSA_DETALLE_RECIBOS_FACTURA_SVW,
	EXT.CSA_INF_LIQUIDACION_RM_SVW,
	EXT.CSA_INF_DETALLE_LIQUIDACION_RM_SVW
WHERE (positionseq, periodseq) in ( 
	select DESCENDANTPOSITIONSEQ, periodseq
	from csa_pareportingdimension 
	where (ancestorpa_sk, periodseq) in (
		select pa_sk, periodseq 
		from csa_padimension pad
		where  userid = SESSION_CONTEXT('APPLICATIONUSER')
			or userid in  (	select par.userid from CSA_DataSecurity ds
									inner join CS_POSITION pos on ds.VALUE = pos.RULEELEMENTOWNERSEQ and pos.removeDate=TO_DATE('22000101','YYYYMMDD') and pos.islast=1
									Inner join CS_PARTICIPANT par on pos.PAYEESEQ = par.PAYEESEQ and par.removeDate=TO_DATE('22000101','YYYYMMDD') and par.islast=1
								where  ds.userid = SESSION_CONTEXT('APPLICATIONUSER') 
								and ds.securityType='POS')
			or exists ( 
				select 1 
				from CSA_DataSecurity 
				where userid = SESSION_CONTEXT('APPLICATIONUSER') 
					and securityType='ALL' 
					and removeDate=TO_DATE('22000101','YYYYMMDD') 
				)
	)
);
