SELECT CRH.IDMEDIADOR || '-' || CRH.IDSUBCLAVE CODIGO
,PA.FIRSTNAME || ' ' || PA.LASTNAME MEDIADOR
,SC.SOCIEDAD
,NULL IDRECIBO --¿?
,'Comisiones por servicios' CONCEPTO
,ADD_DAYS(ADD_MONTHS(TO_DATE(MESCIERRE),1),-1) AS FECHA_RECIBO_LIQUIDACION --¿?
,CRH.ESTADOREG ESTADO
,CRH.NUMFACTURA REFERENCIA
,CRH.FEC_MOVIMIENTO FECHA_FACTURA --¿?
,(SELECT EXT.LIB_GLOBAL_CESCE:getCurrency(198,'').currencyISO FROM DUMMY) MONEDA
,CRH.IMPORTE_COBRADA IMPORTE --¿?
,CRH.*
FROM EXT.COMISIONES_RM_HIST CRH 
LEFT JOIN CS_POSITION PO ON CRH.IDMEDIADOR || '-' || CRH.IDSUBCLAVE = PO.NAME AND PO.REMOVEDATE >= '2200-01-01 00:00:00' AND PO.ISLAST = 1
LEFT JOIN CS_PARTICIPANT PA ON PA.PAYEESEQ = PO.PAYEESEQ AND PA.REMOVEDATE >= '2200-01-01 00:00:00' AND PA.ISLAST = 1
LEFT JOIN EXT.SOCIEDADES_CESCE SC ON CRH.IDPAIS = SC.IDPAIS
WHERE CRH.IDMEDIADOR = '0181' AND CRH.ESTADOREG = 'ENVIADA';