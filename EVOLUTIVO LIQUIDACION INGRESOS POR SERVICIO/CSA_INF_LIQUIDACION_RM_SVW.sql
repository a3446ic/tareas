CREATE VIEW "EXT"."CSA_INF_LIQUIDACION_RM_SVW" ( "CODIGO", "MEDIADOR", "IDRECIBO", "SOCIEDAD", "PAIS", "CONCEPTO", "FECHA_RECIBO_LIQUIDACION", "NUM_FACTURA", "FECHA_FACTURA", "MESCIERRE", "MONEDA", "IMPORTE_COBRADA", "PERIODSEQ", "POSITIONSEQ" ) AS (SELECT CODIGO,
	    	MEDIADOR,
	    	IDRECIBO,
	    	SOCIEDAD,
	    	PAIS,
	    	CONCEPTO,
	    	FECHA_RECIBO_LIQUIDACION,
	    	NUM_FACTURA,
	    	FECHA_FACTURA,
	    	MESCIERRE,
	    	MONEDA,
	    	SUM(IMPORTE_COBRADA) IMPORTE_COBRADA,
	    	PERIODSEQ,
	    	POSITIONSEQ
	    	FROM EXT.INF_DETALLE_LIQUIDACION_RM
			GROUP BY CODIGO,MEDIADOR,IDRECIBO,SOCIEDAD,PAIS,CONCEPTO,FECHA_RECIBO_LIQUIDACION,NUM_FACTURA,FECHA_FACTURA,MESCIERRE,MONEDA,PERIODSEQ,POSITIONSEQ) 
			WITH READ ONLY STRUCTURED PRIVILEGE CHECK;