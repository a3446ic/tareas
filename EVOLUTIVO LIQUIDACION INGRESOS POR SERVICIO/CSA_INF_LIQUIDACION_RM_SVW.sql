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
	    	IMPORTE_COBRADA,
	    	PERIODSEQ,
	    	POSITIONSEQ
	    	FROM EXT.INF_LIQUIDACION_RM)
			WITH READ ONLY STRUCTURED PRIVILEGE CHECK;