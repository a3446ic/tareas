CREATE OR REPLACE PROCEDURE "EXT"."SP_GENERAR_RECIBOS_FACTURAS_NOPOST" (IN pPlRunSeq BIGINT, IN actualizaOrder VARCHAR(1)  DEFAULT 'N') LANGUAGE SQLSCRIPT AS
BEGIN
-- v2 se cambia CR.VALUE por CR.GENERICNUMBER3 que tiene el signo segun estado del recibo
-- v3 check CR.GENERICNUMBER3
-- v4 REDEPLOY
-- v5 cambio valor obtenido del credito para el detalle (GN3 para comisiones, Value para otros conceptos)
-- v6 Filtro para los depositos de Ajustes Manuales y Cambio en la busqueda de las formas de pago para tener en cuenta si es Factura propia o no
-- v7 Actualizar VIA PAGO V para PORTUGAL
-- v8 Modificar consulta de PAYMENTS y DEPOSIT por necesitada de union por tipo de valor porque se duplicaban en caso de monedas diferentes y se filtran IMPORTE_DET <> 0
-- v9 Se generan detalles de recibos de facturas para depositos y pagos sin calculos
-- v10 Se genera la fecha de alta con el día previo
-- v12

-- actualizaOrder 'S' o 'N' para enviar datos al order
	DECLARE IdRecibo BIGINT;						
	DECLARE existeFactura INTEGER;
	DECLARE sumaImporte DECIMAL(18,3);
	DECLARE referenciaFactura VARCHAR(16);

	DECLARE io_contador Number := 0;
	DECLARE i_Tenant VARCHAR(127);
	DECLARE cReportTable CONSTANT VARCHAR(50) := 'SP_GENERAR_RECIBOS_FACTURAS';
	DECLARE vIdFactura BIGINT;
	DECLARE batchname VARCHAR(50);
	DECLARE cVersion CONSTANT VARCHAR(2) := '12';

-- ----------------------------------------------------------------------------------------------------
-- Cursor para insertar facturas a partir de los recibos
-- ----------------------------------------------------------------------------------------------------

	DECLARE CURSOR recibos FOR
	SELECT r.*
	FROM EXT.RECIBOS_FACTURAS r
	LEFT JOIN EXT.MODIFICAR_MEDIADOR m
	ON m.POSITIONNAME = r.POSITIONNAME
	WHERE ESTADO = 'PENDIENTE'
	AND (m.FACT_PROPIA = 0 OR m.FACT_PROPIA IS NULL);

-- ----------------------------------------------------------------------------------------------------
-- Cursor: Pagos con fecha de POST del Periodo no liquidados previamente en la tabla de RECIBOS
-- ----------------------------------------------------------------------------------------------------
/*
	DECLARE CURSOR CUR_payment FOR 
	SELECT pay.PAYMENTSEQ, pay.PERIODSEQ, pay.VALUE, dep.GENERICATTRIBUTE1,
	pay.EARNINGCODEID, ut1.NAME, ec.DESCRIPTION, fp.VIAPAGO, sc.IDSOCIEDAD, MED.*
	FROM CS_PAYMENT pay 
	inner join CS_APPLDEPOSITPAYMENTTRACE payapd on pay.PAYMENTSEQ = payapd.PAYMENTSEQ
	inner join CS_APPLIEDDEPOSIT apd on payapd.APPLIEDDEPOSITSEQ = apd.APPLIEDDEPOSITSEQ
	inner join CS_DEPOSITAPPLDEPOSITTRACE apddep ON apd.APPLIEDDEPOSITSEQ = apddep.APPLIEDDEPOSITSEQ
	inner join CS_DEPOSIT dep on apddep.DEPOSITSEQ = dep.DEPOSITSEQ
	LEFT JOIN CS_EARNINGCODE ec on pay.EARNINGCODEID = ec.EARNINGCODEID 
	LEFT JOIN EXT.MODIFICAR_MEDIADOR med on pay.POSITIONSEQ = med.POSITIONSEQ 
	LEFT JOIN CS_UNITTYPE ut1 on pay.UNITTYPEFORVALUE = ut1.UNITTYPESEQ 
	LEFT JOIN EXT.FORMAS_PAGO fp on med.FORMA_PAGO = fp.IDFORMAPAGO 
	LEFT JOIN EXT.SOCIEDADES_CESCE sc on sc.IDPAIS = (CASE WHEN dep.GENERICATTRIBUTE1='F' THEN 28 Else med.COD_PAIS END)
	WHERE pay.PERIODSEQ = (SELECT PERIODSEQ FROM TCMP.CS_PLRUN where   PIPELINERUNSEQ = pPlRunSeq  ) --20547673299878462
	--AND pay.POSTPIPELINERUNDATE <> NULL   
	AND pay.PAYMENTSEQ NOT IN (SELECT PAYMENTSEQ FROM EXT.RECIBOS_FACTURAS WHERE PAYMENTSEQ IS NOT NULL);

*/

	----------------------------- HANDLER EXCEPTION -------------------------
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'SQL ERROR_MESSAGE: ' ||
						IFNULL(::SQL_ERROR_MESSAGE,'') || '. SQL_ERROR_CODE: ' || ::SQL_ERROR_CODE, cReportTable, io_contador);
		END;

	---------------------------------------------------------------------------
		

		SELECT EXT.LIB_GLOBAL_CESCE:getTenantID() INTO i_Tenant FROM DUMMY;

		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INICIO PROCEDIMIENTO v07 with SESSION_USER '|| SESSION_USER, cReportTable, io_contador);

		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Se insertan datos de PAGOS en tabla EXT.RECIBOS_FACTURAS ' , cReportTable, io_contador);

		-- Se insertan los recibos de facturas --
		INSERT INTO EXT.RECIBOS_FACTURAS 
		(
			FECHA_ALTA,
			CONCEPTO,
			IMPORTE,
			MONEDA,
			ESTADO,
			CODPROVEEDOR,
			POSITIONNAME,
			NOMBRE_MEDIADOR,
			NUM_IDENTIFICACION,
			TIPO_ID_FISCAL,
			TAXID,
			BUSINESSUNITNAME,
			SOCIEDAD,
			CONDPAGO,
			VIAPAGO,
			MATERIAL,
			CENTROCOSTE,
			POSITIONSEQ,
			PERIODSEQ,
			PAYMENTSEQ,
			CREATEDATE,
			MODIF_DATE,
			MODIF_USER
		)
		SELECT
			--CURRENT_DATE,
			ADD_DAYS (CURRENT_DATE, -1) as FECHA_ALTA,
			CASE WHEN pay.EARNINGCODEID = 'SIN_PAGO' THEN 'SIN_PAGO' ELSE  ec.DESCRIPTION END as DESCRIPTION, --CONCEPTO
			pay.VALUE,
			ut1.NAME,
			'NUEVO' as ESTADO,
			(CASE WHEN dep.GENERICATTRIBUTE1 IS NOT NULL THEN dep.GENERICATTRIBUTE1 || med.IDHOST ELSE '' END),
			med.POSITIONNAME,
			(CASE WHEN med.NOMBRE IS NULL THEN '' ELSE med.NOMBRE || ' ' END) || med."APELLIDO/RAZON_SOCIAL",
			med.NUM_IDENTIFICACION,
			med.TIPO_ID_FISCAL,
			med.IRPF,
			med.BUSINESSUNIT,
			sc.IDSOCIEDAD,
--			'000' || fp.VIAPAGO,
--			fp.VIAPAGO,
			'000' || CASE WHEN med.BUSINESSUNIT = 'Portugal' then 'V' else fp.VIAPAGO end as COND_PAGO,
			CASE WHEN med.BUSINESSUNIT = 'Portugal' then 'V' else fp.VIAPAGO end as VIAPAGO,
			pay.EARNINGCODEID,
			'' as CENTROCOSTE,
			med.POSITIONSEQ,
			pay.PERIODSEQ,
			pay.PAYMENTSEQ,
			CURRENT_TIMESTAMP as CREATEDATE,
			CURRENT_TIMESTAMP as MODIF_DATE,
			SESSION_USER as MODIF_USER
		FROM CS_PAYMENT pay 
			INNER JOIN CS_APPLDEPOSITPAYMENTTRACE payapd on pay.PAYMENTSEQ = payapd.PAYMENTSEQ
			INNER JOIN CS_APPLIEDDEPOSIT apd on payapd.APPLIEDDEPOSITSEQ = apd.APPLIEDDEPOSITSEQ
			INNER JOIN CS_KPRDEPOSITAPPLDEPOSITTRACE apddep ON apd.APPLIEDDEPOSITSEQ = apddep.APPLIEDDEPOSITSEQ AND
															apd.UNITTYPEFORVALUE= apddep.UNITTYPEFORCONTRIBUTIONVALUE --V08
															AND apddep.KPRSEQ = pay.POSTPIPELINERUNSEQ
			INNER JOIN CS_KPRDEPOSIT dep on apddep.DEPOSITSEQ = dep.DEPOSITSEQ 
			AND dep.KPRSEQ = pay.POSTPIPELINERUNSEQ
			and dep.NAME <> 'D_SP_Ajustes_Manuales'
			LEFT JOIN CS_EARNINGCODE ec on pay.EARNINGCODEID = ec.EARNINGCODEID and ec.REMOVEDATE = TO_DATE('22000101','yyyymmdd') 
			LEFT JOIN EXT.MODIFICAR_MEDIADOR med on pay.POSITIONSEQ = med.POSITIONSEQ 
			LEFT JOIN CS_UNITTYPE ut1 on pay.UNITTYPEFORVALUE = ut1.UNITTYPESEQ and ut1.REMOVEDATE = TO_DATE('22000101','yyyymmdd')
			LEFT JOIN EXT.FORMAS_PAGO fp on ( med.FORMA_PAGO || case when med.FACT_PROPIA = 1 then '1' else '0' end ) = fp.IDFORMAPAGO --se tiene en cuenta si es facturacion propia o no
			LEFT JOIN EXT.SOCIEDADES_CESCE sc on sc.IDPAIS = (CASE WHEN dep.GENERICATTRIBUTE1='F' THEN 28 Else med.COD_PAIS END)
		WHERE pay.PERIODSEQ = (SELECT PERIODSEQ FROM TCMP.CS_PLRUN where   PIPELINERUNSEQ = pPlRunSeq )
			AND pay.POSTPIPELINERUNDATE <> NULL
			AND pay.PAYMENTSEQ NOT IN (SELECT PAYMENTSEQ FROM EXT.RECIBOS_FACTURAS WHERE PAYMENTSEQ IS NOT NULL);
	
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Insertados ' || To_VARCHAR(::ROWCOUNT)  || ' REGISTROS EN EXT.RECIBOS_FACTURAS ' , cReportTable, io_contador);
		
-- ----------------------------------------------------------------------------------------------------
-- Se obtienen los detalles de recibos de facturas insertados anteriormente
-- ----------------------------------------------------------------------------------------------------
		
		-- Obtener los datos en una tabla previa
		
		IF (SELECT TABLE_NAME FROM SYS.TABLES where SCHEMA_NAME='EXT' and TABLE_NAME = 'DETALLE_RECIBOS_FACTURAS_TEMP' ) IS NULL THEN
			EXEC('CREATE COLUMN TABLE EXT.DETALLE_RECIBOS_FACTURAS_TEMP  AS (SELECT * FROM EXT.DETALLE_RECIBOS_FACTURAS)');
			CALL LIB_GLOBAL_CESCE :w_debug (
				i_Tenant,
				'Creado DETALLE_RECIBOS_FACTURAS_TEMP',
				cReportTable,
				io_contador
			);
		END IF;

		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Se insertan datos de Detalles de Recibos en tabla EXT.DETALLE_RECIBOS_FACTURAS ' , cReportTable, io_contador);
		-- Se insertan los recibos de facturas --
		INSERT INTO EXT.DETALLE_RECIBOS_FACTURAS 
		(
			IDRECIBO,
			MATERIAL,
			POSITIONNAME,
			IMPORTE_DET,
			MONEDA_DET,
			ORDERID,
			LINENUMBER,
			SUBLINENUMBER,
			COD_OPERACION,
			NUM_POLIZA,
			NUM_RECIBO,
			COD_AVAL,
			PRODUCTID,
			EVENTTYPEID,
			CASEIDTXN,
			ESTADO_DET,
			MODIF_DATE
		)
		SELECT 
			rfa.IDRECIBO,
			rfa.MATERIAL,
			rfa.POSITIONNAME,
			--CASE WHEN CONCEPTO = 'SIN_PAGO' THEN pay.VALUE 
			CASE WHEN ( CONCEPTO = 'SIN_PAGO')  -- Si es SIN_PAGO, el valor final esta en el PAYMENT 
				 THEN pay.VALUE --v3
				 ELSE CASE WHEN CR.GENERICNUMBER3 is null --v5 -si no en el credito (GN3 para comisiones, value para el resto)
							THEN CR.VALUE
							ELSE CR.GENERICNUMBER3 
							END
				END as IMPORTE_DET, 
			rfa.MONEDA as MONEDA_DET,
			IFNULL(SO.ORDERID, '000000'),
			IFNULL(TX.LINENUMBER,0),
			IFNULL(TX.SUBLINENUMBER,0),
			IFNULL(SO.GENERICATTRIBUTE1, '000000') as COD_OPERACION,
			CASE WHEN length(ltrim(TX.GENERICATTRIBUTE1,'+-.0123456789')) >0 THEN 0 ELSE IFNULL(TX.GENERICATTRIBUTE1,0) END as NUM_POLIZA, -- se verifica si no es numerico
			CASE WHEN length(ltrim(TX.GENERICATTRIBUTE2,'+-.0123456789')) >0 THEN 0 ELSE IFNULL(TX.GENERICATTRIBUTE2,0) END as NUM_RECIBO, -- se verifica si no es numerico		
			CASE WHEN length(ltrim(TX.GENERICATTRIBUTE5,'+-.0123456789')) >0 THEN 0 ELSE IFNULL(TX.GENERICATTRIBUTE5,0.0) END as COD_AVAL, -- se verifica si no es numerico		
			TX.PRODUCTID,
			et.EVENTTYPEID,
			TX.GENERICATTRIBUTE13 as CASEIDTXN,
			'PENDIENTE' as ESTADO_DET,
			CURRENT_TIMESTAMP

		FROM CS_PAYMENT pay
			INNER JOIN EXT.RECIBOS_FACTURAS rfa on pay.PAYMENTSEQ = rfa.PAYMENTSEQ
			INNER JOIN CS_PERIOD per on pay.PERIODSEQ=per.PERIODSEQ and per.REMOVEDATE = TO_DATE('22000101','yyyymmdd')
			INNER JOIN CS_APPLDEPOSITPAYMENTTRACE payapd on pay.PAYMENTSEQ = payapd.PAYMENTSEQ
			INNER JOIN CS_APPLIEDDEPOSIT apd on payapd.APPLIEDDEPOSITSEQ = apd.APPLIEDDEPOSITSEQ
			INNER JOIN CS_DEPOSITAPPLDEPOSITTRACE apddep ON apd.APPLIEDDEPOSITSEQ = apddep.APPLIEDDEPOSITSEQ AND
															apd.UNITTYPEFORVALUE = apddep.UNITTYPEFORCONTRIBUTIONVALUE --V08
			INNER JOIN CS_DEPOSIT dep on apddep.DEPOSITSEQ = dep.DEPOSITSEQ
			INNER JOIN CS_DEPOSITINCENTIVETRACE depinc on dep.DEPOSITSEQ = depinc.DEPOSITSEQ
			INNER JOIN CS_INCENTIVE inc on depinc.INCENTIVESEQ = inc.INCENTIVESEQ
			INNER JOIN CS_INCENTIVEPMTRACE incmed on inc.INCENTIVESEQ = incmed.INCENTIVESEQ
			INNER JOIN CS_MEASUREMENT med on incmed.MEASUREMENTSEQ = med.MEASUREMENTSEQ
			LEFT JOIN CS_PMSELFTRACE medmd1 on med.MEASUREMENTSEQ = medmd1.TARGETMEASUREMENTSEQ
			LEFT JOIN CS_MEASUREMENT md1 on medmd1.SOURCEMEASUREMENTSEQ = md1.MEASUREMENTSEQ
			INNER JOIN CS_PMCREDITTRACE medcr on ifNULL(md1.MEASUREMENTSEQ, med.MEASUREMENTSEQ) = medcr.MEASUREMENTSEQ 
			INNER JOIN CS_CREDIT cr on medcr.CREDITSEQ = cr.CREDITSEQ
			INNER JOIN CS_SALESTRANSACTION TX on CR.SALESTRANSACTIONSEQ = TX.SALESTRANSACTIONSEQ
			INNER JOIN  CS_EVENTTYPE et on TX.EVENTTYPESEQ = et.DATATYPESEQ and et.REMOVEDATE = TO_DATE('22000101','yyyymmdd')
			INNER JOIN CS_SALESORDER SO on CR.SALESORDERSEQ = SO.SALESORDERSEQ and SO.REMOVEDATE = TO_DATE('22000101','yyyymmdd')
		WHERE ( CASE WHEN ( CONCEPTO = 'SIN_PAGO') THEN pay.VALUE ELSE CASE WHEN CR.GENERICNUMBER3 is null THEN CR.VALUE ELSE CR.GENERICNUMBER3 END
				END ) <> 0  -- Solo se pasan los detalles con IMPORTE_DET <> 0 porque da error en la factura de SAP
				AND rfa.ESTADO ='NUEVO';

		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Insertados ' || To_VARCHAR(::ROWCOUNT)  || ' REGISTROS EN EXT.DETALLE_RECIBOS_FACTURAS ' , cReportTable, io_contador);
-- ----------------------------------------------------------------------------------------------------
-- Se insertan detalles de recibos de facturas sin Calculos previos (v9)
-- ----------------------------------------------------------------------------------------------------
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Se insertan Detalles de Recibos sin Calculos previos en tabla EXT.DETALLE_RECIBOS_FACTURAS ' , cReportTable, io_contador);
		INSERT INTO EXT.DETALLE_RECIBOS_FACTURAS 
		(
			IDRECIBO,
			MATERIAL,
			POSITIONNAME,
			IMPORTE_DET,
			MONEDA_DET,
			ORDERID,
			LINENUMBER,
			SUBLINENUMBER,
			COD_OPERACION,
			NUM_POLIZA,
			NUM_RECIBO,
			COD_AVAL,
			PRODUCTID,
			EVENTTYPEID,
			CASEIDTXN,
			ESTADO_DET,
			MODIF_DATE
		)
		SELECT 
			rfa.IDRECIBO,
			rfa.MATERIAL,
			rfa.POSITIONNAME,
			--CASE WHEN CONCEPTO = 'SIN_PAGO' THEN pay.VALUE 
			pay.VALUE as IMPORTE_DET, 
			rfa.MONEDA as MONEDA_DET,
			'000000' as ORDERID,
			0 as LINENUMBER,
			0 as SUBLINENUMBER,
			'000000' as COD_OPERACION,
			0 as NUM_POLIZA, -- se verifica si no es numerico
			0 as NUM_RECIBO, -- se verifica si no es numerico		
			0 as COD_AVAL, -- se verifica si no es numerico		
			null as PRODUCTID,
			'' as EVENTTYPEID,
			null as CASEIDTXN,
			'PENDIENTE' as ESTADO_DET,
			CURRENT_TIMESTAMP

		FROM CS_PAYMENT pay
			INNER JOIN EXT.RECIBOS_FACTURAS rfa on pay.PAYMENTSEQ = rfa.PAYMENTSEQ
			INNER JOIN CS_PERIOD per on pay.PERIODSEQ=per.PERIODSEQ and per.REMOVEDATE = TO_DATE('22000101','yyyymmdd')
			INNER JOIN CS_APPLDEPOSITPAYMENTTRACE payapd on pay.PAYMENTSEQ = payapd.PAYMENTSEQ
			INNER JOIN CS_APPLIEDDEPOSIT apd on payapd.APPLIEDDEPOSITSEQ = apd.APPLIEDDEPOSITSEQ
			INNER JOIN CS_DEPOSITAPPLDEPOSITTRACE apddep ON apd.APPLIEDDEPOSITSEQ = apddep.APPLIEDDEPOSITSEQ AND
															apd.UNITTYPEFORVALUE = apddep.UNITTYPEFORCONTRIBUTIONVALUE --V08
			INNER JOIN CS_DEPOSIT dep on apddep.DEPOSITSEQ = dep.DEPOSITSEQ
		WHERE rfa.ESTADO ='NUEVO' AND
			  rfa.IDRECIBO not in ( SELECT DISTINCT rfa.IDRECIBO
										from EXT.RECIBOS_FACTURAS rfa 
											 INNER JOIN EXT.DETALLE_RECIBOS_FACTURAS drf  on rfa.IDRECIBO = drf.IDRECIBO
										WHERE rfa.ESTADO ='NUEVO' );
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Insertados ' || To_VARCHAR(::ROWCOUNT)  || ' REGISTROS Sin calculos EN EXT.DETALLE_RECIBOS_FACTURAS ' , cReportTable, io_contador);

----------------------------------------------------------------------------------------------------------------------
-- Se insertan detalles desde la tabla temporal que no existan previamente en otra liquidación
-- ----------------------------------------------------------------------------------------------------
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Se insertan Detalles de Recibos que no existan en otra liquidación. EXT.DETALLE_RECIBOS_FACTURAS ' , cReportTable, io_contador);
		INSERT INTO EXT.DETALLE_RECIBOS_FACTURAS 
		(
			IDRECIBO,
			MATERIAL,
			POSITIONNAME,
			IMPORTE_DET,
			MONEDA_DET,
			ORDERID,
			LINENUMBER,
			SUBLINENUMBER,
			COD_OPERACION,
			NUM_POLIZA,
			NUM_RECIBO,
			COD_AVAL,
			PRODUCTID,
			EVENTTYPEID,
			CASEIDTXN,
			ESTADO_DET,
			MODIF_DATE
		)
		SELECT 
			drft.IDRECIBO,
			drft.MATERIAL,
			drft.POSITIONNAME,
			drft.IMPORTE_DET,
			drft.MONEDA_DET,
			drft.ORDERID,
			drft.LINENUMBER,
			drft.SUBLINENUMBER,
			drft.COD_OPERACION,
			drft.NUM_POLIZA,
			drft.NUM_RECIBO,
			drft.COD_AVAL,
			drft.PRODUCTID,
			drft.EVENTTYPEID,
			drft.CASEIDTXN,
			'PENDIENTE' as ESTADO_DET,
			CURRENT_TIMESTAMP as MODIF_DATE
		FROM EXT.DETALLE_RECIBOS_FACTURAS_TEMP drft INNER JOIN EXT.DETALLE_RECIBOS_FACTURAS drf ON drft.ORDERID = drf.ORDERID AND drft.LINENUMBER = drf.LINENUMBER AND drft.SUBLINENUMBER = drf.SUBLINENUMBER
		WHERE drf.ORDERID IS NULL AND drf.LINENUMBER IS NULL AND drf.SUBLINENUMBER IS NULL;

		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Insertados ' || To_VARCHAR(::ROWCOUNT)  || ' REGISTROS que no existen en otra liquidación EN EXT.DETALLE_RECIBOS_FACTURAS ' , cReportTable, io_contador);
------------------------------------------------

	 IF actualizaOrder = 'S' THEN
		SELECT 'LIQUIDACION_' || TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')||'.txt' INTO batchname FROM DUMMY;
		
		INSERT INTO TCMP.CS_STAGESALESORDER
		(
			TENANTID,
			BATCHNAME,
			ORDERID,
			SALESORDERSEQ,
			BUSINESSUNITNAME,
			BUSINESSUNITMAP,
			GENERICATTRIBUTE1,
			GENERICATTRIBUTE2,
			GENERICATTRIBUTE3,
			GENERICATTRIBUTE4,
			GENERICATTRIBUTE5,
			GENERICATTRIBUTE6,
			GENERICATTRIBUTE7,
			GENERICATTRIBUTE8,
			GENERICATTRIBUTE9,
			GENERICATTRIBUTE10,
			GENERICATTRIBUTE11,
			GENERICATTRIBUTE12,
			GENERICATTRIBUTE13,
			GENERICATTRIBUTE14,
			GENERICATTRIBUTE15,
			GENERICATTRIBUTE16,
			GENERICNUMBER1,
			UNITTYPEFORGENERICNUMBER1,
			GENERICNUMBER2,
			UNITTYPEFORGENERICNUMBER2,
			GENERICNUMBER3,
			UNITTYPEFORGENERICNUMBER3,
			GENERICNUMBER4,
			UNITTYPEFORGENERICNUMBER4,
			GENERICNUMBER5,
			UNITTYPEFORGENERICNUMBER5,
			GENERICNUMBER6,
			UNITTYPEFORGENERICNUMBER6,
			GENERICDATE1,
			GENERICDATE2,
			GENERICDATE3,
			GENERICDATE4,
			GENERICDATE5,
			GENERICDATE6,
			GENERICBOOLEAN1,
			GENERICBOOLEAN2,
			GENERICBOOLEAN3,
			GENERICBOOLEAN4,
			GENERICBOOLEAN5,
			GENERICBOOLEAN6
		)
		
		SELECT 
			TENANTID,
			batchname AS BATCHNAME,
			ORDERID,
			SALESORDERSEQ,
			(SELECT NAME FROM TCMP.CS_BUSINESSUNIT WHERE MASK = BUSINESSUNITMAP) AS BUSINESSUNITNAME,
			BUSINESSUNITMAP,
			GENERICATTRIBUTE1,
			GENERICATTRIBUTE2,
			GENERICATTRIBUTE3,
			GENERICATTRIBUTE4,
			GENERICATTRIBUTE5,
			GENERICATTRIBUTE6,
			GENERICATTRIBUTE7,
			GENERICATTRIBUTE8,
			GENERICATTRIBUTE9,
			GENERICATTRIBUTE10,
			GENERICATTRIBUTE11,
			GENERICATTRIBUTE12,
			GENERICATTRIBUTE13,
			GENERICATTRIBUTE14,
			GENERICATTRIBUTE15,
			GENERICATTRIBUTE16,
			GENERICNUMBER1,
			UNITTYPEFORGENERICNUMBER1,
			GENERICNUMBER2,
			UNITTYPEFORGENERICNUMBER2,
			GENERICNUMBER3,
			UNITTYPEFORGENERICNUMBER3,
			GENERICNUMBER4,
			UNITTYPEFORGENERICNUMBER4,
			GENERICNUMBER5,
			UNITTYPEFORGENERICNUMBER5,
			GENERICNUMBER6,
			UNITTYPEFORGENERICNUMBER6,
			GENERICDATE1,
			GENERICDATE2,
			GENERICDATE3,
			GENERICDATE4,
			GENERICDATE5,
			CURRENT_DATE AS GENERICDATE6,
			1 AS GENERICBOOLEAN1,
			GENERICBOOLEAN2,
			GENERICBOOLEAN3,
			GENERICBOOLEAN4,
			GENERICBOOLEAN5,
			GENERICBOOLEAN6
		FROM CS_SALESORDER 
		WHERE REMOVEDATE = TO_DATE('22000101', 'yyyymmdd')
		AND ORDERID IN (
			SELECT DISTINCT ORDERID FROM EXT.DETALLE_RECIBOS_FACTURAS DET INNER JOIN EXT.RECIBOS_FACTURAS REC ON DET.IDRECIBO = REC.IDRECIBO WHERE ESTADO = 'NUEVO'
		);
/*		
		INSERT INTO "EXT"."VT_PIPELINERUNS" ("Command","StageType","TraceLevel","SkipAnalyzeSchema","SqlLogging","DebugContext","UserId","RunMode","BatchName","Module","ProcessingUnit","CalendarName","StartDateScheduled")
	
		SELECT
		'Import' as "Command",
		'ValidateAndTransfer' as "StageType",
		'status' as "TraceLevel",
		null as "SkipAnalyzeSchema",
		null as "SqlLogging",
		null as "DebugContext",
		'data_integration_service_account' as "UserId",
		'all' as "RunMode",
		batchname as "BatchName",
		'TransactionalData' as "Module",
		NULL as "ProcessingUnit",
		'Main Monthly Calendar' as "CalendarName",
		add_seconds(current_utctimestamp,2) as "StartDateScheduled" FROM Dummy;
*/
	ELSE
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'SALES ORDER NO ACTUALIZADOS POR PARAMETRO actualizaOrder' , cReportTable, io_contador);
	END IF; -- FIN de actualizaOrder

-- ----------------------------------------------------------------------------------------------------
-- Se Actualizan los datos de los recibos de FACTURA de NUEVO --> PENDIENTE. LOS DE INFO NO PASAN A FACTURAS
-- ----------------------------------------------------------------------------------------------------
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Actualización de estado de NUEVO a INFO Para Concepto SIN_PAGO en EXT.RECIBOS_FACTURAS' , cReportTable, io_contador);

		UPDATE EXT.RECIBOS_FACTURAS 
		SET ESTADO = 'INFO',
			MODIF_DATE = CURRENT_TIMESTAMP
		WHERE ESTADO='NUEVO' and CONCEPTO = 'SIN_PAGO';
		
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || To_VARCHAR(::ROWCOUNT)  || ' REGISTROS EN EXT.RECIBOS_FACTURAS ' , cReportTable, io_contador);

		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Actualización de estado de NUEVO a PENDIENTE en EXT.RECIBOS_FACTURAS' , cReportTable, io_contador);

		UPDATE EXT.RECIBOS_FACTURAS 
		SET ESTADO = 'PENDIENTE',
			MODIF_DATE = CURRENT_TIMESTAMP
		WHERE ESTADO='NUEVO';

		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || To_VARCHAR(::ROWCOUNT)  || ' REGISTROS EN EXT.RECIBOS_FACTURAS ' , cReportTable, io_contador);


-- ----------------------------------------------------------------------------------------------------
	-- Se crean las facturas y sus detalles, y se actualizan recibos --
-- ----------------------------------------------------------------------------------------------------
--	OPEN recibos;

	FOR r AS recibos DO 
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Generando Factura para recibo' || To_VARCHAR(r.IDRECIBO)  || ' REGISTROS EN EXT.RECIBOS_FACTURAS ' , cReportTable, io_contador);
		
		CALL EXT.LIB_GLOBAL_CESCE:ObtenerSigRefFactura(r.POSITIONNAME,r.SOCIEDAD,YEAR(CURRENT_DATE), :referenciaFactura);

		INSERT INTO EXT.FACTURAS (
			REFFACTURA,
			FECHA_ALTA,
			CONCEPTO,
			IMPORTETOTAL, 
			MONEDA,
			CODPROVEEDOR,
			POSITIONNAME,
			NOMBRE_MEDIADOR,
			NUM_IDENTIFICACION,
			TIPO_ID_FISCAL,
			TAXID,
			BUSINESSUNITNAME,
			CLASE_DOCUMENTO,
			CLAVE_CONTABLE,
			SOCIEDAD,
			-- IND_CME,
			CONDPAGO,
			VIAPAGO,
			CREATEDATE,
			MODIF_DATE
		)
		VALUES(
			referenciaFactura,
			--CURRENT_DATE,
			r.FECHA_ALTA,
			r.CONCEPTO,
			r.IMPORTE,
			r.MONEDA,
			r.CODPROVEEDOR,
			r.POSITIONNAME,
			r.NOMBRE_MEDIADOR,
			r.NUM_IDENTIFICACION,
			r.TIPO_ID_FISCAL,
			r.TAXID,
			r.BUSINESSUNITNAME,
			CASE WHEN r.IMPORTE >= 0 THEN 'KR' ELSE 'KG' END,
			CASE WHEN r.IMPORTE >= 0 THEN '31' ELSE '21' END, 
			r.SOCIEDAD,
			r.CONDPAGO,
			r.VIAPAGO,
			CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP
		);

		SELECT IDFACTURA INTO vIdFactura FROM EXT.FACTURAS 
		WHERE REFFACTURA = referenciaFactura 
				AND POSITIONNAME = r.POSITIONNAME
				AND SOCIEDAD = r.SOCIEDAD;

-- Se insertan los detalles de facturas en un procedimiento externo
	CALL "EXT"."INSERT_DET_FAC"(vIdFactura, r.IDRECIBO, r.CENTROCOSTE,r.CODPROVEEDOR );


		UPDATE EXT.RECIBOS_FACTURAS SET ESTADO = 'ENVIADO'
		WHERE IDRECIBO = r.IDRECIBO;

	END FOR;
--	CLOSE recibos;
------------------------------------------------------------------------
----  Borramos tabla temporal EXT.DETALLE_RECIBOS_FACTURAS_TEMP --------
------------------------------------------------------------------------

	DROP TABLE EXT.DETALLE_RECIBOS_FACTURAS_TEMP;
	CALL EXT.LIB_GLOBAL_CESCE :w_debug (
		i_Tenant,
		'Borrada tabla temporal EXT.DETALLE_RECIBOS_FACTURAS_TEMP',
		cReportTable,
		io_contador
	);

END
