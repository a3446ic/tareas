CREATE OR REPLACE PROCEDURE "EXT"."SP_INF_LIQUIDACION_RM"(IN IN_FILENAME VARCHAR(250)) LANGUAGE SQLSCRIPT AS
BEGIN

    --versión inicial
    DECLARE i_Tenant VARCHAR(127);
    DECLARE io_contador Number := 0;
    DECLARE vRegistrosCabecera Number := 0;
    DECLARE vRegistrosDetalle Number := 0;

	--Constantes
    DECLARE cReport CONSTANT VARCHAR(50) := 'SP_INF_LIQUIDACION_RM';
	DECLARE cTableCabecera CONSTANT VARCHAR(50) := 'INF_LIQUIDACION_RM';
    DECLARE cTableDetalle CONSTANT VARCHAR(50) := 'INF_DETALLE_LIQUIDACION_RM';
	DECLARE cEOT  CONSTANT DATE := TO_DATE('22000101','yyyymmdd');
	DECLARE cCalendar  CONSTANT VARCHAR(50) :='Main Monthly Calendar';
	DECLARE cConcepto  CONSTANT VARCHAR(50) :='Comisiones por servicio';
	DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
	DECLARE cEsquema  CONSTANT VARCHAR(3) :='EXT';
	DECLARE cDescProducto CONSTANT VARCHAR(50) := 'Quantum Servicios';
	
	--LOTE
	DECLARE BATCH_SIZE INT := 1000;
    DECLARE OFFSET INT := 0;
    DECLARE TOTAL_COUNT INT;
    DECLARE BATCH_ID INT := 1;
	

    ----------------------------- HANDLER EXCEPTION -------------------------
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'SQL ERROR_MESSAGE: ' ||
			IFNULL(::SQL_ERROR_MESSAGE,'') || '. SQL_ERROR_CODE: ' || ::SQL_ERROR_CODE, cReport, io_contador);
	END;
	---------------------------------------------------------------------------

    SELECT EXT.LIB_GLOBAL_CESCE:getTenantID() INTO i_Tenant FROM DUMMY;

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INICIO PROCEDIMIENTO v' || cVersion || ' with SESSION_USER '|| SESSION_USER, CReport, io_contador);

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Se insertan datos de PAGOS en tabla EXT.'|| cTableDetalle , cReport, io_contador);
-- DROP TABLE EXT.INF_DETALLE_LIQUIDACION_RM
    --Si no existe las tablas las creamos    
    IF (SELECT TABLE_NAME FROM SYS.TABLES where SCHEMA_NAME= cEsquema and TABLE_NAME = cTableDetalle) IS NULL THEN
	    CREATE COLUMN TABLE EXT.INF_DETALLE_LIQUIDACION_RM (
            CODIGO VARCHAR(20),
            MEDIADOR VARCHAR(250),
            COMPANIA VARCHAR(1),
            SUCURSAL VARCHAR(50),
            IDPAIS SMALLINT CS_INT,
            PAIS VARCHAR(50),
            SOCIEDAD VARCHAR(50),           
            IDRECIBO BIGINT,
            CONCEPTO VARCHAR(50),
            DESCRIPCION_PRODUCTO VARCHAR(250),
            MODALIDAD VARCHAR(3),
            NUM_POLIZA DECIMAL(11,0) CS_FIXED,
            TOMADOR VARCHAR(50),
            MESCIERRE VARCHAR(6),
            FECHA_EFECTO_ANUALIDAD DATE CS_DAYDATE,
            FECHA_RECIBO_LIQUIDACION DATE CS_DAYDATE,
            FECHA_COBRO DATE CS_DAYDATE,
            NUM_FACTURA VARCHAR(8),
            FECHA_FACTURA DATE CS_DAYDATE,
            IDDIVISA SMALLINT CS_INT,
            MONEDA VARCHAR(5),
            IDTIPOCOMISION VARCHAR(3),
            TIPOCOMISION VARCHAR(50),
            IDTIPO_MVTO DECIMAL(4,0) CS_FIXED,
            TIPO_MVTO VARCHAR(50),
            IMPORTE_COBRADA DECIMAL(18, 3) CS_FIXED,
            IMPORTE_COM_COBRADA DECIMAL(18, 3) CS_FIXED,
            PERIODSEQ BIGINT CS_FIXED,
            POSITIONSEQ BIGINT CS_FIXED,
            CREATEDATE LONGDATE CS_LONGDATE,
			MODIF_DATE LONGDATE CS_LONGDATE,
			MODIF_USER NVARCHAR(50),
            BATCHNAME VARCHAR(250)
        )UNLOAD PRIORITY 5 AUTO MERGE;

        CALL EXT.LIB_GLOBAL_CESCE :w_debug (
            i_Tenant,
            'CREADA TABLA ' || cEsquema || '.' || cTableDetalle,
            cReport,
            io_contador
        );
    END IF;
    -------------------------------------------------------------------------------------------------------------------
    
    -- Borramos registros almacenados para el fichero de entrada
   IF EXISTS(SELECT 1 FROM EXT.INF_DETALLE_LIQUIDACION_RM WHERE BATCHNAME LIKE CASE WHEN IN_FILENAME = '' OR IN_FILENAME IS NULL THEN '%%' ELSE IN_FILENAME END) THEN
    	DELETE FROM EXT.INF_DETALLE_LIQUIDACION_RM WHERE BATCHNAME LIKE CASE WHEN IN_FILENAME = '' OR IN_FILENAME IS NULL THEN '%%' ELSE IN_FILENAME END;
    	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'BORRADOS ' || To_VARCHAR(::ROWCOUNT)  || ' REGISTROS EN ' || cEsquema || '.' || cTableDetalle , cReport, io_contador);
    END IF;
    
    -- Obtener el número total de registros a procesar
    SELECT COUNT(*) INTO TOTAL_COUNT 
    FROM EXT.COMISIONES_RM_HIST CRH
    WHERE 1=1
        AND CRH.BATCHNAME LIKE CASE WHEN IN_FILENAME = '' OR IN_FILENAME IS NULL THEN '%%' ELSE IN_FILENAME END
        AND CRH.ESTADOREG = 'ENVIADA';


  -- DROP TABLE EXT.INF_DETALLE_LIQUIDACION_RM
	OFFSET := 0;
	WHILE OFFSET < TOTAL_COUNT DO
    BEGIN
        INSERT INTO EXT.INF_DETALLE_LIQUIDACION_RM(
	    	CODIGO,
	    	MEDIADOR,
	    	IDRECIBO,
	    	IDPAIS,
	    	PAIS,
	    	SUCURSAL,
	    	COMPANIA,
	    	SOCIEDAD,
	    	CONCEPTO,
	    	DESCRIPCION_PRODUCTO,
	    	FECHA_RECIBO_LIQUIDACION,
	    	FECHA_EFECTO_ANUALIDAD,
	    	NUM_FACTURA,
	    	FECHA_FACTURA,
	    	FECHA_COBRO,
	    	MODALIDAD,
	    	NUM_POLIZA,
	    	TOMADOR,
	    	MESCIERRE,
	    	IDDIVISA,
	    	MONEDA,
	    	IDTIPOCOMISION,
	    	TIPOCOMISION,
	    	IDTIPO_MVTO,
	    	TIPO_MVTO,
	    	IMPORTE_COBRADA,
	    	IMPORTE_COM_COBRADA,
	    	PERIODSEQ,
	    	POSITIONSEQ,
	    	CREATEDATE,
	    --	MODIF_DATE,
	    --	MODIF_USER,
	    	BATCHNAME
	    )
	    SELECT 
	    	CRH.IDMEDIADOR || '-' || CRH.IDSUBCLAVE CODIGO
	    	, (CASE WHEN MM.NOMBRE IS NULL THEN '' ELSE MM.NOMBRE || ' ' END) || MM."APELLIDO/RAZON_SOCIAL" MEDIADOR
	    	, DENSE_RANK() OVER (ORDER BY NUMFACTURA) AS IDRECIBO 
	    	, CRH.IDPAIS
	    	, EXT.LIB_GLOBAL_CESCE:getPais(CRH.IDPAIS).NAME PAIS
	    	, EXT.LIB_GLOBAL_CESCE:getPais(CRH.IDPAIS).NAME SUCURSAL
	    	, CRH.COMPANIA
	    	, SC.SOCIEDAD
	    	, cConcepto CONCEPTO
	    	, cDescProducto DESCRIPCION_PRODUCTO
	    	, ADD_DAYS(ADD_MONTHS(TO_DATE(SUBSTRING(MESCIERRE,1,4) || '-' || SUBSTRING(MESCIERRE,5,2) || '-' || '01'),1),-1) FECHA_RECIBO_LIQUIDACION
	    	, CRH.FECHA_EFECTO_ANUALIDAD
	    	, CRH.NUMFACTURA NUM_FACTURA
	    	, ADD_DAYS(ADD_MONTHS(TO_DATE(SUBSTRING(MESCIERRE,1,4) || '-' || SUBSTRING(MESCIERRE,5,2) || '-' || '01'),1),-1) FECHA_FACTURA
	    	, CRH.FEC_MOVIMIENTO
	    	, SUBSTRING(NUM_POLIZA,0,3)
	    	, SUBSTRING(NUM_POLIZA,4,LENGTH(NUM_POLIZA)) NUM_POLIZA
	    	, NULL TOMADOR
	    	, CRH.MESCIERRE
	    	, CRH.IDDIVISA
	    	, (SELECT EXT.LIB_GLOBAL_CESCE:getCurrency(CRH.IDDIVISA,'').currencyISO FROM DUMMY) MONEDA
	    	, CRH.TIPOCOMISION IDTIPOCOMISION
	    	, CASE 
	            WHEN CRH.TIPOCOMISION = 1 THEN 'EMISIÓN'
	            WHEN CRH.TIPOCOMISION = 2 THEN 'RENOVACIÓN'
	            WHEN CRH.TIPOCOMISION = 3 THEN 'PRESENTACIÓN'
	            ELSE 'Desconocido'  -- Valor por defecto si IDTIPO_MVTO no coincide con ninguna de las opciones anteriores
    		END AS TIPOCOMISION
	    	, CRH.TIPO_MVTO IDTIPO_MVTO
	    	, CASE 
	            WHEN TIPO_MVTO = 1 THEN 'Emisión automática'
	            WHEN TIPO_MVTO = 2 THEN 'Cobro automático'
	            WHEN TIPO_MVTO = 3 THEN 'Anulación cobro automático'
	            WHEN TIPO_MVTO = 4 THEN 'Anulación emisión automática'
	            WHEN TIPO_MVTO = 5 THEN 'Emisión manual'
	            WHEN TIPO_MVTO = 6 THEN 'Cobro manual'
	            WHEN TIPO_MVTO = 7 THEN 'Anulación cobro manual'
	            WHEN TIPO_MVTO = 8 THEN 'Anulación emisión manual'
	            ELSE 'Desconocido'  -- Valor por defecto si IDTIPO_MVTO no coincide con ninguna de las opciones anteriores
    		END AS TIPO_MVTO
	    	, CRH.IMPORTE_COBRADA IMPORTE_COBRADA
	    	, CRH.IMPORTE_COM_COBRADA IMPORTE_COM_COBRADA
	    	, (SELECT P.PERIODSEQ 
                FROM CS_PERIOD P 
                INNER JOIN CS_CALENDAR C 
                    ON P.CALENDARSEQ = C.CALENDARSEQ AND C.NAME = cCalendar AND C.REMOVEDATE = cEOT
				WHERE 1=1
                    AND STARTDATE >= TO_DATE(MESCIERRE)  
                    AND ENDDATE <= ADD_MONTHS(TO_DATE(MESCIERRE),1)
				    AND P.REMOVEDATE = cEOT) PERIODSEQ
			, MM.POSITIONSEQ
			, CURRENT_TIMESTAMP
			, CRH.BATCHNAME
	    FROM EXT.COMISIONES_RM_HIST CRH	   
	    LEFT JOIN EXT.MODIFICAR_MEDIADOR MM 
            ON MM.COD_MEDIADOR = CRH.IDMEDIADOR AND MM.SUBCLAVE = CRH.IDSUBCLAVE

		LEFT JOIN EXT.SOCIEDADES_CESCE SC
            ON CRH.IDPAIS = SC.IDPAIS
	    
        WHERE 1=1
	        AND CRH.BATCHNAME LIKE CASE WHEN IN_FILENAME = '' OR IN_FILENAME IS NULL THEN '%%' ELSE IN_FILENAME END
	        AND CRH.ESTADOREG = 'ENVIADA'
	    LIMIT :BATCH_SIZE OFFSET :OFFSET;
	    	
	    vRegistrosDetalle := vRegistrosDetalle + ::ROWCOUNT;
	    -- Incrementar el offset para el siguiente lote
        OFFSET := OFFSET + BATCH_SIZE;
            
        END;
    END WHILE;
	   

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || To_VARCHAR(vRegistrosDetalle)  || ' REGISTROS EN EXT.' || cTableDetalle , cReport, io_contador);
    
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);
END


