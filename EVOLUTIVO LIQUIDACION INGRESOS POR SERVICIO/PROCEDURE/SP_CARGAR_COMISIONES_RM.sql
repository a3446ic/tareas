CREATE PROCEDURE "EXT"."SP_CARGAR_COMISIONES_RM" LANGUAGE SQLSCRIPT SQL SECURITY DEFINER DEFAULT SCHEMA "EXT" AS BEGIN 


DECLARE i_Tenant VARCHAR(4);
DECLARE vProcedure VARCHAR(127);
DECLARE batchname VARCHAR(255);
DECLARE io_contador Number := 0;

DECLARE txn_insertadas VARCHAR(10);
DECLARE cVersion CONSTANT VARCHAR(2) := '08';

-- VERSIONES
--v02 - Se modifica el SUBLINE, que esta asignado a "1", del insert de la tabla de stage de asignacion (TIPO_MVTO as SUBLINENUMBER)
--v03 - Modificado el sublinenumber en las inserciones (ahora se concatena TIPO_MVTO y NUM_MVTO)
------- Comento el log del numero de txn transferidas porque la SELECT cuenta todas las lineas en STAGE en lugar de las asociadas al batchname
--v04 - Se modifica para actualziar el IDPAIS de los Mediadores de portugal, porque todas las lineas vienen con el pais 116 (españa). Se obtiene el pais de la ficha del mediador
------- Se incluyen los movimiento 14 que son las Cuotas de suscripcion Quantum
--v05 - Modificado el sublinenumber :concatena TIPO_MVTO, NUM_MVTO y TIPOCOMISION, ya que puede haber TIPOCOMISION 014 y 015 de la misma poliza
--v06 - Modificado el ESTADO segun tipo Mvto 2 (cobrado) o 3 (Anulado Cobrado) en GA3
--v07 - Se añade RESIGNAL en el Exception handler y un secuencial en SUBLINENUMBER para movimientos iguales de una misma poliza con facturas distintas
--v08 - Se añade el secuencial al SUBLINENUMBER del TRANSACTION ASSIGNEMENT
----------------------------- HANDLER EXCEPTION -------------------------
DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN 
CALL EXT.LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'SQL_ERROR_MESSAGE: ' || IFNULL( ::SQL_ERROR_MESSAGE, '') || '. SQL_ERROR_CODE: ' ||  ::SQL_ERROR_CODE,
    vProcedure,
    io_contador
);
RESIGNAL;

END;

SELECT
    TENANTID INTO i_Tenant
FROM
    CS_TENANT;

SELECT
    BATCHNAME INTO batchname
FROM
    COMISIONES_RM_HIST 
WHERE ESTADOREG = 'PENDIENTE'
LIMIT 1;

vProcedure := 'SP_CARGAR_COMISIONES' || ' '  || cVersion;

CALL EXT.LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'INICIO PROCEDIMIENTO with SESSION_USER ' || SESSION_USER,
    vProcedure,
    io_contador
);

------------------------------
CALL EXT.LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'Se actualiza el IDPAIS para los mediadores de PORTUGAL',
    vProcedure,
    io_contador
);

	UPDATE EXT.COMISIONES_RM_HIST rmh 
		set rmh.IDPAIS = cast(med.COD_PAIS as integer)
	FROM  EXT.COMISIONES_RM_HIST rmh
	INNER JOIN EXT.MODIFICAR_MEDIADOR med 
			   ON rmh.IDMEDIADOR = med.COD_MEDIADOR and rmh.IDSUBCLAVE = med.SUBCLAVE
	WHERE 
		rmh.ESTADOREG = 'PENDIENTE' and med.COD_PAIS in (116, 151)  and cast(med.COD_PAIS as integer) <> rmh.IDPAIS ;

CALL EXT.LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'Actualizados ' || To_VARCHAR(::ROWCOUNT)  || ' registros con el IDPAIS de PORTUGAL',
    vProcedure,
    io_contador
);		
---------------------------
CALL EXT.LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    ' INSERT TABLAS STAGE',
    vProcedure,
    io_contador
);

INSERT INTO
    TCMP.CS_STAGESALESTRANSACTION (
        TENANTID,
        ORDERID,
        LINENUMBER,
        SUBLINENUMBER,
        EVENTTYPEID,
        PRODUCTID,
        PRODUCTNAME,
        VALUE,
        UNITTYPEFORVALUE,
        COMPENSATIONDATE,
        BATCHNAME,
        BUSINESSUNITNAME,
        GENERICATTRIBUTE1,
        GENERICATTRIBUTE3,
        GENERICATTRIBUTE6,
        GENERICATTRIBUTE7,
        GENERICATTRIBUTE8,
        GENERICATTRIBUTE9,
        GENERICATTRIBUTE11,
        GENERICATTRIBUTE12,
        GENERICATTRIBUTE16,
        GENERICNUMBER1,
        UNITTYPEFORGENERICNUMBER1,
        GENERICNUMBER3,
        UNITTYPEFORGENERICNUMBER3,
        GENERICDATE4,
        GENERICDATE5
    )
SELECT
    i_Tenant as TENANTID,
    NUM_POLIZA as ORDERID,
    MESCIERRE as LINENUMBER,
--    CONCAT(CONCAT(TIPO_MVTO, NUM_MVTO),TIPOCOMISION) as SUBLINENUMBER, --CONCAT(TIPO_MVTO, NUM_MVTO) as SUBLINENUMBER, --TIPO_MVTO as SUBLINENUMBER,
    CONCAT(CONCAT(TIPO_MVTO, NUM_MVTO),TIPOCOMISION) || LPAD(ROW_NUMBER() OVER (PARTITION BY NUM_POLIZA),3,'0')  as SUBLINENUMBER,
    'Servicios' as EVENTTYPE,
    (
        SELECT
            EXT.LIB_GLOBAL_CESCE:getProductId(SUBSTRING(NUM_POLIZA, 1, 3), null, IDPAIS, SUBSTRING(NUM_POLIZA, 4, 8)).productId
        FROM
            DUMMY
    ) as PRODUCTID,
    (
        SELECT
            EXT.LIB_GLOBAL_CESCE:getProductId(SUBSTRING(NUM_POLIZA, 1, 3), null, IDPAIS, SUBSTRING(NUM_POLIZA, 4, 8)).productDescription
        FROM
            DUMMY
    ) as PRODUCTNAME,
    IMPORTE_COBRADA as VALUE,
    'EUR' as UNITTYPEFORVALUE,
    FEC_MOVIMIENTO as COMPENSATIONDATE,
    batchname as BATCHNAME,
    (
        SELECT
            EXT.LIB_GLOBAL_CESCE:getProductId(SUBSTRING(NUM_POLIZA, 1, 3), null, IDPAIS, SUBSTRING(NUM_POLIZA, 4, 8)).buname
        FROM
            DUMMY
    ) as BUSINESSUNITNAME,
    SUBSTR(NUM_POLIZA, 4, 8) as GENERICATTRIBUTE1,
    --'C' as GENERICATTRIBUTE3,
	CASE WHEN TIPO_MVTO = 3 THEN 'AC' ELSE 'C' END  as GENERICATTRIBUTE3,  -- PARA MVTO 3 es AC ANULADO COBRO,. para elresto es C - CoBRADO
    COD_GARANTIA as GENERICATTRIBUTE6,
    CUE_RIESGO as GENERICATTRIBUTE7,
    NUM_MVTO as GENERICATTRIBUTE8,
    TIPO_MVTO as GENERICATTRIBUTE9,
    COMPANIA as GENERICATTRIBUTE11,
    IDPAIS as GENERICATTRIBUTE12,
    IDDIVISA AS GENERICATTRIBUTE16,
    IMPORTE_COM_COBRADA as GENERICNUMBER1,
    'EUR' AS UNITTYPEFORGENERICNUMBER1,
    0 as GENERICNUMBER3,
    'integer' AS UNITTYPEFORGENERICNUMBER3,
    FEC_MOVIMIENTO AS GENERICDATE4,
    FECHA_EFECTO_ANUALIDAD AS GENERICDATE5 
FROM
    EXT.COMISIONES_RM_HIST
WHERE
	(TIPOCOMISION = '015' or TIPOCOMISION = '014') AND (TIPO_MVTO = 2 OR TIPO_MVTO = 3)
	AND ESTADOREG = 'PENDIENTE';

CALL EXT.LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'Insertados ' || To_VARCHAR(::ROWCOUNT)  || ' registros en CS_STAGESALESTRANSACTION',
    vProcedure,
    io_contador
);	

INSERT INTO 
    CS_STAGETRANSACTIONASSIGN (
        TENANTID,
    	BATCHNAME,
        ORDERID,
        LINENUMBER,
        SUBLINENUMBER,
        EVENTTYPEID,
        POSITIONNAME,
        GENERICATTRIBUTE1,
        GENERICATTRIBUTE2,
        GENERICATTRIBUTE3,
        GENERICNUMBER1,
        UNITTYPEFORGENERICNUMBER1,
        GENERICNUMBER2,
        UNITTYPEFORGENERICNUMBER2
    )
    SELECT
        i_Tenant as TENANTID,
    	BATCHNAME AS BATCHNAME,
        NUM_POLIZA as ORDERID,
        MESCIERRE as LINENUMBER,
        --CONCAT(CONCAT(TIPO_MVTO, NUM_MVTO),TIPOCOMISION) as SUBLINENUMBER, --CONCAT(TIPO_MVTO, NUM_MVTO) as SUBLINENUMBER, --TIPO_MVTO as SUBLINENUMBER,
        CONCAT(CONCAT(TIPO_MVTO, NUM_MVTO),TIPOCOMISION) || LPAD(ROW_NUMBER() OVER (PARTITION BY NUM_POLIZA),3,'0')  as SUBLINENUMBER,
        'Servicios' as EVENTTYPE,
        CONCAT(CONCAT(LPAD(IDMEDIADOR, 4, '0'), '-'), LPAD(IDSUBCLAVE, 4, '0')),
        CANAL_DISTRIB AS GENERICATTRIBUTE1,
        IDMEDIADOR AS GENERICATTRIBUTE2,
        IDSUBCLAVE AS GENERICATTRIBUTE3,
        100 AS GENERICNUMBER1,
        'percent' AS UNITTYPEFORGENERICNUMBER1,
        0,
        'integer' AS UNITTYPEFORGENERICNUMBER2
	FROM
    	EXT.COMISIONES_RM_HIST
	WHERE
	(TIPOCOMISION = '015' or TIPOCOMISION = '014') AND (TIPO_MVTO = 2 OR TIPO_MVTO = 3)
	AND ESTADOREG = 'PENDIENTE';

 CALL EXT.LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'Insertados ' || To_VARCHAR(::ROWCOUNT)  || ' registros en CS_STAGETRANSACTIONASSIGN',
    vProcedure,
    io_contador
);	
    	
    UPDATE COMISIONES_RM_HIST SET ESTADOREG = 'ENVIADA' 
    WHERE (TIPOCOMISION = '015' or TIPOCOMISION = '014') AND (TIPO_MVTO = 2 OR TIPO_MVTO = 3) AND ESTADOREG = 'PENDIENTE';    	
    	
    UPDATE COMISIONES_RM_HIST SET ESTADOREG = 'NO_APLICA' WHERE --TIPOCOMISION != '015' OR (TIPO_MVTO != 2 AND TIPO_MVTO != 3) AND 
    ESTADOREG = 'PENDIENTE';
    
 --   SELECT COUNT(*) INTO txn_insertadas FROM TCMP.CS_STAGESALESTRANSACTION WHERE BATCHNAME = batchname;
    
    /*CALL EXT.LIB_GLOBAL_CESCE:w_debug (
    	i_Tenant,
    	vProcedure || ' SE HAN INSERTADO ' || txn_insertadas || ' EN STAGE with SESSION_USER '|| SESSION_USER,
    	vProcedure
    	, io_contador
    );*/
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (
		i_Tenant,
		' INSERT TABLA VIRTUAL with SESSION_USER '|| SESSION_USER,
		vProcedure,
		io_contador
	);
    	
    INSERT INTO "EXT"."VT_PipelineRuns" ("Command","StageType","TraceLevel","SkipAnalyzeSchema","SqlLogging","DebugContext","UserId","RunMode","BatchName","Module","ProcessingUnit","CalendarName","StartDateScheduled")
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
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (
		i_Tenant,
		' FIN PROCEDIMIENTO with SESSION_USER ' || SESSION_USER,
		vProcedure,
		io_contador
	);
END