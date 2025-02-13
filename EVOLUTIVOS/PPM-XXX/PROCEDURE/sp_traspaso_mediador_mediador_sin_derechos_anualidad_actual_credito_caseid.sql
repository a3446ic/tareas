CREATE OR REPLACE PROCEDURE EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito_caseid (IN caseId BIGINT)
LANGUAGE SQLSCRIPT 
AS
/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 03/02/2025 
	|---------------------------------------------------------------------------------------------- 
	| Procedure Purpose: TRASPASO DE CARTERA DE UN MEDIADOR A OTRO MEDIADOR SIN DERECHOS Y OBLIGACIONES EN LA ANUALIDAD ACTUAL
	| 
	| Version: 1	
	|
	| 
	|
	----------------------------------------------------------------------------------------------- 
*/

BEGIN
    -- DECLARACION DE VARIABLES
    DECLARE i_Tenant VARCHAR(4);
	DECLARE vProcedure VARCHAR(127);
	DECLARE io_contador Number := 0;
    DECLARE v_codMediadorCedente NVARCHAR(100);
    DECLARE v_subClaveMediadorCedente NVARCHAR(100);
    DECLARE v_fechaTraspaso DATE;
    DECLARE v_caseId BIGINT;
    DECLARE v_modifSource NVARCHAR(250);
    DECLARE v_tipoTraspaso NVARCHAR(10);
    DECLARE v_numPolizaCedente NVARCHAR(50);
    DECLARE v_tipoTraspasoCaucion NVARCHAR(100);
	DECLARE cTipoMovimiento NVARCHAR(50);
    -- CONSTANTES
    DECLARE cReport CONSTANT VARCHAR(250) := 'sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    DECLARE cRamo CONSTANT VARCHAR(10) := 'CREDITO';
    DECLARE cDerechosObligaciones NVARCHAR(50) := 'SIN DERECHOS Y OBLIGACIONES ANUALIDAD ACTUAL';
    
    -- DECLARACION DE CURSOR    
    DECLARE CURSOR CURSOR_TRASPASOS FOR
	SELECT DISTINCT NUM_POLIZA,COD_MEDIADOR_RECEPTOR,SUBCLAVE_RECEPTOR,INTERMEDIACION_RECEPTOR,FECHA_EFECTO_SOLICITUD,COD_MEDIADOR_CEDENTE,SUBCLAVE_CEDENTE
	FROM EXT.SOLICITUD_TRASPASO 
	WHERE CASEID = :caseId;

 ------------------------------- HANDLER EXCEPTION -------------------------
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'SQL ERROR_MESSAGE: ' ||
			IFNULL(::SQL_ERROR_MESSAGE,'') || '. SQL_ERROR_CODE: ' || ::SQL_ERROR_CODE, cReport, io_contador);
	END;
    ---------------------------------------------------------------------------
     --Obtenemos tenant
    SELECT EXT.LIB_GLOBAL_CESCE:getTenantID() INTO i_Tenant FROM DUMMY;

    --Inicio
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INICIO PROCEDIMIENTO v' || cVersion || ' with SESSION_USER '|| SESSION_USER, CReport, io_contador);


    --OBTENER VALORES
	SELECT DISTINCT 
		CASE WHEN TIPO_TRASPASO = 'N' THEN 'TOTAL' ELSE 'PARCIAL' END
		, COD_MEDIADOR_CEDENTE
		, SUBCLAVE_CEDENTE
		, FECHA_EFECTO_SOLICITUD
		INTO v_tipoTraspaso,v_codMediadorCedente,v_subClaveMediadorCedente,v_fechaTraspaso 
	FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId;

    
    -- COMPROBAR SI ES TRASPASO TOTAL O PARCIAL
	IF v_tipoTraspaso = 'TOTAL' THEN
		v_modifSource:= 'TRASPASO TOTAL MEDIADOR MEDIADOR CON DERECHOS Y OBLIGACIONES ' || v_caseId;
	ELSE
		v_modifSource:= 'TRASPASO PARCIAL MEDIADOR MEDIADOR CON DERECHOS Y OBLIGACIONES ' || v_caseId;
	END IF;
	
	
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'TRASPASO MEDIADOR > MEDIADOR ' || UPPER(:v_tipoTraspaso)  || ' MEDIADOR CEDENTE: '|| :v_codMediadorCedente ||'-'||:v_subClaveMediadorCedente , CReport, io_contador);
	
    -- ABRIR CURSOR
    OPEN CURSOR_TRASPASOS;

    FOR CT AS CURSOR_TRASPASOS
	DO
	    -- select CR.CODIGOMEDIADORRECEPTOR,CR.SUBCLAVEMEDIADORRECEPTOR
	    -- ,CR.PORCENTAJE_INTERMEDIACION_RECEPTOR, CR.FECHATRASPASO
	    -- ,CR.CODIGOMEDIADORCEDENTE,CR.subClaveMediadorCedente,CR.NUM_POLIZA_CEDENTE from dummy;
	    
	   
        -- INSERTAMOS PÓLIZAS MEDIADOR RECEPTOR
        INSERT INTO EXT.CARTERA 
        SELECT 
        C."RAMO",
        C."IDPRODUCT",
        C."NUM_POLIZA",
        C."IDMODALIDAD",
        C."IDSUBMODALIDAD",
        C."NUM_FIANZA",
        C."NUM_EXPEDIENTE",
        C."NUM_AVAL_HOST",
        C."NUM_ANUALIDAD",
        C."FECHA_EMISION",
        C."FECHA_EFECTO",
        C."FECHA_VENCIMIENTO",
        C."IDPAIS",
        C."PRIMA_PROVISIONAL_INT",
        C."PRIMA_PROVISIONAL_EXT",
        C."IDDIVISA_INT",
        C."IDDIVISA_EXT",
        C."IDDIVISA_COBERTURA",
        C."PRIMA_MIN_INT",
        C."PRIMA_MIN_EXT",
        CT.COD_MEDIADOR_RECEPTOR,
        CT.SUBCLAVE_RECEPTOR,
        CT.INTERMEDIACION_RECEPTOR,
        C.FECHA_EFECTO, --FECHA_INICIO
        '2200-01-01',
        C."P_ESPECIAL_EMISION",
        C."P_ESPECIAL_RENOVACION",
        C."NIF_TOMADOR",
        C."NOMBRE_TOMADOR",
        C."FECHA_EFECTO_TRASPASO",
        C."MEDIADOR_PRINCIPAL_CIC",
        C."ACTIVO",
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'SMM',
        v_modifSource,
        NULL,
        NULL
        FROM EXT.CARTERA C INNER JOIN (SELECT *
						FROM (
							SELECT *
							,ROW_NUMBER() OVER (PARTITION BY NUM_POLIZA, COD_MEDIADOR ORDER BY NUM_ANUALIDAD DESC) AS RN
							FROM EXT.CARTERA CAR WHERE CAR.COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE AND CAR.NUM_POLIZA = CT.NUM_POLIZA AND CAR.RAMO = 'CREDITO' AND CAR.ACTIVO = 1
							AND CAR.NUM_POLIZA = CT.NUM_POLIZA
							
						) WHERE RN = 1) CRT ON C.NUM_POLIZA = CRT.NUM_POLIZA AND C.COD_MEDIADOR = CRT.COD_MEDIADOR AND C.COD_SUBCLAVE = CRT.COD_SUBCLAVE AND C.NUM_ANUALIDAD = CRT.NUM_ANUALIDAD
        WHERE C.COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE
        AND C.COD_SUBCLAVE = CT.SUBCLAVE_CEDENTE
        AND C.RAMO = 'CREDITO'
        AND C.ACTIVO = 1
        AND C.NUM_POLIZA = CT.NUM_POLIZA        
        ORDER BY NUM_POLIZA,NUM_ANUALIDAD;
        
        CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. PÓLIZA ' || CT.NUM_POLIZA || ' - MEDIADOR RECEPTOR ' || CT.COD_MEDIADOR_RECEPTOR||'-'||CT.SUBCLAVE_RECEPTOR, cReport, io_contador);

    
		END FOR;
	CLOSE CURSOR_TRASPASOS;
	
	--ACTUALIZAMOS MEDIADOR CEDENTE 
	--ANUALIDAD ACTUAL
	UPDATE C
	SET ACTIVO = 0		
		, MODIF_USER = 'SMM'
		, MODIF_SOURCE = v_modifSource
		, MODIF_DATE = CURRENT_TIMESTAMP
	FROM EXT.CARTERA C INNER JOIN (SELECT *
						FROM (
							SELECT *
							,ROW_NUMBER() OVER (PARTITION BY NUM_POLIZA, COD_MEDIADOR ORDER BY NUM_ANUALIDAD DESC) AS RN
							FROM EXT.CARTERA CART WHERE COD_MEDIADOR = CART.COD_MEDIADOR AND NUM_POLIZA = CART.NUM_POLIZA AND RAMO = 'CREDITO' AND ACTIVO = 1
							
						) WHERE RN = 1) CRT ON C.NUM_POLIZA = CRT.NUM_POLIZA AND C.COD_MEDIADOR = CRT.COD_MEDIADOR AND C.COD_SUBCLAVE = CRT.COD_SUBCLAVE AND C.NUM_ANUALIDAD = CRT.NUM_ANUALIDAD
	WHERE C.COD_MEDIADOR = :v_codMediadorCedente
	AND C.COD_SUBCLAVE = :v_subClaveMediadorCedente
	AND C.RAMO = 'CREDITO'
	AND C.ACTIVO = 1
	AND C.NUM_POLIZA IN (SELECT DISTINCT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId)
	;
	
	--RESTO DE ANUALIDADES
	UPDATE C
	SET FECHA_FIN = (SELECT FECHA_VENCIMIENTO
						FROM (
							SELECT DISTINCT FECHA_VENCIMIENTO
							,ROW_NUMBER() OVER (PARTITION BY NUM_POLIZA, COD_MEDIADOR ORDER BY NUM_ANUALIDAD DESC) AS RN
							FROM EXT.CARTERA WHERE COD_MEDIADOR IN (SELECT DISTINCT COD_MEDIADOR_RECEPTOR FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId)
						)
					)
		, MODIF_USER = 'SMM'
		, MODIF_SOURCE = v_modifSource
		, MODIF_DATE = CURRENT_TIMESTAMP
	FROM EXT.CARTERA C
	WHERE C.COD_MEDIADOR = :v_codMediadorCedente
	AND RAMO = 'CREDITO'
	AND ACTIVO = 1
	AND C.NUM_POLIZA IN (SELECT DISTINCT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId)
	;
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
	
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);

END;


DO BEGIN 
--DECLARE JSON VARCHAR(5000) = '{"codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000","polizas":[{"num_poliza":"22751","porcentaje_emision":"100"}],"receptor":[{"codigoMediadorReceptor":"1347","subClaveMediadorReceptor":"0000","polizasReceptor":[{"num_poliza":"22751","porcentaje_emision":"50"}]},{"codigoMediadorReceptor":"2244","subClaveMediadorReceptor":"0000","polizasReceptor":[{"num_poliza":"22751","porcentaje_emision":"50"}]}],"fechaTraspaso":"2025-02-11","idCase":"1751"}';
--DECLARE JSON VARCHAR(5000) = '{ "codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000", "polizas":"[{"num_poliza":"22751","porcentaje_intermediacion":"100.000"},{"num_poliza":"9008150","porcentaje_intermediacion":"100.000"},{"num_poliza":"9052663","porcentaje_intermediacion":"100.000"},{"num_poliza":"9053785","porcentaje_intermediacion":"100.000"}]", "receptor":"[{"codigo_mediador":"3071","subclave_mediador":"0000","polizas":[]}]", "fechaTraspaso":"2025-02-12", "caseId":"1779" }';
DECLARE JSON VARCHAR(5000) = '{ "codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000", "polizas":[{"num_poliza":"22751","porcentaje_intermediacion":"100.000"},{"num_poliza":"9008150","porcentaje_intermediacion":"100.000"},{"num_poliza":"9052663","porcentaje_intermediacion":"100.000"},{"num_poliza":"9053785","porcentaje_intermediacion":"100.000"}],"fechaTraspaso":"2025-02-12","caseId":"1779" }';

TRUNCATE TABLE EXT.CARTERA;
INSERT INTO EXT.CARTERA SELECT * FROM EXT.CARTERA_BKP_04022025 ;

DELETE FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito%';

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_ANUALIDAD,COD_MEDIADOR,COD_SUBCLAVE,FECHA_EFECTO,FECHA_VENCIMIENTO,FECHA_INICIO,FECHA_FIN FROM EXT.CARTERA WHERE COD_MEDIADOR = '0004' AND RAMO = 'CREDITO' ORDER BY ACTIVO,COD_MEDIADOR,NUM_POLIZA,NUM_ANUALIDAD;

	
--CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito('{ "codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000", "tipoTraspaso":"total", "polizas":[{"num_poliza":"22751","porcentaje_intermediacion":"100.000"},{"num_poliza":"9008150","porcentaje_intermediacion":"100.000"},{"num_poliza":"9052663","porcentaje_intermediacion":"100.000"},{"num_poliza":"9053785","porcentaje_intermediacion":"100.000"}], "receptor":[{"codigoMediadorReceptor":"3071","subClaveMediadorReceptor":"0000","porcentajeTraspaso":"50","polizasReceptor":[]},{"codigoMediadorReceptor":"2517","subClaveMediadorReceptor":"0000","porcentajeTraspaso":"50","polizasReceptor":[]}], "fechaTraspaso":"2025-02-12", "caseId":"1781" }');
CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito('{ "codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000", "tipoTraspaso":"parcial", "polizas":[{"num_poliza":"22751","porcentaje_intermediacion":"100"},{"num_poliza":"9053785","porcentaje_intermediacion":"100"}], "receptor":[{"codigoMediadorReceptor":"4549","subClaveMediadorReceptor":"0000","porcentajeTraspaso":"100","polizasReceptor":[{"num_poliza":"22751","porcentaje_intermediacion":"50"},{"num_poliza":"9053785","porcentaje_intermediacion":"50"}]},{"codigoMediadorReceptor":"3073","subClaveMediadorReceptor":"0000","porcentajeTraspaso":"100","polizasReceptor":[{"num_poliza":"22751","porcentaje_intermediacion":"50"},{"num_poliza":"9053785","porcentaje_intermediacion":"50"}]}], "fechaTraspaso":"2025-02-13", "caseId":"1788" }');

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_ANUALIDAD,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_EFECTO,FECHA_VENCIMIENTO,FECHA_INICIO,FECHA_FIN,MODIF_SOURCE,MODIF_DATE FROM EXT.CARTERA WHERE (COD_MEDIADOR = '0004' OR MODIF_USER = 'SMM') AND RAMO = 'CREDITO' ORDER BY ACTIVO,COD_MEDIADOR,NUM_ANUALIDAD,NUM_POLIZA;
SELECT * FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito%';

END;