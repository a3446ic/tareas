CREATE OR REPLACE PROCEDURE EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito (IN caseId BIGINT)
LANGUAGE SQLSCRIPT 
AS
/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 03/02/2025 
	|---------------------------------------------------------------------------------------------- 
	| Procedure Purpose: TRASPASO DE CARTERA DE UN MEDIADOR A OTRO MEDIADOR SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN
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
    DECLARE v_tipoTraspasoCaucion NVARCHAR(100);
	DECLARE cTipoMovimiento NVARCHAR(50);
    -- CONSTANTES
    DECLARE cReport CONSTANT VARCHAR(250) := 'sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    DECLARE cRamo CONSTANT VARCHAR(10) := 'CREDITO';
    DECLARE cDerechosObligaciones NVARCHAR(50) := 'SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN ';
    
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
    
	IF v_tipoTraspaso = 'TOTAL' THEN
		v_modifSource:= 'TRASPASO TOTAL MEDIADOR MEDIADOR SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN ' || v_caseId;
	ELSE
		v_modifSource:= 'TRASPASO PARCIAL MEDIADOR MEDIADOR SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN' || v_caseId;
	END IF;
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'TRASPASO MEDIADOR > MEDIADOR ' || UPPER(:v_tipoTraspaso)  || ' MEDIADOR CEDENTE: '|| :v_codMediadorCedente ||'-'||:v_subClaveMediadorCedente , CReport, io_contador);
	
    
    -- ABRIR CURSOR
     OPEN CURSOR_TRASPASOS;

     FOR CT AS CURSOR_TRASPASOS
 	DO
 --select CR.CODIGOMEDIADORRECEPTOR,CR.SUBCLAVEMEDIADORRECEPTOR
 --,CR.PORCENTAJE_INTERMEDIACION_RECEPTOR, CR.FECHATRASPASO,CR.CODIGOMEDIADORCEDENTE,CR.subClaveMediadorCedente from dummy;
--     -- INSERTAMOS PÓLIZAS MEDIADOR RECEPTOR
    INSERT INTO EXT.CARTERA 
    WITH CTE AS (
    SELECT *
    	, ADD_DAYS(FECHA_VENCIMIENTO,1) NEW_FECHA_VENCIMIENTO
        ,ROW_NUMBER() OVER (PARTITION BY NUM_POLIZA, COD_MEDIADOR ORDER BY NUM_ANUALIDAD DESC) AS RN
    FROM EXT.CARTERA 
    WHERE COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE
    AND RAMO = 'CREDITO'
	AND ACTIVO = 1
	AND NUM_POLIZA = CT.NUM_POLIZA  
	)
	SELECT "RAMO",
		"IDPRODUCT",
		"NUM_POLIZA",
		"IDMODALIDAD",
		"IDSUBMODALIDAD",
		"NUM_FIANZA",
		"NUM_EXPEDIENTE",
		"NUM_AVAL_HOST",
		"NUM_ANUALIDAD",
		"FECHA_EMISION",
		"FECHA_EFECTO",
		"FECHA_VENCIMIENTO",
		"IDPAIS",
		"PRIMA_PROVISIONAL_INT",
		"PRIMA_PROVISIONAL_EXT",
		"IDDIVISA_INT",
		"IDDIVISA_EXT",
		"IDDIVISA_COBERTURA",
		"PRIMA_MIN_INT",
		"PRIMA_MIN_EXT",
		CT.COD_MEDIADOR_RECEPTOR,
        CT.SUBCLAVE_RECEPTOR,
        CT.INTERMEDIACION_RECEPTOR,
		NEW_FECHA_VENCIMIENTO,
		'2200-01-01',
		"P_ESPECIAL_EMISION",
		"P_ESPECIAL_RENOVACION",
		"NIF_TOMADOR",
		"NOMBRE_TOMADOR",
		"FECHA_EFECTO_TRASPASO",
		"MEDIADOR_PRINCIPAL_CIC",
		2,
		CURRENT_TIMESTAMP,
		CURRENT_TIMESTAMP,
		'SMM',
		v_modifSource,
		NULL,
		NULL 
	FROM CTE
	WHERE RN = 1
	ORDER BY NUM_POLIZA, NUM_ANUALIDAD;
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. PÓLIZA ' || CT.NUM_POLIZA || ' - MEDIADOR RECEPTOR ' || CT.COD_MEDIADOR_RECEPTOR||'-'||CT.SUBCLAVE_RECEPTOR, cReport, io_contador);
	
  END FOR; 
  CLOSE CURSOR_TRASPASOS;
    --ACTUALIZAMOS MEDIADOR CEDENTE
 
	UPDATE C
	SET FECHA_FIN = (SELECT FECHA_VENCIMIENTO
						FROM (
							SELECT NUM_POLIZA,NUM_ANUALIDAD,COD_MEDIADOR,FECHA_VENCIMIENTO
							,ROW_NUMBER() OVER (PARTITION BY NUM_POLIZA, COD_MEDIADOR ORDER BY NUM_ANUALIDAD DESC) AS RN
							FROM EXT.CARTERA WHERE COD_MEDIADOR = C.COD_MEDIADOR AND NUM_POLIZA = C.NUM_POLIZA AND RAMO = 'CREDITO' AND ACTIVO = 1  
						) CT WHERE RN = 1
					)
		, MODIF_USER = 'SMM'
		, MODIF_SOURCE = v_modifSource
		, MODIF_DATE = CURRENT_TIMESTAMP
	FROM EXT.CARTERA C
	WHERE C.COD_MEDIADOR = v_codMediadorCedente
	AND RAMO = 'CREDITO'
	AND ACTIVO = 1
	AND NUM_POLIZA IN (SELECT DISTINCT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId);
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
	
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);

	

END;


DO BEGIN 
--DECLARE JSON VARCHAR(5000) = '{"codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000","polizas":[{"num_poliza":"22751","porcentaje_emision":"100"}],"receptor":[{"codigoMediadorReceptor":"1347","subClaveMediadorReceptor":"0000","polizasReceptor":[{"num_poliza":"22751","porcentaje_emision":"50"}]},{"codigoMediadorReceptor":"2244","subClaveMediadorReceptor":"0000","polizasReceptor":[{"num_poliza":"22751","porcentaje_emision":"50"}]}],"fechaTraspaso":"2025-02-11","idCase":"1751"}';
--DECLARE JSON VARCHAR(5000) = '{ "codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000", "polizas":"[{"num_poliza":"22751","porcentaje_intermediacion":"100.000"},{"num_poliza":"9008150","porcentaje_intermediacion":"100.000"},{"num_poliza":"9052663","porcentaje_intermediacion":"100.000"},{"num_poliza":"9053785","porcentaje_intermediacion":"100.000"}]", "receptor":"[{"codigo_mediador":"3071","subclave_mediador":"0000","polizas":[]}]", "fechaTraspaso":"2025-02-12", "caseId":"1779" }';
DECLARE JSON VARCHAR(5000) = '{ "codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000", "polizas":[{"num_poliza":"22751","porcentaje_intermediacion":"100.000"},{"num_poliza":"9008150","porcentaje_intermediacion":"100.000"},{"num_poliza":"9052663","porcentaje_intermediacion":"100.000"},{"num_poliza":"9053785","porcentaje_intermediacion":"100.000"}],"fechaTraspaso":"2025-02-12","caseId":"1779" }';

TRUNCATE TABLE EXT.CARTERA;
INSERT INTO EXT.CARTERA SELECT * FROM EXT.CARTERA_BKP_04022025 ;

DELETE FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito%';

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_ANUALIDAD,COD_MEDIADOR,COD_SUBCLAVE,FECHA_VENCIMIENTO,FECHA_INICIO,FECHA_FIN FROM EXT.CARTERA WHERE COD_MEDIADOR = '0004' AND RAMO = 'CREDITO' ORDER BY ACTIVO,COD_MEDIADOR,NUM_POLIZA,NUM_ANUALIDAD;

	
--CALL EXT.sp_traspaso_mediador_mediador_con_derechos_credito(JSON);
--CALL EXT.sp_traspaso_mediador_mediador_con_derechos_credito('{ "codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000", "tipoTraspaso":"total", "polizas":[{"num_poliza":"22751","porcentaje_intermediacion":"100.000"},{"num_poliza":"9008150","porcentaje_intermediacion":"100.000"},{"num_poliza":"9052663","porcentaje_intermediacion":"100.000"},{"num_poliza":"9053785","porcentaje_intermediacion":"100.000"}], "receptor":[{"codigo_mediador":"3071","subclave_mediador":"0000","polizas":[]}], "fechaTraspaso":"2025-02-12", "caseId":"1779" }');
--CALL EXT.sp_traspaso_mediador_mediador_con_derechos_credito('{ "codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000", "tipoTraspaso":"total", "polizas":[{"num_poliza":"22751","porcentaje_intermediacion":"100.000"},{"num_poliza":"9008150","porcentaje_intermediacion":"100.000"},{"num_poliza":"9052663","porcentaje_intermediacion":"100.000"},{"num_poliza":"9053785","porcentaje_intermediacion":"100.000"}], "receptor":[{"codigoMediadorReceptor":"3071","subClaveMediadorReceptor":"0000","polizasReceptor":[]}], "fechaTraspaso":"2025-02-12", "caseId":"1779" }');
CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito('{ "codigoMediadorCedente":"0004", "subClaveMediadorCedente":"0000", "tipoTraspaso":"parcial", "polizas":[{"num_poliza":"22751","porcentaje_intermediacion":"100"},{"num_poliza":"9053785","porcentaje_intermediacion":"100"}], "receptor":[{"codigoMediadorReceptor":"4549","subClaveMediadorReceptor":"0000","porcentajeTraspaso":"100","polizasReceptor":[{"num_poliza":"22751","porcentaje_intermediacion":"50"},{"num_poliza":"9053785","porcentaje_intermediacion":"50"}]},{"codigoMediadorReceptor":"3073","subClaveMediadorReceptor":"0000","porcentajeTraspaso":"100","polizasReceptor":[{"num_poliza":"22751","porcentaje_intermediacion":"50"},{"num_poliza":"9053785","porcentaje_intermediacion":"50"}]}], "fechaTraspaso":"2025-02-13", "caseId":"1788" }');

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_ANUALIDAD,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_VENCIMIENTO,FECHA_INICIO,FECHA_FIN,MODIF_SOURCE,MODIF_DATE FROM EXT.CARTERA WHERE (COD_MEDIADOR = '0004' OR MODIF_USER = 'SMM') AND RAMO = 'CREDITO' ORDER BY ACTIVO,COD_MEDIADOR,NUM_ANUALIDAD,NUM_POLIZA;
SELECT * FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito%';

END;