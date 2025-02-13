CREATE OR REPLACE PROCEDURE EXT.sp_traspaso_mediador_mediador_con_derechos_credito_caseid (IN caseId BIGINT)
LANGUAGE SQLSCRIPT 
AS
/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 03/02/2025 
	|---------------------------------------------------------------------------------------------- 
	| Procedure Purpose: TRASPASO DE CARTERA DE UN MEDIADOR A OTRO MEDIADOR CON DERECHOS Y OBLIGACIONES
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
    DECLARE cReport CONSTANT VARCHAR(50) := 'sp_traspaso_mediador_mediador_con_derechos_credito';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    DECLARE cRamo CONSTANT VARCHAR(10) := 'CREDITO';
    DECLARE cDerechosObligaciones NVARCHAR(50) := 'CON DERECHOS Y OBLIGACIONES';
    
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


	SELECT * FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId;
	
	--OBTENER VALORES
	SELECT DISTINCT 
		CASE WHEN TIPO_TRASPASO = 'N' THEN 'TOTAL' ELSE 'PARCIAL' END
		, COD_MEDIADOR_CEDENTE
		, SUBCLAVE_CEDENTE
		, FECHA_EFECTO_SOLICITUD
		INTO v_tipoTraspaso,v_codMediadorCedente,v_subClaveMediadorCedente,v_fechaTraspaso 
	FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId;
	
    
    -- COMPROBAR SI ES TRASPASO TOTAL 'N' O PARCIAL	'P'
	IF v_tipoTraspaso = 'TOTAL' THEN
		v_modifSource:= 'TRASPASO TOTAL MEDIADOR MEDIADOR CON DERECHOS Y OBLIGACIONES ' || :caseId;
	ELSE
		v_modifSource:= 'TRASPASO PARCIAL MEDIADOR MEDIADOR CON DERECHOS Y OBLIGACIONES ' || :caseId;
	END IF;
	
	
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'TRASPASO MEDIADOR > MEDIADOR ' || UPPER(:v_tipoTraspaso)  || ' MEDIADOR CEDENTE: '|| :v_codMediadorCedente ||'-'||:v_subClaveMediadorCedente , CReport, io_contador);
	
    OPEN CURSOR_TRASPASOS;
    FOR CT AS CURSOR_TRASPASOS DO
		-- SELECT 	CT.NUM_POLIZA,CT.COD_MEDIADOR_RECEPTOR,CT.SUBCLAVE_RECEPTOR,CT.INTERMEDIACION_RECEPTOR,CT.FECHA_EFECTO_SOLICITUD FROM DUMMY;
		-- INSERTAMOS PÓLIZAS MEDIADOR RECEPTOR
        INSERT INTO EXT.CARTERA 
        SELECT 
        "RAMO",
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
        CT.FECHA_EFECTO_SOLICITUD,
        '2200-01-01',
        "P_ESPECIAL_EMISION",
        "P_ESPECIAL_RENOVACION",
        "NIF_TOMADOR",
        "NOMBRE_TOMADOR",
        "FECHA_EFECTO_TRASPASO",
        "MEDIADOR_PRINCIPAL_CIC",
        "ACTIVO",
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'SMM',
        v_modifSource,
        NULL,
        NULL
        FROM EXT.CARTERA
        WHERE COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE
        AND COD_SUBCLAVE = CT.SUBCLAVE_CEDENTE
        AND RAMO = 'CREDITO'
        AND ACTIVO = 1
        AND NUM_POLIZA = CT.NUM_POLIZA
        ORDER BY NUM_POLIZA,NUM_ANUALIDAD;
        
        CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. PÓLIZA ' || CT.NUM_POLIZA || ' - MEDIADOR RECEPTOR ' || CT.COD_MEDIADOR_RECEPTOR||'-'||CT.SUBCLAVE_RECEPTOR, cReport, io_contador);
	
    END FOR;
    CLOSE CURSOR_TRASPASOS;
    
    
    -- --ACTUALIZAMOS MEDIADOR CEDENTE
    UPDATE EXT.CARTERA
    SET ACTIVO = 0,
    FECHA_FIN = ADD_DAYS(v_fechaTraspaso,-1),
    MODIF_USER = 'SMM',
    MODIF_SOURCE = v_modifSource,
    MODIF_DATE = CURRENT_TIMESTAMP
    WHERE COD_MEDIADOR = :v_codMediadorCedente
    AND COD_SUBCLAVE = :v_subClaveMediadorCedente
    AND RAMO = 'CREDITO'
    AND ACTIVO = 1
    AND NUM_POLIZA IN (SELECT DISTINCT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId)
    ;
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
	
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);

END;


DO BEGIN 


TRUNCATE TABLE EXT.CARTERA;
INSERT INTO EXT.CARTERA SELECT * FROM EXT.CARTERA_BKP_04022025 ;

DELETE FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_con_derechos_credito%';

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_ANUALIDAD,COD_MEDIADOR,COD_SUBCLAVE,FECHA_INICIO,FECHA_FIN 
FROM EXT.CARTERA 
WHERE COD_MEDIADOR = '0004' AND RAMO = 'CREDITO' ORDER BY ACTIVO,COD_MEDIADOR,NUM_POLIZA,NUM_ANUALIDAD;

	
CALL EXT.sp_traspaso_mediador_mediador_con_derechos_credito(1796);

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_ANUALIDAD,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO,FECHA_FIN,MODIF_SOURCE,MODIF_DATE 
FROM EXT.CARTERA 
WHERE (COD_MEDIADOR = '0004' OR MODIF_USER = 'SMM') AND RAMO = 'CREDITO' 
ORDER BY ACTIVO,COD_MEDIADOR,NUM_ANUALIDAD,NUM_POLIZA;

SELECT * FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_con_derechos_credito%';

END;