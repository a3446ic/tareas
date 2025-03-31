CREATE OR REPLACE PROCEDURE EXT.SP_TRASPASO_OPERACIONES_ESPECIALES_CAUCION (IN caseId BIGINT, IN nombreCasoOrigen VARCHAR(100))
LANGUAGE SQLSCRIPT 
AS
/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 03/02/2025 
	|---------------------------------------------------------------------------------------------- 
	| Procedure Purpose: TRASPASO OPERACIONES ESPECIALES CAUCION
	| 
	| FECHA_INICIO_TRASPASO:
	|	C: APLICAR ANUALIDAD ACTUAL
	|	N: APLICAR ANUALIDAD ANTERIOR
	|	F: FINALIZAR ANUALIDAD ACTUAL
	|	Z: FINALIZAR ANUALIDAD ANTERIOR
	| Version: 1	
	|
	| 
	|
	----------------------------------------------------------------------------------------------- 
*/
BEGIN
    -------------------------------------------------------------------------------------------
    ------------------------- DECLARACIÓN DE VARIABLES ----------------------------------------
    -------------------------------------------------------------------------------------------
    -- VARIABLES
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
	DECLARE v_TipoMovimiento INT;
	DECLARE v_DescTipoMovimiento NVARCHAR(50);
	DECLARE v_FechaIncioTraspaso VARCHAR(1);
	DECLARE v_NumPoliza VARCHAR(10);
	DECLARE v_ModifUser NVARCHAR(50);
    -- CONSTANTES
    DECLARE cReport CONSTANT VARCHAR(250) := 'SP_TRASPASO_OPERACIONES_ESPECIALES_CAUCION';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    DECLARE cRamo CONSTANT VARCHAR(10) := 'CAUCION';
    DECLARE cDerechosObligaciones NVARCHAR(50) := '';

    -------------------------------------------------------------------------------------------
    ------------------------- DECLARACION DE CURSOR    ----------------------------------------
    -------------------------------------------------------------------------------------------
    DECLARE CURSOR CURSOR_TRASPASOS FOR
	SELECT DISTINCT NUM_POLIZA,COD_MEDIADOR_RECEPTOR,SUBCLAVE_RECEPTOR,INTERMEDIACION_RECEPTOR
		,FECHA_EFECTO_SOLICITUD,COD_MEDIADOR_CEDENTE,SUBCLAVE_CEDENTE,FECHA_INICIO_TRASPASO
		,P_ESPECIAL_EMISION,P_ESPECIAL_RENOVACION,COD_AVAL
	FROM EXT.SOLICITUD_TRASPASO 
	WHERE CASEID = :caseId AND RAMO = cRamo;
    

    -------------------------------------------------------------------------------------------
    ------------------------------- HANDLER EXCEPTION -----------------------------------------
    -------------------------------------------------------------------------------------------
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'SQL ERROR_MESSAGE: ' ||
			IFNULL(::SQL_ERROR_MESSAGE,'') || '. SQL_ERROR_CODE: ' || ::SQL_ERROR_CODE, cReport, io_contador);
	END;
    ---------------------------------------------------------------------------
    
    -------------------------------------------------------------------------------------------
    ------------------------------- OBTENER VARIABLES -----------------------------------------
    -------------------------------------------------------------------------------------------
    -- TENANT
    SELECT EXT.LIB_GLOBAL_CESCE:getTenantID() INTO i_Tenant FROM DUMMY;

    -- INICIO
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INICIO PROCEDIMIENTO v' || cVersion || ' with SESSION_USER '|| SESSION_USER, CReport, io_contador);

	-- OBTENER VALORES
	SELECT DISTINCT 
		CASE WHEN TIPO_TRASPASO = 'N' THEN 'TOTAL' ELSE 'PARCIAL' END
		, COALESCE(COD_MEDIADOR_CEDENTE,'0000')
		, COALESCE(SUBCLAVE_CEDENTE,'0000')
		, FECHA_EFECTO_SOLICITUD
		, TIPO_MOVIMIENTO
		, UPPER(FECHA_INICIO_TRASPASO)
		, 'MANUAL - CASEID: ' || :caseId
		INTO v_tipoTraspaso,v_codMediadorCedente,v_subClaveMediadorCedente,v_fechaTraspaso,v_TipoMovimiento,v_FechaIncioTraspaso, v_ModifUser 
	FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND RAMO = cRamo;
	
    -- TIPO MOVIMIENTO
    SELECT CASE 
    	WHEN v_TipoMovimiento = 1 THEN 'SIN MEDIADOR > MEDIADOR'
    	WHEN v_TipoMovimiento = 2 THEN 'MEDIADOR > MEDIADOR'
    	WHEN v_TipoMovimiento = 3 THEN 'TRASPASO %'
    	WHEN v_TipoMovimiento = 4 THEN 'ERROR CAPTURA'
    	WHEN v_TipoMovimiento = 5 THEN 'MEDIADOR > CANAL DIRECTO'
        WHEN v_TipoMovimiento = 6 THEN 'OPERACIONES ESPECIALES'
        WHEN v_TipoMovimiento = 8 THEN 'ENTRE SUBCLAVES'
    	END
    INTO v_DescTipoMovimiento
    FROM DUMMY;
    
    -- COMPROBAR SI ES TRASPASO TOTAL 'N' O PARCIAL	'P'
    -- IF ((SELECT COUNT(*) FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseID) = (SELECT COUNT(*) FROM EXT.CARTERA WHERE COD_MEDIADOR = :v_codMediadorCedente AND COD_SUBCLAVE = v_subClaveMediadorCedente AND RAMO = cRAMO AND FECHA_VENCIMIENTO >= v_fechaTraspaso)) THEN
    -- 	v_tipoTraspaso:= 'TOTAL';
    -- ELSE
    	v_tipoTraspaso:= 'PARCIAL';
    -- END IF;
    
   
	--  IF v_tipoTraspaso = 'TOTAL' THEN
	-- 	v_modifSource:= 'TRASPASO TOTAL '||:v_DescTipoMovimiento|| ' ' || cDerechosObligaciones || ' - CASEID: ' || :caseId;
	-- ELSE
		v_modifSource:= 'TRASPASO PARCIAL '||:v_DescTipoMovimiento|| ' ' || cDerechosObligaciones || ' - CASEID: ' || :caseId;
	-- END IF;

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASE ID: ' ||:caseID|| ' - TRASPASO AVAL ' ||v_tipoTraspaso|| ' ' ||:v_DescTipoMovimiento || ' '  || :cDerechosObligaciones  || ' MEDIADOR CEDENTE: '|| :v_codMediadorCedente ||'-'||:v_subClaveMediadorCedente , CReport, io_contador);
	
	
	
	-------------------------------------------------------------------------------------------
    ------------------------------- INSERTAR PÓLIZA TRASPASO -----------------------------------------
    -------------------------------------------------------------------------------------------
    -- ABRIR CURSOR
    OPEN CURSOR_TRASPASOS;
    FOR CT AS CURSOR_TRASPASOS DO
 
		v_NumPoliza:= CT.NUM_POLIZA;
	
		
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
	        C.P_INTERMEDIACION,
	        CT.FECHA_EFECTO_SOLICITUD,
	        '2200-01-01',
	        CT.P_ESPECIAL_EMISION, --P_ESPECIAL_EMISION
	        CT.P_ESPECIAL_RENOVACION, --P_ESPECIAL_RENOVACION
	        C."NIF_TOMADOR",
	        C."NOMBRE_TOMADOR",
	        C."FECHA_EFECTO_TRASPASO",
	        C."MEDIADOR_PRINCIPAL_CIC",
	        1, --ACTIVO
	        CURRENT_TIMESTAMP,
	        CURRENT_TIMESTAMP,
	        v_ModifUser,
	        nombreCasoOrigen,
	        CASE
	        	-- APLICAR ANUALIDAD ACTUAL: FECHA EFECTO DE LA ANUALIDAD ACTUAL
	        	WHEN v_FechaIncioTraspaso = 'C' AND C.NUM_AVAL_HOST = CT.COD_AVAL THEN CT.FECHA_EFECTO_SOLICITUD
	        	WHEN v_FechaIncioTraspaso = 'F' AND C.NUM_AVAL_HOST = CT.COD_AVAL THEN NULL
				ELSE C.FECHA_INICIO_OPESP
	        END, --FECHA_INICIO_OPESP,
	        CASE
	        -- SI LA ANUALIDAD ANTERIOR TIENE OPERACIÓN ESPECIAL SE CIERRA CON LA FECHA DE VENCIMIENTO DE LA ANUALIDAD ANTERIOR
	        	WHEN v_FechaIncioTraspaso = 'C' AND C.NUM_AVAL_HOST = CT.COD_AVAL THEN NULL
	        	WHEN v_FechaIncioTraspaso = 'F' AND C.NUM_AVAL_HOST = CT.COD_AVAL THEN NULL
	        	ELSE C.FECHA_FIN_OPESP
	        END --FECHA_FIN_OPESP
        FROM EXT.CARTERA C 
        WHERE C.COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE
        AND C.COD_SUBCLAVE = CT.SUBCLAVE_CEDENTE
        AND C.RAMO = cRamo
        AND C.ACTIVO = 1
        AND C.NUM_POLIZA = CT.NUM_POLIZA
        AND C.NUM_AVAL_HOST = CT.COD_AVAL
        ORDER BY C.NUM_POLIZA,C.NUM_AVAL_HOST;
		
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. PÓLIZA ' || CT.NUM_POLIZA || ' - MEDIADOR RECEPTOR ' || CT.COD_MEDIADOR_RECEPTOR||'-'||CT.SUBCLAVE_RECEPTOR, cReport, io_contador);
		
  	END FOR; 
  	CLOSE CURSOR_TRASPASOS;

	

    -------------------------------------------------------------------------------------------
    ------------------------------- ACTUALIZAR MEDIADOR CEDENTE--------------------------------
    -------------------------------------------------------------------------------------------
	UPDATE EXT.CARTERA
    SET FECHA_FIN = ADD_DAYS(v_fechaTraspaso,-1),/*(CASE
    	WHEN v_FechaIncioTraspaso = 'C' THEN ADD_DAYS(v_fechaTraspaso,-1)
    	WHEN v_FechaIncioTraspaso = 'F' THEN FECHA_FIN
    END),*/
    MODIF_USER = v_ModifUser,
    MODIF_SOURCE = nombreCasoOrigen,
    MODIF_DATE = CURRENT_TIMESTAMP
    WHERE COD_MEDIADOR = :v_codMediadorCedente
    AND COD_SUBCLAVE = :v_subClaveMediadorCedente
    AND RAMO = cRamo
    AND ACTIVO = 1
    AND NUM_POLIZA IN (SELECT DISTINCT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND RAMO = cRamo)
    AND MODIF_USER <> v_ModifUser
    AND NUM_AVAL_HOST IN (SELECT DISTINCT COD_AVAL FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND RAMO = cRamo)
    ;
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
    
    --FIN
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);



END;


DO BEGIN 
DECLARE vRAMO VARCHAR(50) = 'CAUCION';
-- FINALIZAR F
DECLARE vCASEID BIGINT = 1904;
DECLARE V_COD_MEDIADOR VARCHAR(4) = '0062';
DECLARE V_COD_SUBCLAVE VARCHAR(4) = '0000';
DECLARE V_NUM_POLIZA VARCHAR(10) = 1001196;
DECLARE V_NUM_AVAL VARCHAR(10) = 203001;
-- APLICAR C
-- DECLARE vCASEID BIGINT = 1902;
-- DECLARE V_COD_MEDIADOR VARCHAR(4) = '0004';
-- DECLARE V_COD_SUBCLAVE VARCHAR(4) = '0000';
-- DECLARE V_NUM_POLIZA VARCHAR(10) = 1000257;
-- DECLARE V_NUM_AVAL VARCHAR(10) = 203001;

--UPDATE EXT.SOLICITUD_TRASPASO SET FECHA_INICIO_OPESP = NULL, fecha_inicio_traspaso = 'N' WHERE CASEID= 1835;

TRUNCATE TABLE EXT.CARTERA;
INSERT INTO EXT.CARTERA SELECT * FROM EXT.CARTERA_BKP_04022025 ;
--UPDATE EXT.CARTERA SET P_ESPECIAL_EMISION = 0.1, FECHA_INICIO_OPESP = '2020-04-01' WHERE NUM_POLIZA = 9008150 AND NUM_ANUALIDAD = 20;

DELETE FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%SP_TRASPASO_OPERACIONES_ESPECIALES_CAUCION%';

SELECT * FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = VCASEID;
SELECT * FROM EXT.CARTERA 
WHERE COD_MEDIADOR = V_COD_MEDIADOR 
AND COD_SUBCLAVE = V_COD_SUBCLAVE 
AND RAMO = VRAMO 
AND NUM_POLIZA = V_NUM_POLIZA
-- AND NUM_AVAL_HOST = V_NUM_AVAL
ORDER BY COD_MEDIADOR,NUM_AVAL_HOST,NUM_POLIZA,NUM_ANUALIDAD;

CALL EXT.sp_traspaso_operaciones_especiales_caucion(vCASEID,'WF TRASPASO');

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_ANUALIDAD,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION
,FECHA_INICIO,FECHA_FIN,FECHA_EFECTO,FECHA_VENCIMIENTO
,P_ESPECIAL_EMISION, P_ESPECIAL_RENOVACION
,FECHA_INICIO_OPESP,FECHA_FIN_OPESP
,MODIF_SOURCE,MODIF_DATE,MODIF_USER,CREATEDATE 
FROM EXT.CARTERA 
WHERE (COD_MEDIADOR IN(V_COD_MEDIADOR) ) and cod_subclave = V_COD_SUBCLAVE AND RAMO = vRAMO
AND NUM_POLIZA = V_NUM_POLIZA 
--AND NUM_AVAL_HOST = V_NUM_AVAL
ORDER BY ACTIVO,COD_MEDIADOR,NUM_AVAL_HOST,NUM_POLIZA,NUM_ANUALIDAD,CREATEDATE;



SELECT * FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_operaciones_especiales_caucion%';


--UPDATE EXT.SOLICITUD_TRASPASO SET COD_MEDIADOR_CEDENTE = '0004', SUBCLAVE_CEDENTE = '0000' WHERE CASEID = vCASEID;

--SELECT SUM(INTERMEDIACION_RECEPTOR) FROM (SELECT COD_MEDIADOR_RECEPTOR,INTERMEDIACION_RECEPTOR FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = 1793 GROUP BY COD_MEDIADOR_RECEPTOR,INTERMEDIACION_RECEPTOR);
END;