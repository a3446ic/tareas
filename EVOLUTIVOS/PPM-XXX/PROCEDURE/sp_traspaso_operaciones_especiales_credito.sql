CREATE OR REPLACE PROCEDURE EXT.sp_traspaso_operaciones_especiales_credito (IN caseId BIGINT)
LANGUAGE SQLSCRIPT 
AS
/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 03/02/2025 
	|---------------------------------------------------------------------------------------------- 
	| Procedure Purpose: TRASPASO OPERACIONES ESPECIALES CREDITO
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
    -- CONSTANTES
    DECLARE cReport CONSTANT VARCHAR(250) := 'sp_traspaso_operaciones_especiales_credito';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    DECLARE cRamo CONSTANT VARCHAR(10) := 'CREDITO';
    DECLARE cDerechosObligaciones NVARCHAR(50) := '';

    -------------------------------------------------------------------------------------------
    ------------------------- DECLARACION DE CURSOR    ----------------------------------------
    -------------------------------------------------------------------------------------------
    DECLARE CURSOR CURSOR_TRASPASOS FOR
	SELECT DISTINCT NUM_POLIZA,COD_MEDIADOR_RECEPTOR,SUBCLAVE_RECEPTOR,INTERMEDIACION_RECEPTOR
		,FECHA_EFECTO_SOLICITUD,COD_MEDIADOR_CEDENTE,SUBCLAVE_CEDENTE,FECHA_INICIO_TRASPASO
		,P_ESPECIAL_EMISION,P_ESPECIAL_RENOVACION
	FROM EXT.SOLICITUD_TRASPASO 
	WHERE CASEID = :caseId;
    

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
		INTO v_tipoTraspaso,v_codMediadorCedente,v_subClaveMediadorCedente,v_fechaTraspaso,v_TipoMovimiento,v_FechaIncioTraspaso 
	FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId;
	
    -- TIPO MOVIMIENTO
    SELECT CASE 
    	WHEN v_TipoMovimiento = 1 THEN 'SIN MEDIADOR > MEDIADOR'
    	WHEN v_TipoMovimiento = 2 THEN 'MEDIADOR > MEDIADOR'
    	WHEN v_TipoMovimiento = 3 THEN 'TRASPASO %'
    	WHEN v_TipoMovimiento = 4 THEN 'ERROR CAPTURA'
    	WHEN v_TipoMovimiento = 5 THEN 'MEDIADOR > CANAL DIRECTO'
    	WHEN v_TipoMovimiento = 6 THEN 'OPERACIONES ESPECIALES'
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

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'TRASPASO ' ||:v_tipoTraspaso|| ' ' ||:v_DescTipoMovimiento || ' '  || :cDerechosObligaciones  || ' MEDIADOR CEDENTE: '|| :v_codMediadorCedente ||'-'||:v_subClaveMediadorCedente , CReport, io_contador);
	
	
	
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
	        CASE
	        	-- SI ES LA ANUALIDAD ACTUAL SE COGE LA DE LA SOLICITUD DE TRASPASO
	        	WHEN C_ACT.RN = 1 THEN CT.P_ESPECIAL_EMISION
	        	-- SI NO ES LA ANUALIDAD ACTUAL SE COGE LA QUE VIENE POR DEFECTO
	        	ELSE C.P_ESPECIAL_EMISION
	        END P_ESPECIAL_EMISION, --P_ESPECIAL_EMISION
	        CASE
	        	-- SI ES LA ANUALIDAD ACTUAL SE COGE LA DE LA SOLICITUD DE TRASPASO
	        	WHEN C_ACT.RN = 1 THEN CT.P_ESPECIAL_RENOVACION
	        	-- SI NO ES LA ANUALIDAD ACTUAL SE COGE LA QUE VIENE POR DEFECTO
	        	ELSE C.P_ESPECIAL_RENOVACION
	        END P_ESPECIAL_RENOVACION, --P_ESPECIAL_RENOVACION
	        C."NIF_TOMADOR",
	        C."NOMBRE_TOMADOR",
	        C."FECHA_EFECTO_TRASPASO",
	        C."MEDIADOR_PRINCIPAL_CIC",
	        1, --ACTIVO
	        CURRENT_TIMESTAMP,
	        CURRENT_TIMESTAMP,
	        'SMM',
	        v_modifSource,
	        CASE
	        	-- APLICAR ANUALIDAD ACTUAL: FECHA EFECTO DE LA ANUALIDAD ACTUAL
	        	WHEN UPPER(v_FechaIncioTraspaso) = 'C' AND C_ACT.RN = 1 THEN C_ACT.FECHA_EFECTO 
	        	-- APLICAR ANUALIDAD ANTERIOR: FECHA EFECTO DE LA ANUALIDAD ANTERIOR PARA LA ANUALIDAD ACTUAL Y LA ANTERIOR
	        	WHEN UPPER(v_FechaIncioTraspaso) = 'N' AND C.NUM_ANUALIDAD IN (C_ACT.NUM_ANUALIDAD, C_ACT.NUM_ANUALIDAD-1) THEN (SELECT FECHA_EFECTO FROM (SELECT CRT.FECHA_EFECTO
	    												,ROW_NUMBER() OVER (PARTITION BY CT.NUM_POLIZA ORDER BY CRT.NUM_ANUALIDAD DESC) AS RN 
	    												FROM EXT.CARTERA CRT
	    												WHERE CRT.RAMO = UPPER(cRamo)
														    	AND CRT.NUM_POLIZA = CT.NUM_POLIZA
														    	AND CRT.COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE AND CRT.COD_SUBCLAVE = CT.SUBCLAVE_CEDENTE
														    	AND CRT.ACTIVO = 1
														    	) WHERE RN = 2)
				-- FINALIZAR ANUALIDAD ACTUAL: FECHA TRASPASO SOLICITUD
	        	WHEN UPPER(v_FechaIncioTraspaso) = 'F' AND C_ACT.RN = 1 THEN CT.FECHA_EFECTO_SOLICITUD 
	        	-- FINALIZAR ANUALIDAD ANTERIOR: FECHA TRASPASO SOLICITUD
	        	WHEN UPPER(v_FechaIncioTraspaso) = 'Z' AND C_ACT.RN = 1 THEN NULL
				ELSE C.FECHA_INICIO_OPESP
	        END, --FECHA_INICIO_OPESP,
	        CASE
	        -- SI LA ANUALIDAD ANTERIOR TIENE OPERACIÓN ESPECIAL SE CIERRA CON LA FECHA DE VENCIMIENTO DE LA ANUALIDAD ANTERIOR
	        	WHEN (C_ACT.P_ESPECIAL_EMISION IS NOT NULL OR C_ACT.P_ESPECIAL_EMISION <> 0) 
	        	OR (C_ACT.P_ESPECIAL_RENOVACION IS NOT NULL OR C_ACT.P_ESPECIAL_RENOVACION <> 0) THEN C_ACT.FECHA_VENCIMIENTO
	        END FECHA_FIN_OPESP
        FROM EXT.CARTERA C 
        -- JOIN CON LA ANUALIDAD ACTUAL Y ANTERIOR
		LEFT JOIN (SELECT *
			,ROW_NUMBER() OVER (PARTITION BY CR.NUM_POLIZA, CR.COD_MEDIADOR ORDER BY CR.NUM_ANUALIDAD DESC) AS RN
			FROM EXT.CARTERA CR WHERE CR.COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE AND CR.NUM_POLIZA = CT.NUM_POLIZA AND CR.RAMO = 'CREDITO' AND CR.ACTIVO = 1
		)	C_ACT ON C_ACT.NUM_POLIZA = C.NUM_POLIZA 	AND C_ACT.COD_MEDIADOR = C.COD_MEDIADOR AND C_ACT.COD_SUBCLAVE = C.COD_SUBCLAVE AND C_ACT.NUM_ANUALIDAD = C.NUM_ANUALIDAD AND C_ACT.RN IN (1,2)		
        WHERE C.COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE
        AND C.COD_SUBCLAVE = CT.SUBCLAVE_CEDENTE
        AND C.RAMO = cRamo
        AND C.ACTIVO = 1
        AND C.NUM_POLIZA = CT.NUM_POLIZA
        ORDER BY C.NUM_POLIZA,C.NUM_ANUALIDAD;
		
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. PÓLIZA ' || CT.NUM_POLIZA || ' - MEDIADOR RECEPTOR ' || CT.COD_MEDIADOR_RECEPTOR||'-'||CT.SUBCLAVE_RECEPTOR, cReport, io_contador);
		
  	END FOR; 
  	CLOSE CURSOR_TRASPASOS;

	

    -------------------------------------------------------------------------------------------
    ------------------------------- ACTUALIZAR MEDIADOR CEDENTE--------------------------------
    -------------------------------------------------------------------------------------------
	UPDATE EXT.CARTERA
    SET ACTIVO = 0,
    FECHA_FIN = ADD_DAYS(v_fechaTraspaso,-1),
    MODIF_USER = 'SMM',
    MODIF_SOURCE = v_modifSource,
    MODIF_DATE = CURRENT_TIMESTAMP
    WHERE COD_MEDIADOR = :v_codMediadorCedente
    AND COD_SUBCLAVE = :v_subClaveMediadorCedente
    AND RAMO = cRamo
    AND ACTIVO = 1
    AND NUM_POLIZA IN (SELECT DISTINCT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId)
    AND MODIF_USER <> 'SMM'
    AND CAST(CREATEDATE AS DATE) <> CURRENT_DATE
    ;
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
    
    --FIN
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);



END;


DO BEGIN 
DECLARE vRAMO VARCHAR(50) = 'CREDITO';
DECLARE vCASEID BIGINT = 1835;

UPDATE EXT.SOLICITUD_TRASPASO SET FECHA_INICIO_OPESP = NULL, fecha_inicio_traspaso = 'N' WHERE CASEID= 1835;

TRUNCATE TABLE EXT.CARTERA;
INSERT INTO EXT.CARTERA SELECT * FROM EXT.CARTERA_BKP_04022025 ;
UPDATE EXT.CARTERA SET P_ESPECIAL_EMISION = 0.1, FECHA_INICIO_OPESP = '2020-04-01' WHERE NUM_POLIZA = 9008150 AND NUM_ANUALIDAD = 19;

DELETE FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_operaciones_especiales_credito%';

SELECT * FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = VCASEID;
SELECT * FROM EXT.CARTERA WHERE COD_MEDIADOR = '0004' AND RAMO = VRAMO ORDER BY COD_MEDIADOR,NUM_AVAL_HOST,NUM_POLIZA,NUM_ANUALIDAD;

CALL EXT.sp_traspaso_operaciones_especiales_credito_V2(vCASEID);

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_ANUALIDAD,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION
,FECHA_INICIO,FECHA_FIN,FECHA_EFECTO,FECHA_VENCIMIENTO
,P_ESPECIAL_EMISION, P_ESPECIAL_RENOVACION
,FECHA_INICIO_OPESP,FECHA_FIN_OPESP
,MODIF_SOURCE,MODIF_DATE,MODIF_USER,CREATEDATE 
FROM EXT.CARTERA 
WHERE (COD_MEDIADOR IN('0004') OR MODIF_USER = 'SMM') AND RAMO = vRAMO
AND NUM_POLIZA = 9008150 ORDER BY ACTIVO,COD_MEDIADOR,NUM_AVAL_HOST,NUM_POLIZA,NUM_ANUALIDAD;


SELECT * FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_operaciones_especiales_credito%';


--UPDATE EXT.SOLICITUD_TRASPASO SET COD_MEDIADOR_CEDENTE = '0004', SUBCLAVE_CEDENTE = '0000' WHERE CASEID = vCASEID;

--SELECT SUM(INTERMEDIACION_RECEPTOR) FROM (SELECT COD_MEDIADOR_RECEPTOR,INTERMEDIACION_RECEPTOR FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = 1793 GROUP BY COD_MEDIADOR_RECEPTOR,INTERMEDIACION_RECEPTOR);
END;