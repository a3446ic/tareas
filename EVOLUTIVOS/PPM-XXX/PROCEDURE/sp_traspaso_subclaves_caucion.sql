CREATE OR REPLACE PROCEDURE EXT.sp_traspaso_subclaves_caucion (IN caseId BIGINT, IN nombreCasoOrigen VARCHAR(100))
LANGUAGE SQLSCRIPT 
AS
/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 03/02/2025 
	|---------------------------------------------------------------------------------------------- 
	| Procedure Purpose: TRASPASO DE CARTERA ENTRE SUBCLAVES CAUCIÓN
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
	DECLARE v_TipoMovimiento INT;
	DECLARE v_DescTipoMovimiento NVARCHAR(50);
	DECLARE v_ModifUser NVARCHAR(50);
    -- CONSTANTES
    DECLARE cReport CONSTANT VARCHAR(50) := 'sp_traspaso_subclaves_caucion';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    DECLARE cRamo CONSTANT VARCHAR(10) := 'CAUCION';
    DECLARE cDerechosObligaciones NVARCHAR(50) := 'CAUCION';
    
	-- DECLARACION DE CURSOR    
    DECLARE CURSOR CURSOR_TRASPASOS FOR
	SELECT DISTINCT NUM_POLIZA,COD_MEDIADOR_RECEPTOR,SUBCLAVE_RECEPTOR,INTERMEDIACION_RECEPTOR,FECHA_EFECTO_SOLICITUD,COD_MEDIADOR_CEDENTE,SUBCLAVE_CEDENTE,COD_AVAL
	FROM EXT.SOLICITUD_TRASPASO 
	WHERE CASEID = :caseId
	ORDER BY NUM_POLIZA,COD_AVAL;

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
		, TIPO_MOVIMIENTO
		, 'MANUAL - CASEID: ' || :caseId
		INTO v_tipoTraspaso,v_codMediadorCedente,v_subClaveMediadorCedente,v_fechaTraspaso,v_TipoMovimiento, v_ModifUser  
	FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId;
    
	-- TIPO MOVIMIENTO
    SELECT CASE 
    	WHEN v_TipoMovimiento = 1 THEN 'SIN MEDIADOR > MEDIADOR'
    	WHEN v_TipoMovimiento = 2 THEN 'MEDIADOR > MEDIADOR'
    	WHEN v_TipoMovimiento = 3 THEN 'TRASPASO %'
    	WHEN v_TipoMovimiento = 4 THEN 'ERROR CAPTURA'
    	WHEN v_TipoMovimiento = 5 THEN 'MEDIADOR > CANAL DIRECTO'
        WHEN v_TipoMovimiento = 8 THEN 'ENTRE SUBCLAVES'
    	END
    INTO v_DescTipoMovimiento
    FROM DUMMY;

    -- COMPROBAR SI ES TRASPASO TOTAL 'N' O PARCIAL	'P'
    IF ((SELECT COUNT(*) FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseID) = (SELECT COUNT(DISTINCT COD_SUBCLAVE) FROM EXT.CARTERA WHERE COD_MEDIADOR = :v_codMediadorCedente AND COD_SUBCLAVE <> v_subClaveMediadorCedente AND RAMO = cRAMO AND FECHA_VENCIMIENTO >= v_fechaTraspaso)) THEN
    	v_tipoTraspaso:= 'TOTAL';
    ELSE
    	v_tipoTraspaso:= 'PARCIAL';
    END IF;
	
	IF EXISTS(SELECT 1 FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND COD_AVAL IS NULL) 
		AND NOT EXISTS(SELECT 1 FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND COD_AVAL IS NOT NULL) THEN
		
		v_tipoTraspasoCaucion := 'expediente';
	ELSE
		v_tipoTraspasoCaucion := 'aval';
	END IF;

	IF v_tipoTraspasoCaucion = 'expediente' THEN
		
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASE ID: ' ||:caseID|| ' - TRASPASO EXPEDIENTE ' ||v_tipoTraspaso|| ' ' ||:v_DescTipoMovimiento || ' '  || :cDerechosObligaciones  || ' MEDIADOR CEDENTE: '|| :v_codMediadorCedente ||'-'||:v_subClaveMediadorCedente , CReport, io_contador);
	
		v_modifSource:= 'EXPEDIENTE MEDIADOR MEDIADOR '|| :cDerechosObligaciones || ' ' || :caseId;
		
		-- ABRIR CURSOR
	    OPEN CURSOR_TRASPASOS;
	    FOR CT AS CURSOR_TRASPASOS DO		
		
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
			-- (CASE WHEN NUM_FIANZA IS NULL THEN v_fechaTraspaso ELSE FECHA_INICIO END), --FECHA_INICIO
            "FECHA_INICIO",
			-- '2200-01-01', --FECHA_FIN
            "FECHA_FIN",
			"P_ESPECIAL_EMISION",
			"P_ESPECIAL_RENOVACION",
			"NIF_TOMADOR",
			"NOMBRE_TOMADOR",
			"FECHA_EFECTO_TRASPASO",
			"MEDIADOR_PRINCIPAL_CIC",
			"ACTIVO",
			CURRENT_TIMESTAMP, --CREATION_DATE
			CURRENT_TIMESTAMP, --MODIF_DATE
			v_ModifUser, --CREATION_USER
			nombreCasoOrigen, --MODIF_SOURCE
			NULL, --FECHA_INICIO_OPESP
			NULL -- FECHA_FIN_OPESP
			FROM EXT.CARTERA
			WHERE COD_MEDIADOR = :v_codMediadorCedente
			AND COD_SUBCLAVE = :v_subClaveMediadorCedente
			AND RAMO = cRamo
			AND ACTIVO = 1
			AND NUM_POLIZA = CT.NUM_POLIZA
			ORDER BY NUM_POLIZA,NUM_ANUALIDAD;

			CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. PÓLIZA ' || CT.NUM_POLIZA || ' - MEDIADOR RECEPTOR ' || CT.COD_MEDIADOR_RECEPTOR||'-'||CT.SUBCLAVE_RECEPTOR, cReport, io_contador);
	    
		END FOR;
		CLOSE CURSOR_TRASPASOS;


	    --ACTUALIZAMOS MEDIADOR CEDENTE
	    UPDATE EXT.CARTERA
	    SET ACTIVO = 0,	   
	    -- FECHA_FIN = (CASE WHEN NUM_FIANZA IS NULL THEN ADD_DAYS(v_fechaTraspaso,-1) ELSE FECHA_FIN END),
	    MODIF_USER = v_ModifUser,
	    MODIF_SOURCE = nombreCasoOrigen,
	    MODIF_DATE = CURRENT_TIMESTAMP
	    WHERE COD_MEDIADOR = :v_codMediadorCedente
	    AND COD_SUBCLAVE = :v_subClaveMediadorCedente
	    AND RAMO = cRamo
	    AND ACTIVO = 1
	    AND NUM_POLIZA IN (SELECT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId)
	    ;
		
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
		
		
		
	ELSE
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASE ID: ' ||:caseID|| ' - TRASPASO AVAL ' ||v_tipoTraspaso|| ' ' ||:v_DescTipoMovimiento || ' '  || :cDerechosObligaciones  || ' MEDIADOR CEDENTE: '|| :v_codMediadorCedente ||'-'||:v_subClaveMediadorCedente , CReport, io_contador);
		
		v_modifSource:= 'AVAL MEDIADOR MEDIADOR '|| cDerechosObligaciones ||' ' || :caseId;
		
		-- ABRIR CURSOR
		OPEN CURSOR_TRASPASOS;
    	FOR CT AS CURSOR_TRASPASOS DO

    		INSERT INTO EXT.CARTERA 
		    SELECT 
			"RAMO",
			"IDPRODUCT",
			"NUM_POLIZA",
			"IDMODALIDAD",
			"IDSUBMODALIDAD",
			"NUM_FIANZA",
			"NUM_EXPEDIENTE",
			CT.COD_AVAL,
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
			CT.COD_MEDIADOR_RECEPTOR, --COD_MEDIADOR
			CT.SUBCLAVE_RECEPTOR, --COD_SUBCLAVE
			CT.INTERMEDIACION_RECEPTOR,
	        -- (CASE WHEN NUM_FIANZA IS NULL THEN v_fechaTraspaso ELSE FECHA_INICIO END), --FECHA_INICIO
            "FECHA_INICIO",
			-- '2200-01-01', --FECHA_FIN
            "FECHA_FIN",
			"P_ESPECIAL_EMISION",
			"P_ESPECIAL_RENOVACION",
			"NIF_TOMADOR",
			"NOMBRE_TOMADOR",
			"FECHA_EFECTO_TRASPASO",
			"MEDIADOR_PRINCIPAL_CIC",
			"ACTIVO",
			CURRENT_TIMESTAMP, --CREATION_DATE
			CURRENT_TIMESTAMP, --MODIF_DATE
			v_ModifUser, --CREATION_USER
			nombreCasoOrigen, --MODIF_SOURCE
			NULL, --FECHA_INICIO_OPESP
			NULL -- FECHA_FIN_OPESP
			FROM EXT.CARTERA
		    WHERE COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE
		    AND COD_SUBCLAVE = CT.SUBCLAVE_CEDENTE
		    AND RAMO = cRamo
		    AND ACTIVO = 1
		    AND NUM_AVAL_HOST = CT.COD_AVAL
		  
		    ORDER BY NUM_POLIZA,NUM_ANUALIDAD;
		    
		    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. PÓLIZA ' || CT.NUM_POLIZA || ' AVAL: '||CT.COD_AVAL||' - MEDIADOR RECEPTOR ' || CT.COD_MEDIADOR_RECEPTOR||'-'||CT.SUBCLAVE_RECEPTOR, cReport, io_contador);
    	
		END FOR;
    	CLOSE CURSOR_TRASPASOS;
	 
		
		--ACTUALIZAMOS MEDIADOR CEDENTE
	    UPDATE EXT.CARTERA
	    SET ACTIVO = 0,	   
	    MODIF_USER = v_ModifUser,
	    MODIF_SOURCE = nombreCasoOrigen,
	    MODIF_DATE = CURRENT_TIMESTAMP
	    WHERE COD_MEDIADOR = :v_codMediadorCedente
	    AND COD_SUBCLAVE = :v_subClaveMediadorCedente
	    AND RAMO = cRamo
	    AND ACTIVO = 1
	    AND NUM_AVAL_HOST IN (SELECT COD_AVAL FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId)
	    ;
	    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
	
	END IF;
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO  with SESSION_USER '|| SESSION_USER, cReport, io_contador);

	
END;


-- DO BEGIN 

-- DECLARE vRAMO VARCHAR(50) = 'CAUCION';
-- DECLARE vCASEID BIGINT = 1799;

-- TRUNCATE TABLE EXT.CARTERA;
-- INSERT INTO EXT.CARTERA SELECT * FROM EXT.CARTERA_BKP_04022025 ;
-- DELETE FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_con_derechos_caucion%';

-- SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,FECHA_INICIO,FECHA_FIN FROM EXT.CARTERA WHERE COD_MEDIADOR = '0004' AND RAMO = vRAMO ORDER BY ACTIVO,COD_MEDIADOR,NUM_POLIZA,NUM_AVAL_HOST;

	
-- CALL EXT.sp_traspaso_mediador_mediador_con_derechos_caucion(vCASEID);

-- SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO,FECHA_FIN,MODIF_SOURCE,MODIF_DATE,MODIF_USER 
-- FROM EXT.CARTERA 
-- WHERE (COD_MEDIADOR IN('0004') OR MODIF_USER = 'SMM') AND RAMO = vRAMO ORDER BY ACTIVO,COD_MEDIADOR,NUM_AVAL_HOST,NUM_POLIZA;


-- SELECT * FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_con_derechos_caucion%';

-- END;