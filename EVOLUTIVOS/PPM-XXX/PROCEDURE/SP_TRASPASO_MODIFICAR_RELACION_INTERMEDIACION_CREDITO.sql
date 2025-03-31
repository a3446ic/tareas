CREATE PROCEDURE EXT.SP_TRASPASO_MODIFICAR_RELACION_INTERMEDIACION_CREDITO (IN caseId BIGINT, IN nombreCasoOrigen VARCHAR(100))
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
	DECLARE v_ModifUser NVARCHAR(50);
    -- CONSTANTES
    DECLARE cReport CONSTANT VARCHAR(250) := 'SP_TRASPASO_MODIFICAR_RELACION_INTERMEDIACION_CREDITO';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    DECLARE cRamo CONSTANT VARCHAR(10) := 'CREDITO';
    DECLARE cDerechosObligaciones NVARCHAR(50) := 'SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN ';
    
    -------------------------------------------------------------------------------------------
    ------------------------- DECLARACION DE CURSOR    ----------------------------------------
    -------------------------------------------------------------------------------------------
    
    DECLARE CURSOR CURSOR_TRASPASOS FOR
	SELECT DISTINCT NUM_POLIZA,COD_MEDIADOR_RECEPTOR,SUBCLAVE_RECEPTOR,INTERMEDIACION_RECEPTOR,FECHA_EFECTO_SOLICITUD,COD_MEDIADOR_CEDENTE,SUBCLAVE_CEDENTE
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
		, COD_MEDIADOR_CEDENTE
		, SUBCLAVE_CEDENTE
		, FECHA_EFECTO_SOLICITUD
		, TIPO_MOVIMIENTO
		, 'MANUAL - CASEID: ' || :caseId
		INTO v_tipoTraspaso,v_codMediadorCedente,v_subClaveMediadorCedente,v_fechaTraspaso,v_TipoMovimiento, v_ModifUser 
	FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND RAMO = cRamo;
    
	-- TIPO MOVIMIENTO
    SELECT CASE 
    	WHEN v_TipoMovimiento = 1 THEN 'SIN MEDIADOR > MEDIADOR'
    	WHEN v_TipoMovimiento = 2 THEN 'MEDIADOR > MEDIADOR'
    	WHEN v_TipoMovimiento = 3 THEN 'TRASPASO %'
    	WHEN v_TipoMovimiento = 4 THEN 'ERROR CAPTURA'
    	WHEN v_TipoMovimiento = 5 THEN 'MEDIADOR > CANAL DIRECTO'
        WHEN v_TipoMovimiento = 6 THEN 'OPERACIONES ESPECIALES'
        WHEN v_TipoMovimiento = 7 THEN 'MODIFICAR RELACIÓN INTERMEDIACIÓN'
        WHEN v_TipoMovimiento = 8 THEN 'ENTRE SUBCLAVES'
    	END
    INTO v_DescTipoMovimiento
    FROM DUMMY;
    
    -------------------------------------------------------------------------------------------
    ------------------ COMPROBAR SI ES TRASPASO TOTAL 'N' O PARCIAL	'P' -----------------------
    -------------------------------------------------------------------------------------------
    IF ((SELECT COUNT(*) FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseID AND RAMO = cRamo) = (SELECT COUNT(*) FROM EXT.CARTERA WHERE COD_MEDIADOR = :v_codMediadorCedente AND COD_SUBCLAVE = v_subClaveMediadorCedente AND RAMO = cRAMO AND FECHA_VENCIMIENTO >= v_fechaTraspaso)) THEN
    	v_tipoTraspaso:= 'TOTAL';
    ELSE
    	v_tipoTraspaso:= 'PARCIAL';
    END IF;
    
   
	 IF v_tipoTraspaso = 'TOTAL' THEN
		v_modifSource:= 'TRASPASO TOTAL '||:v_DescTipoMovimiento|| ' ' || cDerechosObligaciones || ' - CASEID: ' || :caseId;
	ELSE
		v_modifSource:= 'TRASPASO PARCIAL '||:v_DescTipoMovimiento|| ' ' || cDerechosObligaciones || ' - CASEID: ' || :caseId;
	END IF;
	

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASE ID: ' ||:caseID|| ' - TRASPASO ' ||v_tipoTraspaso|| ' ' ||:v_DescTipoMovimiento || ' '  || :cDerechosObligaciones  || ' MEDIADOR CEDENTE: '|| :v_codMediadorCedente ||'-'||:v_subClaveMediadorCedente , CReport, io_contador);
	
    
    -------------------------------------------------------------------------------------------
    ------------------------------- INSERTAR PÓLIZA TRASPASO -----------------------------------------
    -------------------------------------------------------------------------------------------
    -- ABRIR CURSOR
    
     OPEN CURSOR_TRASPASOS;
     FOR CT AS CURSOR_TRASPASOS DO
     
 		UPDATE C
		SET FECHA_INICIO = (SELECT FECHA_INICIO FROM EXT.CARTERA WHERE COD_MEDIADOR = v_codMediadorCedente AND COD_SUBCLAVE = v_subClaveMediadorCedente
								AND RAMO = cRamo AND ACTIVO = 2 AND NUM_POLIZA = C.NUM_POLIZA
		)
		, FECHA_FIN = '2200-01-01'
		, MODIF_USER = v_ModifUser
		, MODIF_SOURCE = nombreCasoOrigen
		, MODIF_DATE = CURRENT_TIMESTAMP
		FROM EXT.CARTERA C
		WHERE C.COD_MEDIADOR = CT.COD_MEDIADOR_RECEPTOR
			AND C.COD_SUBCLAVE = CT.SUBCLAVE_RECEPTOR
		AND RAMO = cRamo
	--	AND ACTIVO = 1
		AND NUM_POLIZA IN (SELECT DISTINCT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND RAMO = cRamo);
    
 
		-- INSERTAMOS PÓLIZAS MEDIADOR RECEPTOR
		UPDATE EXT.CARTERA
		SET ACTIVO = 0
		WHERE COD_MEDIADOR = v_codMediadorCedente
		AND COD_SUBCLAVE = v_subClaveMediadorCedente
		AND NUM_POLIZA IN (SELECT DISTINCT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND RAMO = cRamo);
		
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. PÓLIZA ' || CT.NUM_POLIZA || ' - MEDIADOR RECEPTOR ' || CT.COD_MEDIADOR_RECEPTOR||'-'||CT.SUBCLAVE_RECEPTOR, cReport, io_contador);
	
  	END FOR; 
  	CLOSE CURSOR_TRASPASOS;

    -------------------------------------------------------------------------------------------
    ------------------------------- ACTUALIZAR MEDIADOR RECEPTOR-------------------------------
    -------------------------------------------------------------------------------------------
	
	-- UPDATE C
	-- SET FECHA_INICIO = ()
	-- , FECHA_FIN = (SELECT FECHA_INICIO
	-- 					FROM (
	-- 						SELECT NUM_POLIZA,NUM_ANUALIDAD,COD_MEDIADOR,FECHA_VENCIMIENTO
	-- 							,ROW_NUMBER() OVER (PARTITION BY NUM_POLIZA, COD_MEDIADOR, COD_SUBCLAVE ORDER BY NUM_ANUALIDAD DESC) AS RN
	-- 						FROM EXT.CARTERA WHERE COD_MEDIADOR = C.COD_MEDIADOR AND COD_SUBCLAVE = C.COD_SUBCLAVE AND NUM_POLIZA = C.NUM_POLIZA AND RAMO = cRamo AND ACTIVO = 1  
	-- 					) CT WHERE RN = 1
	-- 				)
	-- 	, MODIF_USER = v_ModifUser
	-- 	, MODIF_SOURCE = nombreCasoOrigen
	-- 	, MODIF_DATE = CURRENT_TIMESTAMP
	-- FROM EXT.CARTERA C
	-- WHERE C.COD_MEDIADOR = v_codMediadorCedente
	-- AND RAMO = 'CREDITO'
	-- AND ACTIVO = 1
	-- AND NUM_POLIZA IN (SELECT DISTINCT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND RAMO = cRamo);
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
	
	-- FIN
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);

	

END