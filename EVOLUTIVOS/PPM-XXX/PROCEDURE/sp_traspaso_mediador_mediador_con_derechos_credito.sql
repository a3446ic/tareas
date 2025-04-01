CREATE OR REPLACE PROCEDURE EXT.sp_traspaso_mediador_mediador_con_derechos_credito (IN caseId BIGINT, IN nombreCasoOrigen VARCHAR(100))
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
	DECLARE v_TipoMovimiento INT;
	DECLARE v_DescTipoMovimiento NVARCHAR(50);
    DECLARE v_ModifUser NVARCHAR(50);
    -- CONSTANTES
    DECLARE cReport CONSTANT VARCHAR(50) := 'sp_traspaso_mediador_mediador_con_derechos_credito';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    DECLARE cRamo CONSTANT VARCHAR(10) := 'CREDITO';
    DECLARE cDerechosObligaciones NVARCHAR(50) := 'CON DERECHOS Y OBLIGACIONES';
    
    -------------------------------------------------------------------------------------------
    ------------------------- DECLARACION DE CURSOR    ----------------------------------------
    -------------------------------------------------------------------------------------------    
    DECLARE CURSOR CURSOR_TRASPASOS FOR
	SELECT DISTINCT NUM_POLIZA,COD_MEDIADOR_RECEPTOR,SUBCLAVE_RECEPTOR,INTERMEDIACION_RECEPTOR,FECHA_EFECTO_SOLICITUD,COD_MEDIADOR_CEDENTE,SUBCLAVE_CEDENTE
	FROM EXT.SOLICITUD_TRASPASO 
	WHERE CASEID = :caseId AND RAMO = cRamo AND NUM_POLIZA IS NOT NULL;
	
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
        , 'MANUAL - CASEID: ' || :caseId
		INTO v_tipoTraspaso,v_codMediadorCedente,v_subClaveMediadorCedente,v_fechaTraspaso,v_TipoMovimiento, v_ModifUser  
	FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND RAMO = cRamo AND NUM_POLIZA IS NOT NULL;
	
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
    
   
    -------------------------------------------------------------------------------------------
    ------------------ COMPROBAR SI ES TRASPASO TOTAL 'N' O PARCIAL	'P' -----------------------
    -------------------------------------------------------------------------------------------
    IF ((SELECT COUNT(*) FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseID AND NUM_POLIZA IS NOT NULL) = (SELECT COUNT(*) FROM EXT.CARTERA WHERE COD_MEDIADOR = :v_codMediadorCedente AND COD_SUBCLAVE = v_subClaveMediadorCedente AND RAMO = cRAMO AND FECHA_VENCIMIENTO >= v_fechaTraspaso)) THEN
    	v_tipoTraspaso:= 'TOTAL';
    ELSE
    	v_tipoTraspaso:= 'PARCIAL';
    END IF;
    
   
	 IF v_tipoTraspaso = 'TOTAL' THEN
		v_modifSource:= 'TRASPASO TOTAL '||:v_DescTipoMovimiento|| ' ' || cDerechosObligaciones || ' - CASEID: ' || :caseId;
	ELSE
		v_modifSource:= 'TRASPASO PARCIAL '||:v_DescTipoMovimiento|| ' ' || cDerechosObligaciones || ' - CASEID: ' || :caseId;
	END IF;
	

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'TRASPASO ' ||v_tipoTraspaso|| ' ' ||:v_DescTipoMovimiento || ' '  || :cDerechosObligaciones  || ' MEDIADOR CEDENTE: '|| :v_codMediadorCedente ||'-'||:v_subClaveMediadorCedente , CReport, io_contador);
	
    -------------------------------------------------------------------------------------------
    ------------------------------- INSERTAR PÓLIZA TRASPASO ----------------------------------
    -------------------------------------------------------------------------------------------
    -- ABRIR CURSOR
    OPEN CURSOR_TRASPASOS;
    FOR CT AS CURSOR_TRASPASOS DO
		
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
        CT.FECHA_EFECTO_SOLICITUD,
        '2200-01-01',
        CASE WHEN
        	CRT.NUM_ANUALIDAD IS NOT NULL THEN C.P_ESPECIAL_EMISION
        END, --P_ESPECIAL_EMISION
        CASE 
        	--ANUALIDAD VIGENTE TIENE P_ESPECIAL_RENOVACION
        	WHEN CRT.NUM_ANUALIDAD IS NOT NULL AND C.P_ESPECIAL_RENOVACION IS NOT NULL THEN C.P_ESPECIAL_RENOVACION
        	--ANUALIDAD VIGENTE TIENE P_ESPECIAL_EMISION Y NO TIENE P_ESPECIAL_RENOVACION
        	WHEN CRT.NUM_ANUALIDAD IS NOT NULL AND C.P_ESPECIAL_EMISION IS NOT NULL AND (C.P_ESPECIAL_RENOVACION IS NULL OR C.P_ESPECIAL_RENOVACION = 0) THEN PP.P_RENOV_FIN 
        END, --P_ESPECIAL_RENOVACION
        C."NIF_TOMADOR",
        C."NOMBRE_TOMADOR",
        C."FECHA_EFECTO_TRASPASO",
        C."MEDIADOR_PRINCIPAL_CIC",
        C."ACTIVO",
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        v_ModifUser,
		nombreCasoOrigen,
        CASE WHEN
        	CRT.NUM_ANUALIDAD IS NOT NULL AND ((C.P_ESPECIAL_EMISION IS NOT NULL AND C.P_ESPECIAL_EMISION <> 0)  OR (C.P_ESPECIAL_RENOVACION IS NOT NULL AND C.P_ESPECIAL_RENOVACION <> 0)) THEN
        		CT.FECHA_EFECTO_SOLICITUD
        END, -- FECHA_INICIO_OPESP
        CASE WHEN
        	CRT.NUM_ANUALIDAD IS NOT NULL AND ((C.P_ESPECIAL_EMISION IS NOT NULL AND C.P_ESPECIAL_EMISION <> 0)  OR (C.P_ESPECIAL_RENOVACION IS NOT NULL AND C.P_ESPECIAL_RENOVACION <> 0)) THEN
        		C.FECHA_VENCIMIENTO
        END -- FECHA_FIN_OPESP
        FROM EXT.CARTERA C 
        -- JOIN ANUALIDAD ACTUAL
        LEFT JOIN (SELECT *
						,ROW_NUMBER() OVER (PARTITION BY CR.NUM_POLIZA, CR.COD_MEDIADOR, CR.COD_SUBCLAVE ORDER BY CR.NUM_ANUALIDAD DESC) AS RN
					FROM EXT.CARTERA CR 
					WHERE CR.COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE AND CR.COD_SUBCLAVE = CT.SUBCLAVE_CEDENTE AND CR.NUM_POLIZA = CT.NUM_POLIZA 
						AND CR.RAMO = cRamo AND CR.ACTIVO = 1 AND CR.NUM_POLIZA = CT.NUM_POLIZA
        	        ) CRT ON C.NUM_POLIZA = CRT.NUM_POLIZA AND C.COD_MEDIADOR = CRT.COD_MEDIADOR AND C.COD_SUBCLAVE = CRT.COD_SUBCLAVE 
        	        	AND C.NUM_ANUALIDAD = CRT.NUM_ANUALIDAD AND CRT.RN = 1
		-- JOIN PLANES DE COMISIONAMIENTO
		LEFT JOIN (
	        SELECT 
	            CART.NUM_POLIZA,
	            CART.COD_MEDIADOR,
	            CART.COD_SUBCLAVE,
	            CART.IDMODALIDAD,
	            ROW_NUMBER() OVER (
	                PARTITION BY CART.NUM_POLIZA, CART.COD_MEDIADOR, CART.COD_SUBCLAVE, CART.IDMODALIDAD
	                ORDER BY CART.NUM_ANUALIDAD DESC, CART.ACTIVO DESC
	            ) AS RN,
	            CASE 
	                WHEN CART.P_ESPECIAL_EMISION IS NULL OR CART.P_ESPECIAL_EMISION = 0 
	                THEN COALESCE(PC.P_EMISION, PV.P_EMISION_DEF)
	                ELSE CART.P_ESPECIAL_EMISION 
	            END AS P_EMISION_FIN,
	            CASE 
	                WHEN CART.P_ESPECIAL_RENOVACION IS NULL OR CART.P_ESPECIAL_RENOVACION = 0 
	                THEN COALESCE(PC.P_RENOVACION, PV.P_RENOVACION_DEF)
	                ELSE CART.P_ESPECIAL_RENOVACION 
	            END AS P_RENOV_FIN
	        FROM EXT.CARTERA CART
	        LEFT JOIN EXT.PLAN_COMISIONAMIENTO PC 
	            ON CART.COD_MEDIADOR || '-' || CART.COD_SUBCLAVE = PC.POSITIONNAME 
	            AND CART.IDPRODUCT = PC.IDPRODUCT 
	            AND CART.FECHA_EFECTO >= PC.EFFECTIVESTARTDATE 
	            AND CART.FECHA_EFECTO < PC.EFFECTIVEENDDATE
	        LEFT JOIN EXT.PRODUCTOS_VW PV 
	            ON CART.IDPRODUCT = PV.IDPRODUCT AND CART.IDMODALIDAD = PV.MODALIDAD
	    ) PP ON PP.NUM_POLIZA = C.NUM_POLIZA AND PP.COD_MEDIADOR = C.COD_MEDIADOR
				AND PP.COD_SUBCLAVE = C.COD_SUBCLAVE AND PP.RN = 1
        WHERE C.COD_MEDIADOR = CT.COD_MEDIADOR_CEDENTE
        AND C.COD_SUBCLAVE = CT.SUBCLAVE_CEDENTE
        AND C.RAMO = cRamo
        AND C.ACTIVO > 0
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
	    MODIF_USER = v_ModifUser,
	    MODIF_SOURCE = nombreCasoOrigen,
	    MODIF_DATE = CURRENT_TIMESTAMP
    WHERE COD_MEDIADOR = :v_codMediadorCedente
    AND COD_SUBCLAVE = :v_subClaveMediadorCedente
    AND RAMO = cRamo
    AND ACTIVO > 0
    AND NUM_POLIZA IN (SELECT DISTINCT NUM_POLIZA FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = :caseId AND NUM_POLIZA IS NOT NULL)
    ;
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
	
    -- FIN
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);

END;


DO BEGIN 

DECLARE vRAMO VARCHAR(50) = 'CREDITO';
DECLARE vCASEID BIGINT = 1880;

TRUNCATE TABLE EXT.CARTERA;
INSERT INTO EXT.CARTERA SELECT * FROM EXT.CARTERA_BKP_04022025 ;
DELETE FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_con_derechos_credito%' AND DATETIME > '2025-03-07 10:00:00';

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,FECHA_INICIO,FECHA_FIN FROM EXT.CARTERA WHERE COD_MEDIADOR = '0044' AND COD_SUBCLAVE = '0003' AND RAMO = vRAMO ORDER BY ACTIVO,COD_MEDIADOR,NUM_POLIZA,NUM_AVAL_HOST;

	
CALL EXT.SP_TRASPASO_MEDIADOR_MEDIADOR_CON_DERECHOS_CREDITO(1880,'Traspaso Pólizas y Avales-GM-335');

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO,FECHA_FIN,MODIF_SOURCE,MODIF_DATE,MODIF_USER 
FROM EXT.CARTERA 
WHERE ( MODIF_USER LIKE '%1880%') AND RAMO = vRAMO ORDER BY ACTIVO,COD_MEDIADOR,NUM_AVAL_HOST,NUM_POLIZA;

SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,FECHA_INICIO,FECHA_FIN,MODIF_USER FROM EXT.CARTERA WHERE COD_MEDIADOR = '0044' AND COD_SUBCLAVE = '0003' AND RAMO = vRAMO ORDER BY ACTIVO,COD_MEDIADOR,NUM_POLIZA,NUM_AVAL_HOST;

-- SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO,FECHA_FIN,MODIF_SOURCE,MODIF_DATE,MODIF_USER 
-- FROM EXT.CARTERA 
-- WHERE ((COD_MEDIADOR IN('3071') ) OR MODIF_SOURCE = 'Traspaso Pólizas y Avales-GM-335') AND RAMO = vRAMO ORDER BY ACTIVO,COD_MEDIADOR,NUM_AVAL_HOST,NUM_POLIZA;



SELECT * FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_con_derechos_credito%' AND DATETIME > '2025-03-07 10:00:00';

END;