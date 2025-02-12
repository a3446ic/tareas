CREATE OR REPLACE PROCEDURE EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito (IN p_json NVARCHAR(5000))
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
    DECLARE CURSOR CURSOR_RECEPTOR FOR
    SELECT 
        C.IDCASE,
        C.CODIGOMEDIADORCEDENTE,
        C.SUBCLAVEMEDIADORCEDENTE,
        C.FECHATRASPASO,
        P.NUM_POLIZA_CEDENTE,
        P.PORCENTAJE_INTERMEDIACION_CEDENTE,
        R.CODIGOMEDIADORRECEPTOR,
        R.SUBCLAVEMEDIADORRECEPTOR,
        --PR.NUM_POLIZA_RECEPTOR,
        R.PORCENTAJE_INTERMEDIACION_TRASPASO,
        PR.PORCENTAJE_INTERMEDIACION_RECEPTOR
    FROM 
        JSON_TABLE(:p_json, '$' 
            COLUMNS (
                idCase BIGINT PATH '$.caseId',
                codigoMediadorCedente NVARCHAR(10) PATH '$.codigoMediadorCedente',
                subClaveMediadorCedente NVARCHAR(10) PATH '$.subClaveMediadorCedente',
                fechaTraspaso DATE PATH '$.fechaTraspaso',
                tipoTraspaso NVARCHAR(10) PATH '$.tipoTraspaso'
            )
        ) AS C
    LEFT JOIN 
        JSON_TABLE(:p_json, '$.polizas[*]' 
            COLUMNS (
                num_poliza_cedente NVARCHAR(20) PATH '$.num_poliza',
                porcentaje_intermediacion_cedente NVARCHAR(10) PATH '$.porcentaje_intermediacion'
            )
        ) AS P 
        ON 1=1
    LEFT JOIN 
        JSON_TABLE(:p_json, '$.receptor[*]' 
            COLUMNS (
                codigoMediadorReceptor NVARCHAR(10) PATH '$.codigoMediadorReceptor',
                subClaveMediadorReceptor NVARCHAR(10) PATH '$.subClaveMediadorReceptor',
                porcentaje_intermediacion_traspaso NVARCHAR(10) PATH '$.porcentajeTraspaso',
                receptorIndex FOR ORDINALITY  -- Índice del receptor
            )
        ) AS R 
        ON 1=1
    LEFT JOIN 
        JSON_TABLE(:p_json, '$.receptor[*].polizasReceptor[*]' 
            COLUMNS (
                receptorIndex FOR ORDINALITY,  -- Índice del receptor al que pertenece esta póliza
                num_poliza_receptor NVARCHAR(20) PATH '$.num_poliza',
                porcentaje_intermediacion_receptor NVARCHAR(10) PATH '$.porcentaje_intermediacion'
                
            )
        ) AS PR 
        ON R.receptorIndex = PR.receptorIndex  
    --     AND P.NUM_POLIZA_CEDENTE = PR.NUM_POLIZA_RECEPTOR;
    ;
    

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


    -- EXTRACCIÓN DE VALORES DEL JSON
    SELECT JSON_VALUE(:p_json, '$.caseId') INTO v_caseId FROM DUMMY;
    SELECT JSON_VALUE(:p_json, '$.tipoTraspaso') INTO v_tipoTraspaso FROM DUMMY;
    SELECT JSON_VALUE(:p_json, '$.fechaTraspaso') INTO v_fechaTraspaso FROM DUMMY;
    SELECT JSON_VALUE(:p_json, '$.tipoMovimiento') INTO cTipoMovimiento FROM DUMMY;
    SELECT JSON_VALUE(:p_json, '$.codigoMediadorCedente') INTO v_codMediadorCedente FROM DUMMY;
    SELECT JSON_VALUE(:p_json, '$.subClaveMediadorCedente') INTO v_subClaveMediadorCedente FROM DUMMY;
    
    -- SELECT v_caseId,v_fechaTraspaso,v_codMediadorCedente,v_subClaveMediadorCedente,v_tipoTraspaso FROM DUMMY;
    
    -- COMPROBAR SI EXISTE LA TABLA TEMPORAL
    IF (SELECT COUNT(*) FROM SYS.TABLES WHERE SCHEMA_NAME = 'EXT' AND TABLE_NAME = 'TRASPASOS_TEMP') = 0 THEN
   
        CREATE COLUMN TABLE EXT.TRASPASOS_TEMP(
            IDCASE BIGINT,
            TIPO_MOVIMIENTO NVARCHAR(250),
            TIPO_TRASPASO NVARCHAR(50),
            RAMO NVARCHAR(10),
            TIPO_TRASPASO_CAUCION NVARCHAR(50),
            DERECHOS_OBLIGACIONES NVARCHAR(50),
            CODIGOMEDIADORCEDENTE NVARCHAR(4),
            SUBCLAVEMEDIADORCEDENTE NVARCHAR(4),
            FECHATRASPASO DATE,
            NUM_POLIZA_CEDENTE NVARCHAR(40),
            NUM_AVAL_CEDENTE NVARCHAR(10),
            PORCENTAJE_INTERMEDIACION_CEDENTE BIGINT,
            CODIGOMEDIADORRECEPTOR NVARCHAR(4),
            SUBCLAVEMEDIADORRECEPTOR NVARCHAR(4),
            NUM_POLIZA_RECEPTOR NVARCHAR(40),
            NUM_AVAL_RECEPTOR NVARCHAR(20),
            PORCENTAJE_INTERMEDIACION_RECEPTOR  BIGINT,
            MODIF_USER NVARCHAR(20),
            MODIF_DATE DATETIME
        ) UNLOAD PRIORITY 5 AUTO MERGE;

        CALL EXT.LIB_GLOBAL_CESCE :w_debug (
            i_Tenant,
            'CREADA TABLA ' || cEsquema || '.' || 'TRASPASOS_TEMP',
            'cReport',
            io_contador
        ); 
    ELSE
    	-- ELIMINAR REGISTROS PREVIOS DE LA TABLA TEMPORAL
    	DELETE FROM EXT.TRASPASOS_TEMP WHERE IDCASE = v_caseId;
    END IF;

    -- INSERTAR DATOS EN LA TABLA TEMPORAL DESDE EL JSON
    INSERT INTO EXT.TRASPASOS_TEMP(IDCASE,TIPO_MOVIMIENTO,TIPO_TRASPASO,RAMO,TIPO_TRASPASO_CAUCION,DERECHOS_OBLIGACIONES,
    	CODIGOMEDIADORCEDENTE,SUBCLAVEMEDIADORCEDENTE,FECHATRASPASO,NUM_POLIZA_CEDENTE,NUM_AVAL_CEDENTE,PORCENTAJE_INTERMEDIACION_CEDENTE,
    	CODIGOMEDIADORRECEPTOR,SUBCLAVEMEDIADORRECEPTOR,NUM_POLIZA_RECEPTOR,NUM_AVAL_RECEPTOR,PORCENTAJE_INTERMEDIACION_RECEPTOR,
    	MODIF_USER,MODIF_DATE
    	
    )
    SELECT 
        C.IDCASE,
        UPPER(cTipoMovimiento),
        UPPER(v_tipoTraspaso),
        cRamo,
        UPPER(v_tipoTraspasoCaucion),
        cDerechosObligaciones,
        C.CODIGOMEDIADORCEDENTE,
        C.SUBCLAVEMEDIADORCEDENTE,
        C.FECHATRASPASO,
        P.NUM_POLIZA_CEDENTE,
        P.NUM_AVAL_CEDENTE,
        P.PORCENTAJE_INTERMEDIACION_CEDENTE,
        R.CODIGOMEDIADORRECEPTOR,
        R.SUBCLAVEMEDIADORRECEPTOR,
    	PR.NUM_POLIZA_RECEPTOR,
    	PR.NUM_AVAL_RECEPTOR,
        PR.PORCENTAJE_INTERMEDIACION_RECEPTOR,
        SESSION_USER,
        CURRENT_TIMESTAMP
    FROM 
        JSON_TABLE(:p_json, '$' 
            COLUMNS (
                idCase BIGINT PATH '$.caseId',
                codigoMediadorCedente NVARCHAR(10) PATH '$.codigoMediadorCedente',
                subClaveMediadorCedente NVARCHAR(10) PATH '$.subClaveMediadorCedente',
                fechaTraspaso DATE PATH '$.fechaTraspaso'
            )
        ) AS C
    LEFT JOIN 
        JSON_TABLE(:p_json, '$.polizas[*]' 
            COLUMNS (
                num_poliza_cedente NVARCHAR(20) PATH '$.num_poliza',
                num_aval_cedente NVARCHAR(20) PATH '$.num_aval',
                porcentaje_intermediacion_cedente NVARCHAR(10) PATH '$.porcentaje_intermediacion'
            )
        ) AS P 
        ON 1=1
    LEFT JOIN 
        JSON_TABLE(:p_json, '$.receptor[*]' 
            COLUMNS (
                codigoMediadorReceptor NVARCHAR(10) PATH '$.codigoMediadorReceptor',
                subClaveMediadorReceptor NVARCHAR(10) PATH '$.subClaveMediadorReceptor',
                porcentaje_intermediacion_traspaso NVARCHAR(10) PATH '$.porcentajeTraspaso',
                receptorIndex FOR ORDINALITY  -- Índice del receptor
            )
        ) AS R 
        ON 1=1
    LEFT JOIN 
        JSON_TABLE(:p_json, '$.receptor[*].polizasReceptor[*]' 
            COLUMNS (
                receptorIndex FOR ORDINALITY,  -- Índice del receptor al que pertenece esta póliza
                num_poliza_receptor NVARCHAR(20) PATH '$.num_poliza',
                num_aval_receptor NVARCHAR(20) PATH '$.num_aval',
            	porcentaje_intermediacion_receptor NVARCHAR(10) PATH '$.porcentaje_intermediacion'
            )
        ) AS PR 
        ON R.receptorIndex = PR.receptorIndex  
        -- AND P.NUM_POLIZA_CEDENTE = PR.NUM_POLIZA_RECEPTOR
    ;
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. TABLA TRASPASOS_TEMP', cReport, io_contador);

	IF v_tipoTraspaso = 'total' THEN
		v_modifSource:= 'TRASPASO TOTAL MEDIADOR MEDIADOR SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN ' || v_caseId;
	ELSE
		v_modifSource:= 'TRASPASO PARCIAL MEDIADOR MEDIADOR SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN' || v_caseId;
	END IF;
	
	
	-- ABRIR CURSOR
     OPEN CURSOR_RECEPTOR;

     FOR CR AS CURSOR_RECEPTOR
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
    WHERE COD_MEDIADOR = CR.CODIGOMEDIADORCEDENTE
    AND RAMO = 'CREDITO'
	AND ACTIVO = 1
	AND NUM_POLIZA = CR.NUM_POLIZA_CEDENTE
  --  AND (
		-- -- SI EXISTEN VALORES EN NUM_POLIZAS SE TRASPASAN ESAS PÓLIZAS
  --  	EXISTS (SELECT 1 FROM #TEMPNUMPOLIZAS) 
  --  			AND NUM_POLIZA IN (SELECT NUM_POLIZA FROM #TEMPNUMPOLIZAS)
  --  	-- SI NO EXISTEN VALORES EN NUM_POLIZAS SE TRASPASAN TODAS LAS PÓLIZAS
  --  	OR NOT EXISTS (SELECT 1 FROM #TEMPNUMPOLIZAS)
  --  ) 
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
		CR.CODIGOMEDIADORRECEPTOR,
		CR.SUBCLAVEMEDIADORRECEPTOR,
		COALESCE(CR.PORCENTAJE_INTERMEDIACION_RECEPTOR,COALESCE(CR.PORCENTAJE_INTERMEDIACION_TRASPASO,P_INTERMEDIACION)),
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
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. PÓLIZA ' || CR.NUM_POLIZA_CEDENTE || ' - MEDIADOR RECEPTOR ' || CR.CODIGOMEDIADORRECEPTOR||'-'||CR.SUBCLAVEMEDIADORRECEPTOR, cReport, io_contador);
	
  END FOR; 
  CLOSE CURSOR_RECEPTOR;
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
	AND (
    	-- SI EXISTEN VALORES EN NUM_POLIZAS SE TRASPASAN ESAS PÓLIZAS
    	EXISTS (SELECT 1 FROM JSON_TABLE(:p_json, '$.polizas[*]' 
        COLUMNS (
            num_poliza NVARCHAR(20) PATH '$.num_poliza',
            porcentaje_emision NVARCHAR(10) PATH '$.porcentaje_emision'
        )
    )) 
    			AND NUM_POLIZA IN (SELECT NUM_POLIZA FROM JSON_TABLE(:p_json, '$.polizas[*]' 
        COLUMNS (
            num_poliza NVARCHAR(20) PATH '$.num_poliza',
            porcentaje_emision NVARCHAR(10) PATH '$.porcentaje_emision'
        )
    ))
    	-- SI NO EXISTEN VALORES EN NUM_POLIZAS SE TRASPASAN TODAS LAS PÓLIZAS
    	OR NOT EXISTS (SELECT 1 FROM JSON_TABLE(:p_json, '$.polizas[*]' 
        COLUMNS (
            num_poliza NVARCHAR(20) PATH '$.num_poliza',
            porcentaje_emision NVARCHAR(10) PATH '$.porcentaje_emision'
        )
    ))
    );
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
	
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);

	-- AND (
	-- 	-- SI EXISTEN VALORES EN NUM_POLIZAS SE TRASPASAN ESAS PÓLIZAS
 --   	EXISTS (SELECT 1 FROM #TEMPNUMPOLIZAS) 
 --   			AND NUM_POLIZA IN (SELECT NUM_POLIZA FROM #TEMPNUMPOLIZAS)
 --   	-- SI NO EXISTEN VALORES EN NUM_POLIZAS SE TRASPASAN TODAS LAS PÓLIZAS
 --   	OR NOT EXISTS (SELECT 1 FROM #TEMPNUMPOLIZAS)
 --   );

	--BORRAMOS TABLA TEMPORAL
    --DROP TABLE #TEMPNUMPOLIZAS;

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