CREATE OR REPLACE PROCEDURE EXT.SP_CORREGIR_POLIZAS()
LANGUAGE SQLSCRIPT AS
BEGIN

	DECLARE v_proc_name VARCHAR2(50) := ::CURRENT_OBJECT_NAME;    
    DECLARE v_version VARCHAR2(10) := '0.1';
    DECLARE v_num_rows INTEGER := 0;
    DECLARE v_log_count INTEGER := 0;
    DECLARE v_idproceso INTEGER := 0;
    DECLARE v_idtenant VARCHAR(50) := EXT.LIB_GLOBAL_CESCE:getTenantID();
    DECLARE v_tipoError VARCHAR(120);
    DECLARE v_codMediador VARCHAR(4);
    DECLARE v_codSubclave VARCHAR(4);
    DECLARE v_batchname VARCHAR(250);
    DECLARE v_totalFilas INT;
    DECLARE v_contFilas INT := 0;
    DECLARE v_numPoliza BIGINT;
    DECLARE v_numAnualidad INT;
    DECLARE v_numFianza BIGINT;
    DECLARE v_filename_credito VARCHAR(250) := '1689_MVCAR_TST_20231202_111111_MovCartera.txt';
    DECLARE v_filename_caucion VARCHAR(250) := '1689_MVFID_PRD_20231023_101720_MovFianzas_CARGA_INI.txt';
    DECLARE v_totalFilasCorregidas BIGINT;
    
    ---------------------------------------------------------------------------------------------------------------------------------------
    --CONTROLADOR DE EXCEPCIONES
    ---------------------------------------------------------------------------------------------------------------------------------------
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'SQL ERROR_MESSAGE: ' ||
					IFNULL(::SQL_ERROR_MESSAGE,'') || '. SQL_ERROR_CODE: ' || ::SQL_ERROR_CODE || ' v_numPoliza ' || v_numPoliza || ' v_numFianza ' || v_numFianza || ' v_contFilas ' || v_contFilas, v_proc_name, v_idproceso);
	END;
	---------------------------------------------------------------------------------------------------------------------------------------
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'Version: ' || v_version || ' - Procedure starting...' , v_proc_name, v_idproceso);
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'INICIO PÓLIZAS DE CRÉDITO CON ERROR' , v_proc_name, v_idproceso);
	
	---------------------------------------------------------------------------------------------------------------------------------------
    --POLIZAS DE CREDITO (MVCAR)
    ---------------------------------------------------------------------------------------------------------------------------------------
    
	-- TEST
	SELECT 'CREDITO ANTES',* FROM  EXT.GET_POLIZAS_CREDITO_ERROR_TEST(v_filename_credito);
	SELECT COUNT(*) INTO v_totalFilas FROM EXT.GET_POLIZAS_CREDITO_ERROR_TEST(v_filename_credito);
	
	-- PRD
	-- SELECT * FROM  EXT.GET_POLIZAS_CREDITO_ERROR();
	-- SELECT COUNT(*) INTO v_totalFilas FROM EXT.GET_POLIZAS_CREDITO_ERROR();
	
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'Registros de CREDITO con error: ' || v_totalFilas, v_proc_name, v_idproceso);
	
	-- TEST
	C_MOTIVOS_ERROR_CREDITO = SELECT ROW_NUMBER() OVER () AS NUM_FILA,* FROM EXT.GET_POLIZAS_CREDITO_ERROR_TEST(v_filename_credito);
	
	-- PRD
	-- C_MOTIVOS_ERROR_CREDITO = SELECT ROW_NUMBER() OVER () AS NUM_FILA,* FROM EXT.GET_POLIZAS_CREDITO_ERROR();
	v_totalFilas = RECORD_COUNT(:C_MOTIVOS_ERROR_CREDITO);
	
	FOR v_contFilas IN 1 .. v_totalFilas DO
		SELECT MOTIVO,NUM_POLIZA,ANUALIDAD,SUBSTRING(IDMEDIADOR,1,4),SUBSTRING(IDMEDIADOR,6,4),BATCHNAME 
			INTO v_tipoError,v_numPoliza,v_numAnualidad,v_codMediador,v_codSubclave,v_batchname 
		FROM :C_MOTIVOS_ERROR_CREDITO 
		WHERE NUM_FILA = v_contFilas;
		
		IF v_tipoError = 'MEDIADOR DISTINTO EN CARTERA' THEN
			IF(SELECT COD_MEDIADOR FROM EXT.CARTERA WHERE NUM_POLIZA = v_numPoliza AND NUM_ANUALIDAD = v_numAnualidad AND ACTIVO = 1) = v_codMediador THEN
				
				CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'POLIZA : ' || v_numPoliza || ' ANUALIDAD ' || v_numAnualidad ||' con MEDIADOR ' || v_codMediador || ' SE MODIFICA POR TENER SUBCLAVE DISTINTA' , v_proc_name, v_idproceso);
				
				UPDATE EXT.CARTERA SET COD_SUBCLAVE = v_codSubclave, MODIF_USER = 'MANUAL SP_CORREGIR_POLIZAS', MODIF_DATE = CURRENT_TIMESTAMP
					WHERE NUM_POLIZA = v_numPoliza AND NUM_ANUALIDAD = v_numAnualidad AND COD_MEDIADOR = v_codMediador;
			END IF;
			
		END IF;
	END FOR;
	
	-- TEST
	SELECT COUNT(*) INTO v_totalFilasCorregidas FROM EXT.GET_POLIZAS_CREDITO_ERROR_TEST(v_filename_credito);
	-- PRD
	-- SELECT COUNT(*) INTO v_totalFilasCorregidas FROM EXT.GET_POLIZAS_CREDITO_ERROR();
	
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'Registros corregidos : ' || v_totalFilas - v_totalFilasCorregidas , v_proc_name, v_idproceso);
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'FIN PÓLIZAS DE CRÉDITO CON ERROR' , v_proc_name, v_idproceso);
	
	-- TEST
	SELECT 'CREDITO DESPUES',* FROM  EXT.GET_POLIZAS_CREDITO_ERROR_TEST(v_filename_credito);
	-- PRD
	-- SELECT * FROM  EXT.GET_POLIZAS_CREDITO_ERROR();
	---------------------------------------------------------------------------------------------------------------------------------------
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, '----------------------------------------------' , v_proc_name, v_idproceso);
	
	---------------------------------------------------------------------------------------------------------------------------------------
    --POLIZAS DE CAUCION (MVFID)
    ---------------------------------------------------------------------------------------------------------------------------------------
    
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'INICIO PÓLIZAS DE CAUCIÓN CON ERROR' , v_proc_name, v_idproceso);
	v_totalFilas := 0;
	v_totalFilasCorregidas := 0;
	
	-- TEST
	SELECT 'CAUCION ANTES',* FROM  EXT.GET_POLIZAS_CAUCION_ERROR_TEST(v_filename_caucion);
	SELECT COUNT(*) INTO v_totalFilas FROM EXT.GET_POLIZAS_CAUCION_ERROR_TEST(v_filename_caucion);

	-- PRD
	--SELECT COUNT(*) INTO v_totalFilas FROM EXT.GET_POLIZAS_CAUCION_ERROR();
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'Registros de CAUCION con error: ' || v_totalFilas, v_proc_name, v_idproceso);
	
	-- TEST
	C_MOTIVOS_ERROR_CAUCION = SELECT ROW_NUMBER() OVER () AS NUM_FILA,* FROM EXT.GET_POLIZAS_CAUCION_ERROR_TEST(v_filename_caucion);
	-- PRD
	-- C_MOTIVOS_ERROR_CAUCION = SELECT ROW_NUMBER() OVER () AS NUM_FILA,* FROM EXT.GET_POLIZAS_CAUCION_ERROR();
	
			
	v_totalFilas = RECORD_COUNT(:C_MOTIVOS_ERROR_CAUCION);
	
	
	v_contFilas:= 0;
	
	FOR v_contFilas IN 1 .. v_totalFilas DO
	
		SELECT MOTIVO,NUM_POLIZA,NUM_FIANZA,SUBSTRING(IDMEDIADOR,1,4),SUBSTRING(IDMEDIADOR,6,4),BATCHNAME INTO v_tipoError,v_numPoliza,v_numFianza,v_codMediador,v_codSubclave,v_batchname 
		FROM :C_MOTIVOS_ERROR_CAUCION WHERE NUM_FILA = v_contFilas;
		IF v_tipoError = 'MEDIADOR DISTINTO EN CARTERA' THEN
			
			IF v_codMediador <> '0000' OR v_codSubclave <> '0000'  THEN
			 
				CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'POLIZA : ' || v_numPoliza || ' FIANZA ' || v_numFianza ||' con MEDIADOR ' || v_codMediador || ' SE MODIFICA POR TENER DISTINTO MEDIADOR (DISTINTO ''0000'')' , v_proc_name, v_idproceso);
				UPDATE EXT.CARTERA SET COD_MEDIADOR = v_codMediador, COD_SUBCLAVE = v_codSubclave, MODIF_USER = 'MANUAL SP_CORREGIR_POLIZAS', MODIF_DATE = CURRENT_TIMESTAMP
				WHERE NUM_POLIZA = v_numPoliza AND NUM_FIANZA = v_numFianza AND ACTIVO = 1;
			END IF;
			
		END IF;
	END FOR;
	
	-- TEST
	SELECT COUNT(*) INTO v_totalFilasCorregidas FROM EXT.GET_POLIZAS_CAUCION_ERROR_TEST(v_filename_caucion);
	-- PRD
	-- SELECT COUNT(*) INTO v_totalFilasCorregidas FROM EXT.GET_POLIZAS_CAUCION_ERROR();
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'Registros corregidos : ' || v_totalFilas - v_totalFilasCorregidas , v_proc_name, v_idproceso);
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'FIN PÓLIZAS DE CAUCIÓN CON ERROR' , v_proc_name, v_idproceso);
	---------------------------------------------------------------------------------------------------------------------------------------
	-- TEST
	SELECT 'CAUCION DESPUES',* FROM  EXT.GET_POLIZAS_CAUCION_ERROR_TEST(v_filename_caucion);
	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (v_idtenant, 'Fin del proceso.', v_proc_name, v_idproceso);

END;

DO BEGIN

TRUNCATE TABLE EXT.CARTERA;
INSERT INTO EXT.CARTERA SELECT * FROM EXT.CARTERA_BKP_SMM;
DELETE FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%SP_CORREGIR_POLIZAS%';

CALL EXT.SP_CORREGIR_POLIZAS();

SELECT * FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%SP_CORREGIR_POLIZAS%';


END;