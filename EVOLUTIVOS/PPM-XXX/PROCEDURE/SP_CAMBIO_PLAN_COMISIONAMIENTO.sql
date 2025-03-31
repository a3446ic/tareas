CREATE OR REPLACE PROCEDURE EXT.SP_CAMBIO_PLAN_COMISIONAMIENTO (IN v_COD_MEDIADOR NVARCHAR(10), IN v_COD_SUBCLAVE NVARCHAR(10), IN v_FECHA_TRASPASO DATE)
LANGUAGE SQLSCRIPT 
AS
/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 31/03/2025
	|---------------------------------------------------------------------------------------------- 
	| Procedure Purpose: AL CAMBIAR UN PLAN DE COMISIONAMIENTO DEL PRODUCTO 431SP, TODAS LAS PÓLIZAS DEL MEDIADOR
	|                    TIENEN QUE ACTUALIZAR SU FECHA DE INTERMEDIACIÓN
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
    DECLARE v_modifSource NVARCHAR(250);
   
    DECLARE v_ModifUser NVARCHAR(50) := 'MANUAL PLAN COMISIONAMIENTO';
    DECLARE v_num_rows BIGINT;
   
    -- CONSTANTES
    DECLARE cReport CONSTANT VARCHAR(50) := ::CURRENT_OBJECT_NAME;
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := ::CURRENT_OBJECT_SCHEMA;
    DECLARE cIDPRODUCT NVARCHAR(127) := '341SP';
    
    -------------------------------------------------------------------------------------------
    ------------------------------- HANDLER EXCEPTION -----------------------------------------
    -------------------------------------------------------------------------------------------
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'SQL ERROR_MESSAGE: ' ||
			IFNULL(::SQL_ERROR_MESSAGE,'') || '. SQL_ERROR_CODE: ' || ::SQL_ERROR_CODE, cReport, io_contador);
	END;
    
	
     
     
    -------------------------------------------------------------------------------------------
    ------------------------------- OBTENER VARIABLES -----------------------------------------
    -------------------------------------------------------------------------------------------
    
    -- TENANT
    SELECT EXT.LIB_GLOBAL_CESCE:getTenantID() INTO i_Tenant FROM DUMMY;

    -- INICIO
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INICIO PROCEDIMIENTO v' || cVersion || ' with SESSION_USER '|| SESSION_USER, CReport, io_contador);

    
    
   
   
	-------------------------------------------------------------------------------------------
    ------------------------------- ACTUALIZAR ACTIVO -----------------------------------------
    -------------------------------------------------------------------------------------------
    -- Marcamos con activo 255 todas las pólizas del mediador que realiza el traspaso con idProduct '341SP' y activo > 0
    UPDATE EXT.CARTERA
    SET ACTIVO = 255,
	    FECHA_FIN = ADD_DAYS(v_FECHA_TRASPASO,-1),
	    MODIF_USER = v_ModifUser,
	    MODIF_DATE = CURRENT_TIMESTAMP
    WHERE COD_MEDIADOR = v_COD_MEDIADOR
    AND COD_SUBCLAVE = v_COD_SUBCLAVE
    AND ACTIVO > 0
    AND IDPRODUCT = cIDPRODUCT
    ;
 
    -------------------------------------------------------------------------------------------
    ------------------------------- INSERTAR PÓLIZA TRASPASO ----------------------------------
    -------------------------------------------------------------------------------------------

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
	        C.COD_MEDIADOR,
	        C.COD_SUBCLAVE,
	        C.P_INTERMEDIACION,
	        v_FECHA_TRASPASO,
	        '2200-01-01',
	        C.P_ESPECIAL_EMISION, 
	        C.P_ESPECIAL_RENOVACION, 
	        C."NIF_TOMADOR",
	        C."NOMBRE_TOMADOR",
	        C."FECHA_EFECTO_TRASPASO",
	        C."MEDIADOR_PRINCIPAL_CIC",
	        1,--ACTIVO
	        CURRENT_TIMESTAMP,
	        CURRENT_TIMESTAMP,
	        v_ModifUser,
			C.MODIF_SOURCE,
	        C.FECHA_INICIO_OPESP,
	        C.FECHA_FIN_OPESP
        FROM EXT.CARTERA C 
        WHERE C.COD_MEDIADOR = v_COD_MEDIADOR
        AND C.COD_SUBCLAVE = v_COD_SUBCLAVE
        AND C.IDPRODUCT = cIDPRODUCT
        AND C.ACTIVO = 255
    	ORDER BY C.NUM_POLIZA,C.NUM_ANUALIDAD;
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || ::ROWCOUNT || ' REGISTROS. PARA EL MEDIADOR ' || :v_COD_MEDIADOR||'-'||:v_COD_SUBCLAVE, cReport, io_contador);    

   -- Marcamos con activo 0 todas las pólizas del mediador que realiza el traspaso con idProduct '341SP' y activo = 255
   UPDATE EXT.CARTERA
    SET ACTIVO = 0,
	    FECHA_FIN = ADD_DAYS(v_FECHA_TRASPASO,-1),
	    MODIF_USER = v_ModifUser,
	    -- MODIF_SOURCE = 'nombreCasoOrigen',
	    MODIF_DATE = CURRENT_TIMESTAMP
    WHERE COD_MEDIADOR = v_COD_MEDIADOR
    AND COD_SUBCLAVE = v_COD_SUBCLAVE
    AND ACTIVO = 255
    AND IDPRODUCT = cIDPRODUCT
    ;
 
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR ' || :v_COD_MEDIADOR||'-'||:v_COD_SUBCLAVE, cReport, io_contador);    

    
    
 
	
	-- CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'ACTUALIZADOS ' || ::ROWCOUNT || ' REGISTROS. MEDIADOR CEDENTE ' || :v_codMediadorCedente||'-'||:v_subClaveMediadorCedente, cReport, io_contador);
	
--	DROP TABLE IF EXISTS #TEMP_NUMPOLIZAS_PLAN_COMISIONAMIENTO;
--	DROP TABLE #TEMP_NUMPOLIZAS_PLAN_COMISIONAMIENTO;
    -- FIN
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);

END;


DO BEGIN 

-- DECLARE vRAMO VARCHAR(50) = 'CREDITO';
-- DECLARE vCASEID BIGINT = 1880;

TRUNCATE TABLE EXT.CARTERA;
INSERT INTO EXT.CARTERA SELECT * FROM EXT.CARTERA_BKP_04022025 ;
-- DELETE FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_con_derechos_credito%' AND DATETIME > '2025-03-07 10:00:00';

-- SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,FECHA_INICIO,FECHA_FIN FROM EXT.CARTERA WHERE COD_MEDIADOR = '0044' AND COD_SUBCLAVE = '0003' AND RAMO = vRAMO ORDER BY ACTIVO,COD_MEDIADOR,NUM_POLIZA,NUM_AVAL_HOST;

	
-- CALL EXT.SP_TRASPASO_MEDIADOR_MEDIADOR_CON_DERECHOS_CREDITO(1880,'Traspaso Pólizas y Avales-GM-335');

-- SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO,FECHA_FIN,MODIF_SOURCE,MODIF_DATE,MODIF_USER 
-- FROM EXT.CARTERA 
-- WHERE ( MODIF_USER LIKE '%1880%') AND RAMO = vRAMO ORDER BY ACTIVO,COD_MEDIADOR,NUM_AVAL_HOST,NUM_POLIZA;

-- SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,FECHA_INICIO,FECHA_FIN,MODIF_USER FROM EXT.CARTERA WHERE COD_MEDIADOR = '0044' AND COD_SUBCLAVE = '0003' AND RAMO = vRAMO ORDER BY ACTIVO,COD_MEDIADOR,NUM_POLIZA,NUM_AVAL_HOST;

-- -- SELECT ACTIVO,IDPAIS,IDPRODUCT,NUM_POLIZA,NUM_AVAL_HOST,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO,FECHA_FIN,MODIF_SOURCE,MODIF_DATE,MODIF_USER 
-- -- FROM EXT.CARTERA 
-- -- WHERE ((COD_MEDIADOR IN('3071') ) OR MODIF_SOURCE = 'Traspaso Pólizas y Avales-GM-335') AND RAMO = vRAMO ORDER BY ACTIVO,COD_MEDIADOR,NUM_AVAL_HOST,NUM_POLIZA;



-- SELECT * FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%sp_traspaso_mediador_mediador_con_derechos_credito%' AND DATETIME > '2025-03-07 10:00:00';
DELETE FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%SP_CAMBIO_PLAN_COMISIONAMIENTO%';
SELECT ACTIVO,RAMO,IDPRODUCT,NUM_POLIZA,NUM_ANUALIDAD,COD_MEDIADOR,COD_SUBCLAVE,FECHA_INICIO,FECHA_FIN,MODIF_USER FROM EXT.CARTERA WHERE COD_MEDIADOR = '0487' AND COD_SUBCLAVE = '0000' AND IDPRODUCT = '341SP' ORDER BY ACTIVO,NUM_POLIZA,NUM_ANUALIDAD;
CALL EXT.SP_CAMBIO_PLAN_COMISIONAMIENTO('0487','0000',CURRENT_DATE);


SELECT ACTIVO,RAMO,IDPRODUCT,NUM_POLIZA,NUM_ANUALIDAD,COD_MEDIADOR,COD_SUBCLAVE,FECHA_INICIO,FECHA_FIN,MODIF_USER FROM EXT.CARTERA WHERE COD_MEDIADOR = '0487' AND COD_SUBCLAVE = '0000' AND IDPRODUCT = '341SP' ORDER BY ACTIVO,NUM_POLIZA,NUM_ANUALIDAD;
SELECT *  FROM EXT.CSE_DEBUG WHERE PROCESO LIKE '%SP_CAMBIO_PLAN_COMISIONAMIENTO%';

--SELECT * FROM EXT.CARTERA WHERE IDPRODUCT = '341SP';
END;