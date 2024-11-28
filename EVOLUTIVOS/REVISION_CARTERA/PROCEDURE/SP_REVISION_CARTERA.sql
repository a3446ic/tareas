CREATE OR REPLACE PROCEDURE EXT.SP_REVISION_CARTERA LANGUAGE SQLSCRIPT AS
/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 13-Nov-2024 
	|---------------------------------------------------------------------------------------------- 
	| Procedure Purpose: REVISIÓN CARTERA
	| 
	| Version: 1	
	| 
	|
	----------------------------------------------------------------------------------------------- 
*/

BEGIN

    -- Declaración de variables
    DECLARE i_Tenant VARCHAR(127);
    DECLARE io_contador Number := 0;
    DECLARE cantRegistros Number := 0;
    DECLARE cantRegistros01 Number := 0;
    DECLARE cantRegistros10 Number := 0;
    DECLARE cantRegistros11 Number := 0;
    DECLARE cantRegistros12 Number := 0;

    DECLARE cantRegistros02 Number := 0;
    DECLARE cantRegistros20 Number := 0;
    DECLARE cantRegistros21 Number := 0;
    DECLARE cantRegistros22 Number := 0;

    DECLARE cantRegistros03 Number := 0;
    DECLARE cantRegistros30 Number := 0;
    DECLARE cantRegistros31 Number := 0;
    DECLARE cantRegistros32 Number := 0;
    
    DECLARE cantRegistros04 Number := 0;
    DECLARE cantRegistros40 Number := 0;
    DECLARE cantRegistros41 Number := 0;
    DECLARE cantRegistros42 Number := 0;
    

    -- Constantes
    DECLARE cReport CONSTANT VARCHAR(50) := 'SP_REVISION_CARTERA';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    
    DECLARE caso1 varchar(100)  := 'CASO 1 POLIZA CREDITO, UN SOLO MEDIADOR SIN TRASPASOS';
    DECLARE caso01 varchar(100) := 'CASO 0.1 - NO EXISTE PÓLIZA';
    DECLARE caso10 varchar(100) := 'CASO 1.0 >>> 1 POLIZA - 1 MEDIADOR -> FALTA POSITIONNAME';
    DECLARE caso101 varchar(100) := 'CASO 1.0.1 >>> 1 POLIZA - 1 MEDIADOR -> FALTA POSITIONNAME. EXISTE MÁS DE UNO';
    DECLARE caso11 varchar(100) := 'CASO 1.1 >>> 1 POLIZA - 1 MEDIADOR -> MISMO MEDIADOR';
    DECLARE caso12 varchar(100) := 'CASO 1.2 >>> 1 POLIZA - 1 MEDIADOR -> DISTINTO MEDIADOR';

    DECLARE caso2 varchar(100)  := 'CASO 2 POLIZA CREDITO, N MEDIADORES SIN TRASPASOS';
    DECLARE caso02 varchar(100) := 'CASO 0.2 - NO EXISTE PÓLIZA';
    DECLARE caso20 varchar(100) := 'CASO 2.0 >>> 1 POLIZA - N MEDIADOR SIN TRASPASOS -> FALTA POSITIONNAME';
    DECLARE caso21 varchar(100) := 'CASO 2.1 >>> 1 POLIZA - N MEDIADOR SIN TRASPASOS -> MISMO MEDIADOR';
    DECLARE caso22 varchar(100) := 'CASO 2.2 >>> 1 POLIZA - N MEDIADOR SIN TRASPASOS -> DISTINTO MEDIADOR';

    DECLARE caso3 varchar(100)  := 'CASO 3 POLIZA CREDITO, N MEDIADORES CON TRASPASOS';
    DECLARE caso03 varchar(100) := 'CASO 0.3 - NO EXISTE PÓLIZA';
    DECLARE caso30 varchar(100) := 'CASO 3.0 >>> 1 POLIZA - N MEDIADOR CON TRASPASOS -> FALTA POSITIONNAME';
    DECLARE caso31 varchar(100) := 'CASO 3.1 >>> 1 POLIZA - N MEDIADOR CON TRASPASOS -> MISMO MEDIADOR';
    DECLARE caso32 varchar(100) := 'CASO 3.2 >>> 1 POLIZA - N MEDIADOR CON TRASPASOS -> DISTINTO MEDIADOR';
    
    DECLARE caso4 varchar(100)  := 'CASO 1 POLIZA CAUCIÓN, UN SOLO MEDIADOR SIN TRASPASOS';
    DECLARE caso04 varchar(100) := 'CASO 0.4 - NO EXISTE PÓLIZA';
    DECLARE caso40 varchar(100) := 'CASO 4.0 >>> 1 POLIZA - 1 MEDIADOR -> FALTA POSITIONNAME';
    DECLARE caso41 varchar(100) := 'CASO 4.1 >>> 1 POLIZA - 1 MEDIADOR -> MISMO MEDIADOR';
    DECLARE caso42 varchar(100) := 'CASO 4.2 >>> 1 POLIZA - 1 MEDIADOR -> DISTINTO MEDIADOR';

    ----------------------------- HANDLER EXCEPTION -------------------------
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


-----------------------------------------------------------------------------------------
    -- CREAR TABLA TEMPORAL
    -----------------------------------------------------------------------------------------

    IF (SELECT TABLE_NAME FROM SYS.TABLES where SCHEMA_NAME= cEsquema and TABLE_NAME = 'CARTERA_OBJ_TEMP') IS NULL THEN
	    
        CREATE COLUMN TABLE "EXT"."CARTERA_OBJ_TEMP"(
            "RAMO" VARCHAR(20) NOT NULL,
            "IDPRODUCT" NVARCHAR(127) NOT NULL,
            "NUM_POLIZA" BIGINT CS_FIXED NOT NULL,
            "IDMODALIDAD" SMALLINT CS_INT NOT NULL,
            "IDSUBMODALIDAD" VARCHAR(20),
            "NUM_FIANZA" DECIMAL(30) CS_FIXED,
            "NUM_EXPEDIENTE" BIGINT CS_FIXED DEFAULT 0,
            "NUM_AVAL_HOST" BIGINT CS_FIXED,
            "NUM_ANUALIDAD" INTEGER CS_INT NOT NULL,
            "FECHA_EMISION" DATE CS_DAYDATE,
            "FECHA_EFECTO" DATE CS_DAYDATE,
            "FECHA_VENCIMIENTO" DATE CS_DAYDATE,
            "IDPAIS" SMALLINT CS_INT,
            "PRIMA_PROVISIONAL_INT" DECIMAL(18, 3) CS_FIXED,
            "PRIMA_PROVISIONAL_EXT" DECIMAL(18, 3) CS_FIXED,
            "IDDIVISA_INT" SMALLINT CS_INT,
            "IDDIVISA_EXT" SMALLINT CS_INT,
            "IDDIVISA_COBERTURA" SMALLINT CS_INT,
            "PRIMA_MIN_INT" DECIMAL(18, 3) CS_FIXED,
            "PRIMA_MIN_EXT" DECIMAL(18, 3) CS_FIXED,
            "COD_MEDIADOR" NVARCHAR(10),
            "COD_SUBCLAVE" NVARCHAR(10),
            "P_INTERMEDIACION" DECIMAL(6, 3) CS_FIXED,
            "FECHA_INICIO" DATE CS_DAYDATE NOT NULL,
            "FECHA_FIN" DATE CS_DAYDATE,
            "P_ESPECIAL_EMISION" DECIMAL(6, 4) CS_FIXED,
            "P_ESPECIAL_RENOVACION" DECIMAL(6, 4) CS_FIXED,
            "NIF_TOMADOR" NVARCHAR(20),
            "NOMBRE_TOMADOR" NVARCHAR(50),
            "FECHA_EFECTO_TRASPASO" DATE CS_DAYDATE,
            "MEDIADOR_PRINCIPAL_CIC" TINYINT CS_INT,
            "ACTIVO" TINYINT CS_INT DEFAULT 1,
            "CREATEDATE" LONGDATE CS_LONGDATE DEFAULT CURRENT_TIMESTAMP,
            "MODIF_DATE" LONGDATE CS_LONGDATE DEFAULT CURRENT_TIMESTAMP,
            "MODIF_USER" VARCHAR(50),
            "MODIF_SOURCE" VARCHAR(100),
            "MODIF_CASE" VARCHAR(250),
            "FECHA_INICIO_TEMP" DATE,
            "FECHA_FIN_TEMP" DATE,
            "FECHA_MODIF" BOOLEAN

        )
        UNLOAD PRIORITY 5 AUTO MERGE;

        CALL EXT.LIB_GLOBAL_CESCE :w_debug (
            i_Tenant,
            'CREADA TABLA ' || cEsquema || '.' || 'CARTERA_OBJ_TEMP',
            'cReport',
            io_contador
        );    
    END IF;

    TRUNCATE TABLE EXT.CARTERA_OBJ_TEMP;

    -----------------------------------------------------------------------------------------
    /****************************  POLIZAS CRÉDITO *****************************************/
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Tratamiento POLIZAS CRÉDITO', CReport, io_contador);


          
    -----------------------------------------------------------------------------------------
    -- CASO 1 POLIZA CREDITO, UN SOLO MEDIADOR SIN TRASPASOS
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso1 , CReport, io_contador);
    

    INSERT INTO EXT.CARTERA_OBJ_TEMP
    SELECT DISTINCT CT.*,C2.MODIF_CASE
    , CASE
        WHEN C2.MODIF_CASE IN (caso01) THEN CT.FECHA_INICIO
        WHEN C2.MODIF_CASE IN (caso10) THEN CT.FECHA_INICIO
        WHEN C2.MODIF_CASE IN (caso101,caso12) AND EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	)
			THEN (
				SELECT COALESCE(FEC_INI,'1990-12-31') FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
			)
		WHEN C2.MODIF_CASE IN (caso11) AND (R.FEC_INI IS NULL OR R.FEC_INI = '0000-00-00') THEN '1990-12-31' 
		ELSE CT.FECHA_INICIO 
      END FECHA_INICIO_TEMP
    , CASE 
        WHEN C2.MODIF_CASE IN (caso01) THEN CT.FECHA_FIN
        WHEN C2.MODIF_CASE IN (caso10) THEN CT.FECHA_FIN
        WHEN C2.MODIF_CASE IN (caso101,caso12) AND EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	)
			THEN (
				SELECT CASE WHEN FEC_FIN IS NULL OR FEC_FIN = '0000-00-00' THEN '2200-01-01' ELSE FEC_FIN END FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
			)
		WHEN C2.MODIF_CASE IN (caso11) AND (R.FEC_FIN IS NULL OR R.FEC_FIN = '0000-00-00') THEN '2200-01-01' 
		ELSE CT.FECHA_FIN
		END FECHA_FIN_TEMP
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
            WHEN NOT EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
           ) THEN caso01
            WHEN EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
               AND R.POSITIONNAME IS NULL
           ) THEN CASE WHEN (SELECT COUNT(*) FROM EXT.RELASUJE_20241023 R
           					WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD) > 1 THEN caso101
           					ELSE caso10 END
           
           --caso10	 
            WHEN COUNT(DISTINCT C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE) = 
                    (SELECT COUNT(DISTINCT R.POSITIONNAME) 
                    FROM EXT.RELASUJE_20241023 R 
                    WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD) 
                AND NOT EXISTS (
                    SELECT 1 
                    FROM EXT.CARTERA C_SUB 
                    WHERE C_SUB.NUM_POLIZA = C.NUM_POLIZA
                    AND C_SUB.RAMO = 'CREDITO'
                    AND (C_SUB.COD_MEDIADOR || '-' || C_SUB.COD_SUBCLAVE) NOT IN 
                        (SELECT R.POSITIONNAME 
                        FROM EXT.RELASUJE_20241023 R 
                        WHERE R.NUM_POLIZA = LPAD(C.NUM_POLIZA,8,0) AND LPAD(C.IDMODALIDAD,3,0) = R.MOD)
                )
            THEN caso11
            ELSE caso12
        END AS MODIF_CASE        
        FROM EXT.CARTERA C   
        WHERE C.RAMO = 'CREDITO'     
        GROUP BY C.NUM_POLIZA, C.IDMODALIDAD       
        HAVING COUNT(DISTINCT C.COD_MEDIADOR) = 1
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    LEFT JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
    WHERE CT.RAMO = 'CREDITO'
    ;	

    -- Obtener registros insertados para debug
    SELECT SUM(CASE WHEN MODIF_CASE = caso01 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso10 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso11 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso12 THEN 1 ELSE 0 END)
    INTO cantRegistros01, cantRegistros10, cantRegistros11, cantRegistros12
    FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO';

    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso01 || ' Insertados ' || cantRegistros01 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso10 || ' Insertados ' || cantRegistros10 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso11 || ' Insertados ' || cantRegistros11 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso12 || ' Insertados ' || cantRegistros12 || ' registros', cReport, io_contador);

    

	
    --------------------------------------------------------------------------------------------
    -- Caso 2 POLIZA CREDITO, N MEDIADORES SIN TRASPASOS
    --------------------------------------------------------------------------------------------

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso2 , CReport, io_contador);
    

    INSERT INTO EXT.CARTERA_OBJ_TEMP
    SELECT DISTINCT CT.*,C2.MODIF_CASE
    , CASE
        WHEN C2.MODIF_CASE IN (caso02) THEN CT.FECHA_INICIO
        --WHEN C2.MODIF_CASE IN (caso10) THEN CT.FECHA_INICIO
        WHEN C2.MODIF_CASE IN (caso20,caso22) AND EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	)
			THEN (
				SELECT COALESCE(FEC_INI,'1990-12-31') FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
			)
		WHEN C2.MODIF_CASE IN (caso21) AND (R.FEC_INI IS NULL OR R.FEC_INI = '0000-00-00') THEN '1990-12-31' 
		ELSE CT.FECHA_INICIO 
      END FECHA_INICIO_TEMP
    , CASE 
        WHEN C2.MODIF_CASE IN (caso02) THEN CT.FECHA_FIN
        --WHEN C2.MODIF_CASE IN (caso10) THEN CT.FECHA_FIN
        WHEN C2.MODIF_CASE IN (caso20,caso22) AND EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	)
			THEN (
				SELECT CASE WHEN FEC_FIN IS NULL OR FEC_FIN = '0000-00-00' THEN '2200-01-01' ELSE FEC_FIN END FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
			)
		WHEN C2.MODIF_CASE IN (caso21) AND (R.FEC_FIN IS NULL OR R.FEC_FIN = '0000-00-00') THEN '2200-01-01' 
		ELSE CT.FECHA_FIN
		END FECHA_FIN_TEMP
    -- ,CASE 
    --     WHEN C2.MODIF_CASE IN (caso02,caso20) THEN NULL
    --     WHEN C2.MODIF_CASE IN (caso21, caso22) THEN R.FEC_INI
    -- END FECHA_INICIO_TEMP
    -- ,CASE 
    --     WHEN C2.MODIF_CASE IN (caso02,caso20) THEN NULL
    --     WHEN C2.MODIF_CASE IN (caso21, caso22) AND (R.FEC_FIN IS NULL OR R.FEC_FIN = '0000-00-00') THEN '2200-01-01' ELSE R.FEC_FIN
    -- END FECHA_FIN_TEMP
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
            WHEN NOT EXISTS (
               SELECT 1
               FROM EXT.RELASUJE_20241023 R
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
           ) THEN caso02
            WHEN EXISTS (
               SELECT 1
               FROM EXT.RELASUJE_20241023 R
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
               AND R.POSITIONNAME IS NULL
           ) THEN caso20
            WHEN COUNT(DISTINCT C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE) =
                    (SELECT COUNT(DISTINCT R.POSITIONNAME)
                    FROM EXT.RELASUJE_20241023 R
                    WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD)
                AND NOT EXISTS (
                    SELECT 1
                    FROM EXT.CARTERA C_SUB
                    WHERE C_SUB.NUM_POLIZA = C.NUM_POLIZA
                    AND C_SUB.RAMO = 'CREDITO'
                    AND (C_SUB.COD_MEDIADOR || '-' || C_SUB.COD_SUBCLAVE) NOT IN
                        (SELECT R.POSITIONNAME
                        FROM EXT.RELASUJE_20241023 R
                        WHERE R.NUM_POLIZA = LPAD(C.NUM_POLIZA,8,0) AND LPAD(C.IDMODALIDAD,3,0) = R.MOD)
                )
            THEN caso21
            ELSE caso22
        END AS MODIF_CASE
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA, C.IDMODALIDAD
        HAVING COUNT(DISTINCT C.COD_MEDIADOR) > 1 AND COUNT(DISTINCT C.ACTIVO) = 1
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    LEFT JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
    WHERE CT.RAMO = 'CREDITO'
    ;	

    -- Obtener registros insertados para debug
    SELECT SUM(CASE WHEN MODIF_CASE = caso02 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso20 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso21 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso22 THEN 1 ELSE 0 END)
    INTO cantRegistros02, cantRegistros20, cantRegistros21, cantRegistros22
    FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO';

    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso02 || ' Insertados ' || cantRegistros02 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso20 || ' Insertados ' || cantRegistros20 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso21 || ' Insertados ' || cantRegistros21 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso22 || ' Insertados ' || cantRegistros22 || ' registros', cReport, io_contador);


    

    -- --------------------------------------------------------------------------------------------
    -- -- Caso 3 POLIZA CREDITO, N MEDIADORES CON TRASPASOS
    -- --------------------------------------------------------------------------------------------

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso3 , CReport, io_contador);
    

    INSERT INTO EXT.CARTERA_OBJ_TEMP
    SELECT CT.*,C2.MODIF_CASE
    --,CT.FECHA_INICIO
    , CASE WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	)
			THEN (
				 SELECT  COALESCE(MAX(FEC_INI),'1990-12-31') FROM EXT.RELASUJE_20241023 R
				 WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
			
			)
			ELSE CT.FECHA_INICIO END FECHA_INICIO_TEMP
    --,CASE WHEN CT.ACTIVO = 2 THEN COALESCE(CT.FECHA_FIN, DATE '2200-01-01') ELSE CT.FECHA_FIN END 
    , CASE WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	)
			THEN (
				 SELECT  CASE WHEN (MAX(FEC_FIN) IS NULL OR MAX(FEC_FIN) = '0000-00-00') THEN '2200-01-01' ELSE MAX(FEC_FIN) END FROM EXT.RELASUJE_20241023 R
				 WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
			
			)
			ELSE CT.FECHA_FIN END FECHA_FIN_TEMP
    
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
            WHEN NOT EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
           ) THEN caso03
            WHEN EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
               AND R.POSITIONNAME IS NULL
           ) THEN caso30
            WHEN COUNT(DISTINCT C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE) =
                    (SELECT COUNT(DISTINCT R.POSITIONNAME)
                    FROM EXT.RELASUJE_20241023 R
                    WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD)
                AND NOT EXISTS (
                    SELECT 1 
                    FROM EXT.CARTERA C_SUB 
                    WHERE C_SUB.NUM_POLIZA = C.NUM_POLIZA
                    AND C_SUB.RAMO = 'CREDITO'
                    AND (C_SUB.COD_MEDIADOR || '-' || C_SUB.COD_SUBCLAVE) NOT IN
                        (SELECT R.POSITIONNAME 
                        FROM EXT.RELASUJE_20241023 R 
                        WHERE R.NUM_POLIZA = LPAD(C.NUM_POLIZA,8,0) AND LPAD(C.IDMODALIDAD,3,0) = R.MOD)
                )
            THEN caso31
            ELSE caso32
        END AS MODIF_CASE
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA, C.IDMODALIDAD
        HAVING COUNT(DISTINCT C.COD_MEDIADOR) > 1 AND COUNT(DISTINCT C.ACTIVO) > 1
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    WHERE CT.RAMO = 'CREDITO'
    ;	

    
    SELECT SUM(CASE WHEN MODIF_CASE = caso03 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso30 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso31 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso32 THEN 1 ELSE 0 END)
    INTO cantRegistros03, cantRegistros30, cantRegistros31, cantRegistros32
    FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO';

    -- Obtener registros insertados para debug
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso03 || ' Insertados ' || cantRegistros03 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso30 || ' Insertados ' || cantRegistros30 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso31 || ' Insertados ' || cantRegistros31 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso32 || ' Insertados ' || cantRegistros32 || ' registros', cReport, io_contador);



    -----------------------------------------------------------------------------------------
    /****************************  POLIZAS CAUCIÓN *****************************************/
    -----------------------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------------------
    -- CASO 4 POLIZA CAUCIÓN, UN SOLO MEDIADOR SIN TRASPASOS
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso4 , CReport, io_contador);
    

    INSERT INTO EXT.CARTERA_OBJ_TEMP
    SELECT DISTINCT CT.*,C2.MODIF_CASE
    , CASE
        WHEN C2.MODIF_CASE IN (caso04,caso40) THEN NULL
        WHEN C2.MODIF_CASE IN (caso41,caso42) AND (R.FEC_INI IS NULL OR R.FEC_INI = '0000-00-00') THEN '1990-12-31' ELSE R.FEC_INI END FECHA_INICIO_TEMP
    , CASE
        WHEN C2.MODIF_CASE IN (caso04,caso40) THEN NULL
        WHEN C2.MODIF_CASE IN (caso41,caso42) AND (R.FEC_FIN IS NULL OR R.FEC_FIN = '0000-00-00') THEN '2200-01-01' ELSE R.FEC_FIN END FECHA_FIN_TEMP
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
            WHEN NOT EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
           ) THEN caso04
            WHEN EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
               AND R.POSITIONNAME IS NULL
           ) THEN caso40
            WHEN COUNT(DISTINCT C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE) =
                    (SELECT COUNT(DISTINCT R.POSITIONNAME)
                    FROM EXT.RELASUJE_20241023 R
                    WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND LPAD(C.IDMODALIDAD,3,0) = R.MOD)
                AND NOT EXISTS (
                    SELECT 1 
                    FROM EXT.CARTERA C_SUB 
                    WHERE C_SUB.NUM_POLIZA = C.NUM_POLIZA
                    AND C_SUB.RAMO = 'CAUCION'
                    AND (C_SUB.COD_MEDIADOR || '-' || C_SUB.COD_SUBCLAVE) NOT IN
                        (SELECT R.POSITIONNAME 
                        FROM EXT.RELASUJE_20241023 R 
                        WHERE R.NUM_POLIZA = LPAD(C.NUM_POLIZA,8,0) AND LPAD(C.IDMODALIDAD,3,0) = R.MOD)
                )
            THEN caso41
            ELSE caso42
        END AS MODIF_CASE
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CAUCION'
        GROUP BY C.NUM_POLIZA, C.IDMODALIDAD
        HAVING COUNT(DISTINCT C.COD_MEDIADOR) = 1
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    LEFT JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
    WHERE CT.RAMO = 'CAUCION'
    ;	

    -- Obtener registros insertados para debug
    SELECT SUM(CASE WHEN MODIF_CASE = caso04 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso40 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso41 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso42 THEN 1 ELSE 0 END)
    INTO cantRegistros04, cantRegistros40, cantRegistros41, cantRegistros42
    FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CAUCION';

    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso04 || ' Insertados ' || cantRegistros04 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso40 || ' Insertados ' || cantRegistros40 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso41 || ' Insertados ' || cantRegistros41 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso42 || ' Insertados ' || cantRegistros42 || ' registros', cReport, io_contador);
    
    INSERT INTO EXT.CARTERA_OBJ_TEMP(RAMO, NUM_POLIZA,IDMODALIDAD,NUM_EXPEDIENTE,IDPAIS,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO_TEMP,FECHA_FIN_TEMP,ACTIVO
    	, IDPRODUCT
    	, NUM_ANUALIDAD
    	, FECHA_INICIO
    	, FECHA_FIN
    	, MODIF_CASE
    )
    SELECT RAMO, NUM_POLIZA,IDMODALIDAD,NUM_EXPEDIENTE,IDPAIS,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO_TEMP,FECHA_FIN_TEMP,ACTIVO
    	, '' IDPRODUCT
    	, 0 NUM_ANUALIDAD
    	, FECHA_INICIO_TEMP FECHA_INICIO
    	, FECHA_FIN_TEMP FECHA_FIN
    	, MODIF_CASE
	FROM EXT.CARTERA_OBJ_TEMP	
	WHERE MODIF_CASE NOT IN (caso04,caso40)
	AND RAMO = 'CAUCION'
	GROUP BY RAMO, NUM_POLIZA,IDMODALIDAD,NUM_EXPEDIENTE,IDPAIS,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO_TEMP,FECHA_FIN_TEMP,ACTIVO,MODIF_CASE;

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'EXPEDIENTE' || ' Insertados EXPEDIENTES ' || ::ROWCOUNT || ' registros', cReport, io_contador);
	
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);
END