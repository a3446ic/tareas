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
    
    --CASO1
    DECLARE cantRegistros10 Number := 0;
    DECLARE cantRegistros11 Number := 0;
    DECLARE cantRegistros12 Number := 0;
    DECLARE cantRegistros13 Number := 0;
    DECLARE cantRegistros14 Number := 0;
    DECLARE cantRegistros15 Number := 0;

    DECLARE cantPolizas10 Number := 0;
    DECLARE cantPolizas11 Number := 0;
    DECLARE cantPolizas12 Number := 0;
    DECLARE cantPolizas13 Number := 0;
    DECLARE cantPolizas14 Number := 0;
    DECLARE cantPolizas15 Number := 0;

    --CASO2
    DECLARE cantRegistros20 Number := 0;
    DECLARE cantRegistros21 Number := 0;
    DECLARE cantRegistros22 Number := 0;
    DECLARE cantRegistros23 Number := 0;
    DECLARE cantRegistros24 Number := 0;
    DECLARE cantRegistros25 Number := 0;

    DECLARE cantPolizas20 Number := 0;
    DECLARE cantPolizas21 Number := 0;
    DECLARE cantPolizas22 Number := 0;
    DECLARE cantPolizas23 Number := 0;
    DECLARE cantPolizas24 Number := 0;
    DECLARE cantPolizas25 Number := 0;

    --CASO3
    DECLARE cantRegistros30 Number := 0;
    DECLARE cantRegistros31 Number := 0;
    DECLARE cantRegistros32 Number := 0;
    DECLARE cantRegistros33 Number := 0;
    DECLARE cantRegistros34 Number := 0;
    DECLARE cantRegistros35 Number := 0;

    DECLARE cantPolizas30 Number := 0;
    DECLARE cantPolizas31 Number := 0;
    DECLARE cantPolizas32 Number := 0;
    DECLARE cantPolizas33 Number := 0;
    DECLARE cantPolizas34 Number := 0;
    DECLARE cantPolizas35 Number := 0;
    
    --CASO4
    DECLARE cantRegistros40 Number := 0;
    DECLARE cantRegistros41 Number := 0;
    DECLARE cantRegistros42 Number := 0;
    DECLARE cantRegistros43 Number := 0;
    DECLARE cantRegistros44 Number := 0;
    DECLARE cantRegistros45 Number := 0;

    DECLARE cantPolizas40 Number := 0;
    DECLARE cantPolizas41 Number := 0;
    DECLARE cantPolizas42 Number := 0;
    DECLARE cantPolizas43 Number := 0;
    DECLARE cantPolizas44 Number := 0;
    DECLARE cantPolizas45 Number := 0;
    

    -- Constantes
    DECLARE cReport CONSTANT VARCHAR(50) := 'SP_REVISION_CARTERA';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    
    DECLARE caso1 varchar(100)  := 'CASO 1 POLIZA CREDITO, UN SOLO MEDIADOR SIN TRASPASOS';
    DECLARE caso10 varchar(100) := 'CASO 1.0 - NO EXISTE PÓLIZA';
    DECLARE caso11 varchar(100) := 'CASO 1.1 >>> 1 POLIZA - 1 MEDIADOR -> DISTINTA MODALIDAD';
    DECLARE caso12 varchar(100) := 'CASO 1.2 >>> 1 POLIZA - 1 MEDIADOR -> FALTA POSITIONNAME';
    DECLARE caso13 varchar(100) := 'CASO 1.3 >>> 1 POLIZA - 1 MEDIADOR -> FALTA POSITIONNAME. EXISTE MÁS DE UNO';
    DECLARE caso14 varchar(100) := 'CASO 1.4 >>> 1 POLIZA - 1 MEDIADOR -> DISTINTO MEDIADOR';
    DECLARE caso15 varchar(100) := 'CASO 1.5 >>> 1 POLIZA - 1 MEDIADOR -> MISMO MEDIADOR';

    DECLARE caso2 varchar(100)  := 'CASO 2 POLIZA CREDITO, N MEDIADORES SIN TRASPASOS';
    DECLARE caso20 varchar(100) := 'CASO 2.0 - NO EXISTE PÓLIZA';
    DECLARE caso21 varchar(100) := 'CASO 2.1 >>> 1 POLIZA - N MEDIADOR SIN TRASPASOS -> DISTINTA MODALIDAD';
    DECLARE caso22 varchar(100) := 'CASO 2.2 >>> 1 POLIZA - N MEDIADOR SIN TRASPASOS -> FALTA POSITIONNAME';
    DECLARE caso23 varchar(100) := 'CASO 2.3 >>> 1 POLIZA - N MEDIADOR SIN TRASPASOS -> ALGÚN POSITIONNAME NULL';
    DECLARE caso24 varchar(100) := 'CASO 2.4 >>> 1 POLIZA - N MEDIADOR SIN TRASPASOS -> DISTINTO MEDIADOR';
    DECLARE caso25 varchar(100) := 'CASO 2.5 >>> 1 POLIZA - N MEDIADOR SIN TRASPASOS -> MISMO MEDIADOR';

    DECLARE caso3 varchar(100)  := 'CASO 3 POLIZA CREDITO, N MEDIADORES CON TRASPASOS';
    DECLARE caso30 varchar(100) := 'CASO 3.0 - NO EXISTE PÓLIZA';
    DECLARE caso31 varchar(100) := 'CASO 3.1 >>> 1 POLIZA - N MEDIADOR CON TRASPASOS -> DISTINTA MODALIDAD';
    DECLARE caso32 varchar(100) := 'CASO 3.2 >>> 1 POLIZA - N MEDIADOR CON TRASPASOS -> FALTA POSITIONNAME';
    DECLARE caso33 varchar(100) := 'CASO 3.3 >>> 1 POLIZA - N MEDIADOR CON TRASPASOS -> ALGÚN POSITIONNAME NULL';
    DECLARE caso34 varchar(100) := 'CASO 3.4 >>> 1 POLIZA - N MEDIADOR CON TRASPASOS -> DISTINTO MEDIADOR';
    DECLARE caso35 varchar(100) := 'CASO 3.5 >>> 1 POLIZA - N MEDIADOR CON TRASPASOS -> MISMO MEDIADOR';
    
    DECLARE caso4 varchar(100)  := 'CASO 1 POLIZA CAUCIÓN, UN SOLO MEDIADOR SIN TRASPASOS';
    DECLARE caso40 varchar(100) := 'CASO 4.0 - NO EXISTE PÓLIZA';
    DECLARE caso41 varchar(100) := 'CASO 4.1 >>> 1 POLIZA - 1 MEDIADOR -> DISTINTA MODALIDAD';
    DECLARE caso42 varchar(100) := 'CASO 4.2 >>> 1 POLIZA - 1 MEDIADOR -> FALTA POSITIONNAME';
    DECLARE caso43 varchar(100) := 'CASO 4.3 >>> 1 POLIZA - 1 MEDIADOR -> FALTA POSITIONNAME. EXISTE MÁS DE UNO';
    DECLARE caso44 varchar(100) := 'CASO 4.4 >>> 1 POLIZA - 1 MEDIADOR -> DISTINTO MEDIADOR';
    DECLARE caso45 varchar(100) := 'CASO 4.5 >>> 1 POLIZA - 1 MEDIADOR -> MISMO MEDIADOR';

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
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		SELECT CASE WHEN FEC_INI IS NULL OR FEC_INI = '0000-00-00' THEN '1990-12-31'ELSE FEC_INI END FROM EXT.RELASUJE_20241023 R
		 		WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
		 	)
		 	ELSE CT.FECHA_INICIO
      END FECHA_INICIO_TEMP
    , CASE 
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		SELECT CASE WHEN FEC_FIN IS NULL OR FEC_FIN = '0000-00-00' THEN '2200-01-01'ELSE FEC_FIN END FROM EXT.RELASUJE_20241023 R
		 		WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
		 	)
		 	ELSE CT.FECHA_FIN
      END FECHA_FIN_TEMP
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
        --COMPRUEBA QUE EXISTE PÓLIZA EN RELASUJE
            WHEN NOT EXISTS (
               SELECT 1
               FROM EXT.RELASUJE_20241023 R
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA
           ) THEN caso10 --1.0 NO EXISTE PÓLIZA

        --COMPRUEBA QUE TIENE LA MISMA MODALIDAD EN RELASUJE
            WHEN NOT EXISTS (
                SELECT 1
                FROM EXT.CARTERA CR
                INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                AND CR.RAMO = 'CREDITO'
           ) THEN caso11 --1.1 DISTINTA MODALIDAD

        --COMPRUEBA SI EXISTE MÁS DE UN POSITIONNAME Y ALGUNO ES NULL
            WHEN EXISTS (
                SELECT 1 
                FROM EXT.CARTERA CR INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                        AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD AND R.POSITIONNAME IS NULL
                    WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                    AND CR.RAMO = 'CREDITO'
            )
                THEN    CASE WHEN (SELECT COUNT(DISTINCT
                                CASE WHEN R.POSITIONNAME IS NULL THEN 'NULL' ELSE R.POSITIONNAME END) AS TOTAL_POSITIONNAME
                                FROM EXT.CARTERA CR
                                INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                                AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                                WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                                AND CR.RAMO = 'CREDITO'
                                GROUP BY CR.NUM_POLIZA) > 1 THEN caso13 --1.3 MÁS DE UN POSITIONNAME. ALGUNO NULL
           					ELSE caso12 --1.2 POSITIONNAME NULL
           			    END
           
        --COMPRUEBA SI TIENEN EL MISMO MEDIADOR
            WHEN EXISTS (SELECT R.POSITIONNAME FROM EXT.CARTERA CR
                        INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                        AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                        WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                        AND CR.COD_MEDIADOR || '-' || CR.COD_SUBCLAVE = R.POSITIONNAME
                        AND CR.RAMO = 'CREDITO')
                THEN caso15 --1.5 MISMO MEDIADOR
                ELSE caso14 --1.4 DISTINTO MEDIADOR
            END AS MODIF_CASE        
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR) = 1
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    LEFT JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
    WHERE CT.RAMO = 'CREDITO'
    ;	

    ---------------------------------------------------------------------------------------------------
    -- Obtener registros insertados para debug
    ---------------------------------------------------------------------------------------------------
    SELECT SUM(CASE WHEN MODIF_CASE = caso10 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso11 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso12 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso13 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso14 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso15 THEN 1 ELSE 0 END)
    INTO cantRegistros10, cantRegistros11, cantRegistros12, cantRegistros13, cantRegistros14, cantRegistros15
    FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO';

    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas10 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso10;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas11 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso11;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas12 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso12;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas13 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso13;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas14 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso14;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas15 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso15;

    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso10 || '   ' || cantPolizas10 || ' Pólizas - Insertados ' || cantRegistros10 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso11 || '   ' || cantPolizas11 || ' Pólizas - Insertados ' || cantRegistros11 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso12 || '   ' || cantPolizas12 || ' Pólizas - Insertados ' || cantRegistros12 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso13 || '   ' || cantPolizas13 || ' Pólizas - Insertados ' || cantRegistros13 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso14 || '   ' || cantPolizas14 || ' Pólizas - Insertados ' || cantRegistros14 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso15 || '   ' || cantPolizas15 || ' Pólizas - Insertados ' || cantRegistros15 || ' registros', cReport, io_contador);
    ---------------------------------------------------------------------------------------------------
    

	
    --------------------------------------------------------------------------------------------
    -- Caso 2 POLIZA CREDITO, N MEDIADORES SIN TRASPASOS
    --------------------------------------------------------------------------------------------

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso2 , CReport, io_contador);
    

    INSERT INTO EXT.CARTERA_OBJ_TEMP
    SELECT DISTINCT CT.*,C2.MODIF_CASE
    , CASE
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
                AND LPAD(CT.IDMODALIDAD,3,0) = R.MOD
        	) THEN (
		 		 SELECT CASE WHEN MAX(FEC_INI) IS NULL OR MAX(FEC_INI) = '0000-00-00' THEN '1990-12-31'ELSE MAX(FEC_INI) END
                 FROM EXT.RELASUJE_20241023 R
		 		 WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
                 AND LPAD(CT.IDMODALIDAD,3,0) = R.MOD
		 	)
		 	ELSE CT.FECHA_INICIO
      END FECHA_INICIO_TEMP
    , CASE
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		SELECT CASE WHEN MAX(FEC_FIN) IS NULL OR MAX(FEC_FIN) = '0000-00-00' THEN '2200-01-01'ELSE MAX(FEC_FIN) END FROM EXT.RELASUJE_20241023 R
		 		WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
		 	)
		 	ELSE CT.FECHA_FIN
      END FECHA_FIN_TEMP
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
        --COMPRUEBA QUE EXISTE PÓLIZA EN RELASUJE
            WHEN NOT EXISTS (
               SELECT 1
               FROM EXT.RELASUJE_20241023 R
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA
           ) THEN caso20 --2.0 NO EXISTE PÓLIZA

        --COMPRUEBA QUE TIENE LA MISMA MODALIDAD EN RELASUJE
            WHEN NOT EXISTS (
                SELECT 1
                FROM EXT.CARTERA CR
                INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                AND CR.RAMO = 'CREDITO'
           ) THEN caso21 --2.1 DISTINTA MODALIDAD

        --COMPRUEBA SI EXISTE MÁS DE UN POSITIONNAME Y ALGUNO ES NULL
            WHEN EXISTS (
                SELECT 1 
                FROM EXT.CARTERA CR INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                        AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD AND R.POSITIONNAME IS NULL
                    WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                    AND CR.RAMO = 'CREDITO'
            )
                THEN    CASE WHEN (SELECT COUNT(DISTINCT
                                CASE WHEN R.POSITIONNAME IS NULL THEN 'NULL' ELSE R.POSITIONNAME END) AS TOTAL_POSITIONNAME
                                FROM EXT.CARTERA CR
                                INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                                AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                                WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                                AND CR.RAMO = 'CREDITO'
                                GROUP BY CR.NUM_POLIZA) > 1 THEN caso23 --2.3 MÁS DE UN POSITIONNAME. ALGUNO NULL
           					ELSE caso22 --2.2 POSITIONNAME NULL
           			    END
           
        --COMPRUEBA SI TIENEN EL MISMO MEDIADOR
            WHEN EXISTS (SELECT R.POSITIONNAME FROM EXT.CARTERA CR
                        INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                        AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                        WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                        AND CR.COD_MEDIADOR || '-' || CR.COD_SUBCLAVE = R.POSITIONNAME
                        AND CR.RAMO = 'CREDITO')
                THEN caso25 --2.5 MISMO MEDIADOR
                ELSE caso24 --2.4 DISTINTO MEDIADOR
            END AS MODIF_CASE
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA
        HAVING (COUNT(DISTINCT C.COD_MEDIADOR) > 1 AND COUNT(DISTINCT C.ACTIVO) = 1) OR (COUNT(DISTINCT C.COD_MEDIADOR) > 1 AND COUNT(DISTINCT C.ACTIVO) > 1)
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    LEFT JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
    WHERE CT.RAMO = 'CREDITO'
    AND NOT EXISTS (
      SELECT 1 
      FROM EXT.CARTERA C_SUB
      WHERE C_SUB.NUM_POLIZA = CT.NUM_POLIZA
        AND C_SUB.ACTIVO = 2
  )
    ;	

    ---------------------------------------------------------------------------------------------------
    -- Obtener registros insertados para debug
    ---------------------------------------------------------------------------------------------------
    SELECT SUM(CASE WHEN MODIF_CASE = caso20 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso21 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso22 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso23 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso24 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso25 THEN 1 ELSE 0 END)   
    INTO cantRegistros20, cantRegistros21, cantRegistros22, cantRegistros23, cantRegistros24, cantRegistros25
    FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO';

    
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas20 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso20;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas21 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso21;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas22 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso22;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas23 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso23;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas24 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso24;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas25 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso25;

    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso20 || '   ' || cantPolizas20 || ' Pólizas - Insertados ' || cantRegistros20 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso21 || '   ' || cantPolizas21 || ' Pólizas - Insertados ' || cantRegistros21 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso22 || '   ' || cantPolizas22 || ' Pólizas - Insertados ' || cantRegistros22 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso23 || '   ' || cantPolizas23 || ' Pólizas - Insertados ' || cantRegistros23 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso24 || '   ' || cantPolizas24 || ' Pólizas - Insertados ' || cantRegistros24 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso25 || '   ' || cantPolizas25 || ' Pólizas - Insertados ' || cantRegistros25 || ' registros', cReport, io_contador);
    ---------------------------------------------------------------------------------------------------

    

    -- --------------------------------------------------------------------------------------------
    -- -- Caso 3 POLIZA CREDITO, N MEDIADORES CON TRASPASOS
    -- --------------------------------------------------------------------------------------------

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso3 , CReport, io_contador);
    

    INSERT INTO EXT.CARTERA_OBJ_TEMP
    SELECT CT.*,C2.MODIF_CASE
    , CASE
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
                AND LPAD(CT.IDMODALIDAD,3,0) = R.MOD
        	) THEN (
		 		  SELECT CASE WHEN MAX(FEC_INI) IS NULL OR MAX(FEC_INI) = '0000-00-00' THEN '1990-12-31'ELSE MAX(FEC_INI) END
                  FROM EXT.RELASUJE_20241023 R
		 		  WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
                  AND LPAD(CT.IDMODALIDAD,3,0) = R.MOD                
		 	)
		 	ELSE CT.FECHA_INICIO
      END FECHA_INICIO_TEMP
    , CASE   
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		 SELECT CASE WHEN MAX(FEC_FIN) IS NULL OR MAX(FEC_FIN) = '0000-00-00' THEN '2200-01-01'ELSE MAX(FEC_FIN) END FROM EXT.RELASUJE_20241023 R
		 		 WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME                
		 	)
		 	ELSE CT.FECHA_FIN
      END FECHA_FIN_TEMP    
    
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
        --COMPRUEBA QUE EXISTE PÓLIZA EN RELASUJE
            WHEN NOT EXISTS (
               SELECT 1
               FROM EXT.RELASUJE_20241023 R
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA
           ) THEN caso30 --3.0 NO EXISTE PÓLIZA

        --COMPRUEBA QUE TIENE LA MISMA MODALIDAD EN RELASUJE
            WHEN NOT EXISTS (
                SELECT 1
                FROM EXT.CARTERA CR
                INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                AND CR.RAMO = 'CREDITO'
           ) THEN caso31 --3.1 DISTINTA MODALIDAD

        --COMPRUEBA SI EXISTE MÁS DE UN POSITIONNAME Y ALGUNO ES NULL
            WHEN EXISTS (
                SELECT 1 
                FROM EXT.CARTERA CR INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                        AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD AND R.POSITIONNAME IS NULL
                    WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                    AND CR.RAMO = 'CREDITO'
            )
                THEN    CASE WHEN (SELECT COUNT(DISTINCT
                                CASE WHEN R.POSITIONNAME IS NULL THEN 'NULL' ELSE R.POSITIONNAME END) AS TOTAL_POSITIONNAME
                                FROM EXT.CARTERA CR
                                INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                                AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                                WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                                AND CR.RAMO = 'CREDITO'
                                GROUP BY CR.NUM_POLIZA) > 1 THEN caso23 --3.3 MÁS DE UN POSITIONNAME. ALGUNO NULL
           					ELSE caso32 --3.2 POSITIONNAME NULL
           			    END
           
        --COMPRUEBA SI TIENEN EL MISMO MEDIADOR
            WHEN EXISTS (SELECT R.POSITIONNAME FROM EXT.CARTERA CR
                        INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                        AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                        WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                        AND CR.COD_MEDIADOR || '-' || CR.COD_SUBCLAVE = R.POSITIONNAME
                        AND CR.RAMO = 'CREDITO')
                THEN caso35 --3.5 MISMO MEDIADOR
                ELSE caso34 --3.4 DISTINTO MEDIADOR
            END AS MODIF_CASE
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR) > 1 AND COUNT(DISTINCT C.ACTIVO) > 1
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    WHERE CT.RAMO = 'CREDITO'
    AND EXISTS (
      SELECT 1 
      FROM EXT.CARTERA C_SUB
      WHERE C_SUB.NUM_POLIZA = CT.NUM_POLIZA
        AND C_SUB.ACTIVO = 2
  )
    ;	

    
    ---------------------------------------------------------------------------------------------------
    -- Obtener registros insertados para debug
    ---------------------------------------------------------------------------------------------------
    SELECT SUM(CASE WHEN MODIF_CASE = caso30 THEN 1 ELSE 0 END)
     , SUM(CASE WHEN MODIF_CASE = caso31 THEN 1 ELSE 0 END)
     , SUM(CASE WHEN MODIF_CASE = caso32 THEN 1 ELSE 0 END)
     , SUM(CASE WHEN MODIF_CASE = caso33 THEN 1 ELSE 0 END)
     , SUM(CASE WHEN MODIF_CASE = caso34 THEN 1 ELSE 0 END)
     , SUM(CASE WHEN MODIF_CASE = caso35 THEN 1 ELSE 0 END)
    INTO cantRegistros30, cantRegistros31, cantRegistros32, cantRegistros33, cantRegistros34, cantRegistros35
    FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO';

    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas30 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso30;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas31 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso31;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas32 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso32;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas33 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso33;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas34 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso34;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas35 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CREDITO' AND MODIF_CASE = caso35;

    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso30 || '   ' || cantPolizas30 || ' Pólizas - Insertados ' || cantRegistros30 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso31 || '   ' || cantPolizas31 || ' Pólizas - Insertados ' || cantRegistros31 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso32 || '   ' || cantPolizas32 || ' Pólizas - Insertados ' || cantRegistros32 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso33 || '   ' || cantPolizas33 || ' Pólizas - Insertados ' || cantRegistros33 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso34 || '   ' || cantPolizas34 || ' Pólizas - Insertados ' || cantRegistros34 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso35 || '   ' || cantPolizas35 || ' Pólizas - Insertados ' || cantRegistros35 || ' registros', cReport, io_contador);
    ---------------------------------------------------------------------------------------------------

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
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		SELECT CASE WHEN MAX(FEC_INI) IS NULL OR MAX(FEC_INI) = '0000-00-00' THEN '1990-12-31'ELSE MAX(FEC_INI) END FROM EXT.RELASUJE_20241023 R
		 		WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
		 	)
		 	ELSE CT.FECHA_INICIO
      END FECHA_INICIO_TEMP
    , CASE 
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE_20241023 R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		SELECT CASE WHEN MAX(FEC_FIN) IS NULL OR MAX(FEC_FIN) = '0000-00-00' THEN '2200-01-01'ELSE MAX(FEC_FIN) END FROM EXT.RELASUJE_20241023 R
		 		WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
		 	)
		 	ELSE CT.FECHA_FIN
      END FECHA_FIN_TEMP
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
        --COMPRUEBA QUE EXISTE PÓLIZA EN RELASUJE
            WHEN NOT EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA --AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
            ) THEN caso40 --4.0 NO EXISTE PÓLIZA

        --COMPRUEBA QUE TIENE LA MISMA MODALIDAD EN RELASUJE
            WHEN NOT EXISTS(
                SELECT 1
                FROM EXT.CARTERA CR INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                    AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                WHERE CR.NUM_POLIZA = C.NUM_POLIZA
            ) THEN caso41 --4.1 DISTINTA MODALIDAD
        --COMPRUEBA SI EXISTE MÁS DE UN POSITIONNAME Y ALGUNO ES NULL
            WHEN EXISTS (
                SELECT 1 
                FROM EXT.CARTERA CR INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                        AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD AND R.POSITIONNAME IS NULL
                    WHERE CR.NUM_POLIZA = C.NUM_POLIZA
            )
                THEN    CASE WHEN (SELECT COUNT(DISTINCT
                                CASE WHEN R.POSITIONNAME IS NULL THEN 'NULL' ELSE R.POSITIONNAME END) AS TOTAL_POSITIONNAME
                                FROM EXT.CARTERA CR
                                INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                                AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                                WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                                GROUP BY CR.NUM_POLIZA) > 1 THEN caso43 --4.3 MÁS DE UN POSITIONNAME. ALGUNO NULL
           					ELSE caso42 --4.2 POSITIONNAME NULL
           			    END
        --COMPRUEBA SI TIENEN EL MISMO MEDIADOR
            WHEN COUNT(DISTINCT C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE) =
                    (SELECT COUNT(DISTINCT R.POSITIONNAME)
                    FROM EXT.CARTERA CR
                        INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                        AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                        WHERE CR.NUM_POLIZA = C.NUM_POLIZA)
                AND NOT EXISTS (
                    SELECT 1
                    FROM EXT.CARTERA C_SUB
                    WHERE C_SUB.NUM_POLIZA = C.NUM_POLIZA
                    AND C_SUB.RAMO = 'CAUCION'
                    AND (C_SUB.COD_MEDIADOR || '-' || C_SUB.COD_SUBCLAVE) NOT IN
                        (SELECT R.POSITIONNAME
                            FROM EXT.CARTERA CR
                            INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CR.NUM_POLIZA,8,0) = R.NUM_POLIZA
                            AND LPAD(CR.IDMODALIDAD,3,0) = R.MOD
                            WHERE CR.NUM_POLIZA = C.NUM_POLIZA
                        )
                )
            THEN caso45 --4.5 MISMO MEDIADOR
            ELSE caso44 --4.4 DISTINTO MEDIADOR
        END AS MODIF_CASE
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CAUCION'
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR) = 1
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    LEFT JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
    WHERE CT.RAMO = 'CAUCION'
    ;		

    ---------------------------------------------------------------------------------------------------
    -- Obtener registros insertados para debug
    ---------------------------------------------------------------------------------------------------
    SELECT SUM(CASE WHEN MODIF_CASE = caso40 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso41 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso42 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso43 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso44 THEN 1 ELSE 0 END)
    , SUM(CASE WHEN MODIF_CASE = caso45 THEN 1 ELSE 0 END)
    INTO cantRegistros40, cantRegistros41, cantRegistros42, cantRegistros43, cantRegistros44, cantRegistros45
    FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CAUCION';

    
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas40 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CAUCION' AND MODIF_CASE = caso40;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas41 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CAUCION' AND MODIF_CASE = caso41;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas42 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CAUCION' AND MODIF_CASE = caso42;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas43 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CAUCION' AND MODIF_CASE = caso43;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas44 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CAUCION' AND MODIF_CASE = caso44;
    SELECT COUNT(DISTINCT NUM_POLIZA) INTO cantPolizas45 FROM EXT.CARTERA_OBJ_TEMP WHERE RAMO = 'CAUCION' AND MODIF_CASE = caso45;

    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso40 || '   ' || cantPolizas40 || ' Pólizas - Insertados ' || cantRegistros40 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso41 || '   ' || cantPolizas41 || ' Pólizas - Insertados ' || cantRegistros41 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso42 || '   ' || cantPolizas42 || ' Pólizas - Insertados ' || cantRegistros42 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso43 || '   ' || cantPolizas43 || ' Pólizas - Insertados ' || cantRegistros43 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso44 || '   ' || cantPolizas44 || ' Pólizas - Insertados ' || cantRegistros44 || ' registros', cReport, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || caso45 || '   ' || cantPolizas45 || ' Pólizas - Insertados ' || cantRegistros45 || ' registros', cReport, io_contador);
    ---------------------------------------------------------------------------------------------------

    INSERT INTO EXT.CARTERA_OBJ_TEMP(RAMO
	, NUM_POLIZA
	, IDPRODUCT
	, IDMODALIDAD
	, NUM_ANUALIDAD
	, FECHA_INICIO
	, FECHA_INICIO_TEMP
	, FECHA_FIN
	, FECHA_FIN_TEMP
	, IDPAIS
	, MODIF_USER
    , MODIF_CASE)
	SELECT RAMO
		, NUM_POLIZA
    	, '' IDPRODUCT
		, IDMODALIDAD
    	, 0 NUM_ANUALIDAD
		, CASE WHEN 1 = (SELECT DISTINCT 1 FROM EXT.CARTERA C
		    				INNER JOIN EXT.RELASUJE_20241023 R ON 
		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA
	    				)-- EXISTE
	    				THEN (SELECT DISTINCT CASE WHEN MIN(FEC_INI) IS NULL OR MIN(FEC_INI) = '0000-00-00' THEN '1990-12-31'ELSE MIN(FEC_INI) END FROM EXT.CARTERA C
		    				INNER JOIN EXT.RELASUJE_20241023 R ON 
		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA)
		    			 --NO EXISTE 
	    				ELSE MIN(CT.FECHA_INICIO_TEMP)
	    	END FECHA_INICIO
	        , CASE WHEN 1 = (SELECT DISTINCT 1 FROM EXT.CARTERA C
		    				INNER JOIN EXT.RELASUJE_20241023 R ON 
		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA
	    				)-- EXISTE
	    				THEN (SELECT DISTINCT CASE WHEN MIN(FEC_INI) IS NULL OR MIN(FEC_INI) = '0000-00-00' THEN '1990-12-31'ELSE MIN(FEC_INI) END FROM EXT.CARTERA C
		    				INNER JOIN EXT.RELASUJE_20241023 R ON 
		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA)
		    			 --NO EXISTE 
	    				ELSE MIN(CT.FECHA_INICIO_TEMP)
	    	END FECHA_INICIO_TEMP
	        , CASE WHEN 1 = (SELECT DISTINCT 1 FROM EXT.CARTERA C
		    				INNER JOIN EXT.RELASUJE_20241023 R ON 
		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA
	    				)-- EXISTE
	    				THEN (SELECT DISTINCT CASE WHEN MAX(FEC_FIN) IS NULL OR MAX(FEC_FIN) = '0000-00-00' THEN '2200-01-01'ELSE MAX(FEC_FIN) END FROM EXT.CARTERA C
		    				INNER JOIN EXT.RELASUJE_20241023 R ON 
		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA)
		    			 --NO EXISTE 
	    				ELSE MAX(CT.FECHA_FIN_TEMP)
	    	END FECHA_FIN
	        , CASE WHEN 1 = (SELECT DISTINCT 1 FROM EXT.CARTERA C
		    				INNER JOIN EXT.RELASUJE_20241023 R ON 
		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA
	    				)-- EXISTE
	    				THEN (SELECT DISTINCT CASE WHEN MAX(FEC_FIN) IS NULL OR MAX(FEC_FIN) = '0000-00-00' THEN '2200-01-01'ELSE MAX(FEC_FIN) END FROM EXT.CARTERA C
		    				INNER JOIN EXT.RELASUJE_20241023 R ON 
		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA)
		    			 --NO EXISTE 
	    				ELSE MAX(CT.FECHA_FIN_TEMP)
	    	END FECHA_FIN_TEMP
		, IDPAIS
        , 'XXX'
        , MODIF_CASE
	FROM EXT.CARTERA_OBJ_TEMP CT
	WHERE RAMO = 'CAUCION' 
	GROUP BY RAMO
	 , NUM_POLIZA
	 , IDMODALIDAD
	 , IDPAIS
     , MODIF_CASE
;

    -- INSERT INTO EXT.CARTERA_OBJ_TEMP(RAMO, NUM_POLIZA,IDMODALIDAD,NUM_EXPEDIENTE,IDPAIS,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO_TEMP,FECHA_FIN_TEMP,ACTIVO
    -- 	, IDPRODUCT
    -- 	, NUM_ANUALIDAD
    -- 	, FECHA_INICIO
    -- 	, FECHA_FIN
    -- 	, MODIF_CASE
    --     , MODIF_USER
    -- )
    -- SELECT RAMO, NUM_POLIZA,IDMODALIDAD,NUM_EXPEDIENTE,IDPAIS,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO_TEMP,FECHA_FIN_TEMP,ACTIVO
    -- 	, '' IDPRODUCT
    -- 	, 0 NUM_ANUALIDAD
    -- 	, FECHA_INICIO_TEMP FECHA_INICIO
    -- 	, FECHA_FIN_TEMP FECHA_FIN
    -- 	, MODIF_CASE
    --     , 'XXX'
	-- FROM EXT.CARTERA_OBJ_TEMP	
	-- --WHERE MODIF_CASE NOT IN (caso40)
	-- WHERE RAMO = 'CAUCION'
	-- GROUP BY RAMO, NUM_POLIZA,IDMODALIDAD,NUM_EXPEDIENTE,IDPAIS,COD_MEDIADOR,COD_SUBCLAVE,P_INTERMEDIACION,FECHA_INICIO_TEMP,FECHA_FIN_TEMP,ACTIVO,MODIF_CASE;

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'EXPEDIENTE' || ' Insertados EXPEDIENTES ' || ::ROWCOUNT || ' registros', cReport, io_contador);
	
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);
END