CREATE OR REPLACE PROCEDURE EXT.SP_REVISION_CARTERA_V3 LANGUAGE SQLSCRIPT AS
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
    

    -- Constantes
    DECLARE cReport CONSTANT VARCHAR(50) := 'SP_REVISION_CARTERA';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';

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
	    
            CREATE COLUMN TABLE EXT.CARTERA_OBJ_TEMP AS (SELECT * FROM EXT.CARTERA WHERE 1=0);

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
    -- Caso 1 POLIZA CREDITO, UN SOLO MEDIADOR SIN TRASPASOS
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASO 1.1 POLIZA CREDITO, UN SOLO MEDIADOR SIN TRASPASOS', CReport, io_contador);
    

    INSERT INTO EXT.CARTERA_OBJ_TEMP
    SELECT CT.*,C2.MODIF_CASE FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
            WHEN NOT EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA
           ) THEN 'CASO 0.1 No existe póliza'
            WHEN EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA
               AND R.POSITIONNAME IS NULL
           ) THEN 'CASO 1.0 >>> 1 POLIZA - 1 MEDIADOR -> FALTA POSITIONNAME'	 
            WHEN COUNT(DISTINCT C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE) = 
                    (SELECT COUNT(DISTINCT R.POSITIONNAME) 
                    FROM EXT.RELASUJE_20241023 R 
                    WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA) 
                AND NOT EXISTS (
                    SELECT 1 
                    FROM EXT.CARTERA C_SUB 
                    WHERE C_SUB.NUM_POLIZA = C.NUM_POLIZA
                    AND C_SUB.RAMO = 'CREDITO'
                    AND (C_SUB.COD_MEDIADOR || '-' || C_SUB.COD_SUBCLAVE) NOT IN 
                        (SELECT R.POSITIONNAME 
                        FROM EXT.RELASUJE_20241023 R 
                        WHERE R.NUM_POLIZA = LPAD(C.NUM_POLIZA,8,0))
                )
            THEN 'CASO 1.1 >>> 1 POLIZA - 1 MEDIADOR -> MISO MEDIADOR'
            ELSE 'CASO 1.2 >>> 1 POLIZA - 1 MEDIADOR -> DISTINTO MEDIADOR'
        END AS MODIF_CASE
        FROM EXT.CARTERA C   
        WHERE C.RAMO = 'CREDITO'     
        GROUP BY C.NUM_POLIZA        
        HAVING COUNT(DISTINCT C.COD_MEDIADOR) = 1
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    WHERE CT.RAMO = 'CREDITO'
    ;	

    

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASO 1 Insertados ' || ::ROWCOUNT || ' registros', cReport, io_contador);

    

	
    --------------------------------------------------------------------------------------------
    -- Caso 2.0 POLIZA CREDITO, N MEDIADORES SIN TRASPASOS. TIENEN DISTINTO MEDIADOR EN RELASUJE
    --------------------------------------------------------------------------------------------

    INSERT INTO EXT.CARTERA_OBJ_TEMP
    SELECT CT.*,C2.MODIF_CASE FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
        WHEN NOT EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA
           ) THEN 'CASO 0.2 No existe póliza'
            WHEN EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA
               AND R.POSITIONNAME IS NULL
           ) THEN 'CASO 2.0 >>> 1 POLIZA - N MEDIADOR -> FALTA POSITIONNAME'	  
            WHEN COUNT(DISTINCT C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE) = 
                    (SELECT COUNT(DISTINCT R.POSITIONNAME) 
                    FROM EXT.RELASUJE_20241023 R 
                    WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA) 
                AND NOT EXISTS (
                    SELECT 1 
                    FROM EXT.CARTERA C_SUB 
                    WHERE C_SUB.NUM_POLIZA = C.NUM_POLIZA
                    AND C_SUB.RAMO = 'CREDITO'
                    AND (C_SUB.COD_MEDIADOR || '-' || C_SUB.COD_SUBCLAVE) NOT IN 
                        (SELECT R.POSITIONNAME 
                        FROM EXT.RELASUJE_20241023 R 
                        WHERE R.NUM_POLIZA = LPAD(C.NUM_POLIZA,8,0))
                )
            THEN 'CASO 2.1 >>> 1 POLIZA - N MEDIADOR -> MISMO MEDIADOR'
            ELSE 'CASO 2.2 >>> 1 POLIZA - N MEDIADOR -> DISTINTO MEDIADOR'
        END AS MODIF_CASE
        FROM EXT.CARTERA C      
        WHERE C.RAMO = 'CREDITO'  
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR) > 1 AND COUNT(DISTINCT C.ACTIVO) = 1        
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    WHERE CT.RAMO = 'CREDITO'
    ;	

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASO 2 Insertados ' || ::ROWCOUNT || ' registros', cReport, io_contador);

    --------------------------------------------------------------------------------------------
    -- Caso 3.0 POLIZA CREDITO, N MEDIADORES CON TRASPASOS. TIENEN DISTINTO MEDIADOR EN RELASUJE
    --------------------------------------------------------------------------------------------

    INSERT INTO EXT.CARTERA_OBJ_TEMP
    SELECT CT.*,C2.MODIF_CASE FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA,
        CASE
        WHEN NOT EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA
           ) THEN 'CASO 0.3 NO EXISTE PÓLIZA'
            WHEN EXISTS (
               SELECT 1 
               FROM EXT.RELASUJE_20241023 R 
               WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA
               AND R.POSITIONNAME IS NULL
           ) THEN 'CASO 3.0 >>> 1 POLIZA - N MEDIADOR -> FALTA POSITIONNAME'	  
            WHEN COUNT(DISTINCT C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE) = 
                    (SELECT COUNT(DISTINCT R.POSITIONNAME) 
                    FROM EXT.RELASUJE_20241023 R 
                    WHERE LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA) 
                AND NOT EXISTS (
                    SELECT 1 
                    FROM EXT.CARTERA C_SUB 
                    WHERE C_SUB.NUM_POLIZA = C.NUM_POLIZA
                    AND C_SUB.RAMO = 'CREDITO'
                    AND (C_SUB.COD_MEDIADOR || '-' || C_SUB.COD_SUBCLAVE) NOT IN 
                        (SELECT R.POSITIONNAME 
                        FROM EXT.RELASUJE_20241023 R 
                        WHERE R.NUM_POLIZA = LPAD(C.NUM_POLIZA,8,0))
                )
            THEN 'CASO 3.1 >>> 1 POLIZA - N MEDIADOR -> MISMO MEDIADOR'
            ELSE 'CASO 3.2 >>> 1 POLIZA - N MEDIADOR -> DISTINTO MEDIADOR'
        END AS MODIF_CASE
        FROM EXT.CARTERA C        
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR) > 1 AND COUNT(DISTINCT C.ACTIVO) > 1        
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    WHERE CT.RAMO = 'CREDITO'
    ;	

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASO 3 Insertados ' || ::ROWCOUNT || ' registros', cReport, io_contador);

    -----------------------------------------------------------------------------------------
    /****************************  POLIZAS CAUCIÓN *****************************************/
    -----------------------------------------------------------------------------------------

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);
END