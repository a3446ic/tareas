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
    

    -- Constantes
    DECLARE cReport CONSTANT VARCHAR(50) := 'SP_REVISION_CARTERA';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';

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
    -- Caso 0 No existe la póliza en RELASUJE_20241023
    -----------------------------------------------------------------------------------------

    INSERT INTO EXT.CARTERA_OBJ
    SELECT C.*,
        CASE WHEN R.NUM_POLIZA IS NULL THEN 'CASO 0 FALTA NUM_POLIZA' 
	         WHEN R.POSITIONNAME IS NULL THEN 'CASO 0 FALTA POSITIONNAME' 
        END
    FROM EXT.CARTERA C LEFT JOIN EXT.RELASUJE_20241023 R ON LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA AND C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE = R.POSITIONNAME
    WHERE C.RAMO = 'CREDITO';

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, ::ROWCOUNT || ' Pólizas de CREDITO no existen la tabla RESALUJ_20241023', CReport, io_contador);

    -----------------------------------------------------------------------------------------
    /****************************  POLIZAS CRÉDITO *****************************************/
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Tratamiento POLIZAS CRÉDITO', CReport, io_contador);
    
    -----------------------------------------------------------------------------------------
    -- Caso 1.1 POLIZA CREDITO, UN SOLO MEDIADOR SIN TRASPASOS
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASO 1.1 POLIZA CREDITO, UN SOLO MEDIADOR SIN TRASPASOS', CReport, io_contador);

    INSERT INTO EXT.CARTERA_OBJ
    SELECT C.*, 'CASO 1.1'    
    FROM EXT.CARTERA C INNER JOIN (SELECT CT.NUM_POLIZA FROM EXT.CARTERA CT INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
            AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        WHERE RAMO = 'CREDITO'
        AND R.POSITIONNAME IS NOT NULL
        GROUP BY CT.NUM_POLIZA
    	HAVING COUNT(DISTINCT CT.COD_MEDIADOR) = 1
    ) C2 ON C.NUM_POLIZA = C2.NUM_POLIZA
    WHERE C.RAMO = 'CREDITO'
    ;

    
	UPDATE CO
    SET FECHA_INICIO = COALESCE(R.FEC_INI, '1990-12-31')
    , FECHA_FIN = COALESCE(R.FEC_FIN,'2020-01-01')
    , MODIF_CASE = MODIF_CASE || ' UPDATE FECHA 1.1'
    FROM EXT.CARTERA_OBJ CO INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CO.NUM_POLIZA,8,0) = R.NUM_POLIZA
    WHERE CO.RAMO = 'CREDITO'
    AND CO.NUM_POLIZA IN ( SELECT CT.NUM_POLIZA FROM EXT.CARTERA_OBJ CT 
        WHERE RAMO = 'CREDITO'
        AND R.POSITIONNAME IS NOT NULL
        GROUP BY CT.NUM_POLIZA
    	HAVING COUNT(DISTINCT CT.COD_MEDIADOR) = 1
    )
    ;

	
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASO 1.1 Insertados ' || ::ROWCOUNT || ' registros', cReport, io_contador);

    -----------------------------------------------------------------------------------------
    --CASO 1.2 POLIZA CREDITO, DOS MEDIADORES SIN TRASPASO
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASO 1.2 POLIZA CREDITO, DOS MEDIADORES SIN TRASPASO', CReport, io_contador);

    INSERT INTO EXT.CARTERA_OBJ
    SELECT C.*, 'CASO 1.2'    
    FROM EXT.CARTERA C INNER JOIN (SELECT CT.NUM_POLIZA FROM EXT.CARTERA CT INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
        WHERE RAMO = 'CREDITO'
        AND R.POSITIONNAME IS NOT NULL
        GROUP BY CT.NUM_POLIZA
    	HAVING COUNT(DISTINCT CT.COD_MEDIADOR) > 1 AND COUNT(DISTINCT CT.ACTIVO) = 1 ) C2 ON C.NUM_POLIZA = C2.NUM_POLIZA
    WHERE C.RAMO = 'CREDITO'
    ;
    -- SELECT C.*,'CASO 1.2' FROM EXT.CARTERA C  INNER JOIN (
    -- 	SELECT DISTINCT CT.NUM_POLIZA
	--     	FROM EXT.CARTERA CT INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
	--     	WHERE RAMO = 'CREDITO'
	--     	GROUP BY CT.NUM_POLIZA,CT.ACTIVO
	-- 	    HAVING COUNT(DISTINCT CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE) = 1
	--     	) C2 
	-- ON C.NUM_POLIZA = C2.NUM_POLIZA;
	    
	UPDATE CO
	SET FECHA_INICIO = COALESCE(R.FEC_INI, '1990-12-31')
    , FECHA_FIN = COALESCE(R.FEC_FIN,'2020-01-01')
   , MODIF_CASE = MODIF_CASE || ' UPDATE FECHA 1.2'
	FROM EXT.CARTERA_OBJ CO
	JOIN EXT.RELASUJE_20241023 R 
	    ON LPAD(CO.NUM_POLIZA,8,0) = R.NUM_POLIZA 
	    AND CO.COD_MEDIADOR || '-' || CO.COD_SUBCLAVE = R.POSITIONNAME
	WHERE CO.RAMO = 'CREDITO'
    AND R.POSITIONNAME IS NOT NULL
	  AND CO.COD_MEDIADOR || '-' || CO.COD_SUBCLAVE IN (
	      SELECT COD_MEDIADOR || '-' || COD_SUBCLAVE
	      FROM EXT.CARTERA_OBJ CO2
	      WHERE CO2.NUM_POLIZA = CO.NUM_POLIZA
	      GROUP BY COD_MEDIADOR || '-' || COD_SUBCLAVE
	      HAVING COUNT(DISTINCT COD_MEDIADOR || '-' || COD_SUBCLAVE) = 1
	  );
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASO 1.2 Insertados ' || ::ROWCOUNT || ' registros' , cReport, io_contador);

    -----------------------------------------------------------------------------------------
    --CASO 1.3 POLIZA CREDITO, DOS MEDIADORES CON TRASPASO
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASO 1.2 POLIZA CREDITO, DOS MEDIADORES CON TRASPASO', CReport, io_contador);

    INSERT INTO EXT.CARTERA_OBJ
    SELECT C.*, 'CASO 1.3'
    -- C.NUM_POLIZA,
    -- COUNT(DISTINCT COD_MEDIADOR) AS num_mediadores,
    -- COUNT(DISTINCT ACTIVO) AS num_activos,
    -- CASE 
    --     WHEN COUNT(DISTINCT COD_MEDIADOR) = 1 THEN 'Un mediador'
    --     WHEN COUNT(DISTINCT COD_MEDIADOR) > 1 AND COUNT(DISTINCT ACTIVO) = 1 THEN 'Varios mediadores, mismo activo'
    --     WHEN COUNT(DISTINCT COD_MEDIADOR) > 1 AND COUNT(DISTINCT ACTIVO) > 1 THEN 'Varios mediadores, varios activos'
    -- END AS clasificacion
    FROM EXT.CARTERA C INNER JOIN (SELECT CT.NUM_POLIZA FROM EXT.CARTERA CT INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
        WHERE RAMO = 'CREDITO'
        AND R.POSITIONNAME IS NOT NULL
        GROUP BY CT.NUM_POLIZA
    	HAVING COUNT(DISTINCT COD_MEDIADOR) > 1 AND COUNT(DISTINCT ACTIVO) > 1 ) C2 ON C.NUM_POLIZA = C2.NUM_POLIZA
    WHERE C.RAMO = 'CREDITO'
    ;
    -- SELECT C.*,'CASO 1.2' FROM EXT.CARTERA C  INNER JOIN (
    -- 	SELECT DISTINCT CT.NUM_POLIZA
	--     	FROM EXT.CARTERA CT INNER JOIN EXT.RELASUJE_20241023 R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
	--     	WHERE RAMO = 'CREDITO'
	--     	GROUP BY CT.NUM_POLIZA,CT.ACTIVO
	-- 	    HAVING COUNT(DISTINCT CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE) = 1
	--     	) C2 
	-- ON C.NUM_POLIZA = C2.NUM_POLIZA;
	    
	UPDATE CO
	SET FECHA_INICIO = COALESCE(R.FEC_INI, '1990-12-31')
    , FECHA_FIN = COALESCE(R.FEC_FIN,'2020-01-01')
    , MODIF_CASE = MODIF_CASE || ' UPDATE FECHA 1.3'
	FROM EXT.CARTERA_OBJ CO
	JOIN EXT.RELASUJE_20241023 R 
	    ON LPAD(CO.NUM_POLIZA,8,0) = R.NUM_POLIZA 
	    AND CO.COD_MEDIADOR || '-' || CO.COD_SUBCLAVE = R.POSITIONNAME
	WHERE CO.RAMO = 'CREDITO'
    AND R.POSITIONNAME IS NOT NULL
	  AND CO.COD_MEDIADOR || '-' || CO.COD_SUBCLAVE IN (
	      SELECT COD_MEDIADOR || '-' || COD_SUBCLAVE
	      FROM EXT.CARTERA_OBJ CO2
	      WHERE CO2.NUM_POLIZA = CO.NUM_POLIZA
	      GROUP BY COD_MEDIADOR || '-' || COD_SUBCLAVE
	      HAVING COUNT(DISTINCT COD_MEDIADOR || '-' || COD_SUBCLAVE) = 1
	  );
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'CASO 1.3 Insertados ' || ::ROWCOUNT || ' registros' , cReport, io_contador);
    -----------------------------------------------------------------------------------------
    /****************************  POLIZAS CAUCIÓN *****************************************/
    -----------------------------------------------------------------------------------------

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);
END