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
	| Version: 2.0 -- Cambios directamente en CARTERA
	| Version: 2.1 -- Cálculo fechas inicio y fin de operaciones especiales en CREDITO
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
    DECLARE cVersion  CONSTANT VARCHAR(3) :='02';
    DECLARE cEsquema CONSTANT VARCHAR(3) := 'EXT';
    
    DECLARE caso1 varchar(100)  := 'CASO 1 POLIZA CREDITO, UN SOLO MEDIADOR SIN TRASPASOS';    

    DECLARE caso2 varchar(100)  := 'CASO 2 POLIZA CREDITO, N MEDIADORES SIN TRASPASOS';    

    DECLARE caso3 varchar(100)  := 'CASO 3 POLIZA CREDITO, N MEDIADORES CON TRASPASOS';
    
    
    DECLARE caso4 varchar(100)  := 'CASO 4 POLIZA CAUCIÓN, UN SOLO MEDIADOR SIN TRASPASOS';
    

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
    -- CREAR BACKUP
    -----------------------------------------------------------------------------------------

    IF (SELECT TABLE_NAME FROM SYS.TABLES where SCHEMA_NAME= cEsquema and TABLE_NAME = 'CARTERA_BKP_012025_FECHAS') IS NULL THEN
	    
        CREATE COLUMN TABLE EXT.CARTERA_BKP_012025_FECHAS AS (SELECT * FROM EXT.CARTERA)
        UNLOAD PRIORITY 5 AUTO MERGE;

        CALL EXT.LIB_GLOBAL_CESCE :w_debug (
            i_Tenant,
            'CREADA TABLA ' || cEsquema || '.' || 'CARTERA_BKP_012025_FECHAS',
            'cReport',
            io_contador
        );    
    END IF;
    
    

    --TRUNCATE TABLE EXT.CARTERA_OBJ_TEMP;

    -----------------------------------------------------------------------------------------
    /****************************  POLIZAS CRÉDITO *****************************************/
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Tratamiento POLIZAS CRÉDITO', CReport, io_contador);


          
    -----------------------------------------------------------------------------------------
    -- CASO 1 POLIZA CREDITO, UN SOLO MEDIADOR SIN TRASPASOS
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso1 , CReport, io_contador);
  
	UPDATE CT
	SET CT.FECHA_INICIO = CASE
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		SELECT CASE WHEN FEC_INI IS NULL OR FEC_INI = '0000-00-00' THEN '1990-12-31'ELSE FEC_INI END FROM EXT.RELASUJE R
		 		WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
		 	)
		 	ELSE CT.FECHA_INICIO
		 END 
    , CT.FECHA_FIN = CASE 
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		SELECT CASE WHEN FEC_FIN IS NULL OR FEC_FIN = '0000-00-00' THEN '2200-01-01'ELSE FEC_FIN END FROM EXT.RELASUJE R
		 		WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
		 	)
		 	ELSE CT.FECHA_FIN
      END 
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE) = 1
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    LEFT JOIN EXT.RELASUJE R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
    WHERE CT.RAMO = 'CREDITO'
    ;	

    ---------------------------------------------------------------------------------------------------
    -- Obtener registros insertados para debug
    ---------------------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || 'REGISTROS ACTUALIZADOS' || '   ' || TO_VARCHAR(::ROWCOUNT) , cReport, io_contador);
    ---------------------------------------------------------------------------------------------------
    

	
    --------------------------------------------------------------------------------------------
    -- Caso 2 POLIZA CREDITO, N MEDIADORES SIN TRASPASOS
    --------------------------------------------------------------------------------------------

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso2 , CReport, io_contador);
  
    UPDATE CT
    SET FECHA_INICIO = CASE
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
                AND LPAD(CT.IDMODALIDAD,3,0) = R.MOD
        	) THEN (
		 		 SELECT CASE WHEN MAX(FEC_INI) IS NULL OR MAX(FEC_INI) = '0000-00-00' THEN '1990-12-31'ELSE MAX(FEC_INI) END
                 FROM EXT.RELASUJE R
		 		 WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
                 AND LPAD(CT.IDMODALIDAD,3,0) = R.MOD
		 	)
		 	ELSE CT.FECHA_INICIO
      END 
    , FECHA_FIN = CASE
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		SELECT CASE WHEN MAX(FEC_FIN) IS NULL OR MAX(FEC_FIN) = '0000-00-00' THEN '2200-01-01'ELSE MAX(FEC_FIN) END FROM EXT.RELASUJE R
		 		WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
		 	)
		 	ELSE CT.FECHA_FIN
      END 
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA
        HAVING (COUNT(DISTINCT C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE) > 1 AND COUNT(DISTINCT C.ACTIVO) = 1) OR (COUNT(DISTINCT C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE) > 1 AND COUNT(DISTINCT C.ACTIVO) > 1)
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    LEFT JOIN EXT.RELASUJE R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
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
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || 'REGISTROS ACTUALIZADOS' || '   ' || TO_VARCHAR(::ROWCOUNT) , cReport, io_contador);
    ---------------------------------------------------------------------------------------------------

    

    -- --------------------------------------------------------------------------------------------
    -- -- Caso 3 POLIZA CREDITO, N MEDIADORES CON TRASPASOS
    -- --------------------------------------------------------------------------------------------

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso3 , CReport, io_contador);
    

    UPDATE CT
    SET FECHA_INICIO = CASE
	    WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
                AND LPAD(CT.IDMODALIDAD,3,0) = R.MOD
        	) THEN (
		 		  SELECT CASE WHEN MAX(FEC_INI) IS NULL OR MAX(FEC_INI) = '0000-00-00' THEN '1990-12-31'ELSE MAX(FEC_INI) END
                  FROM EXT.RELASUJE R
		 		  WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
                  AND LPAD(CT.IDMODALIDAD,3,0) = R.MOD                
		 	)
		 	ELSE CT.FECHA_INICIO
        END 
    , FECHA_FIN = CASE   
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		 SELECT CASE WHEN MAX(FEC_FIN) IS NULL OR MAX(FEC_FIN) = '0000-00-00' THEN '2200-01-01'ELSE MAX(FEC_FIN) END FROM EXT.RELASUJE R
		 		 WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME                
		 	)
		 	ELSE CT.FECHA_FIN
      END     
    
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE) > 1 AND COUNT(DISTINCT C.ACTIVO) > 1
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
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || 'REGISTROS ACTUALIZADOS' || '   ' || TO_VARCHAR(::ROWCOUNT) , cReport, io_contador);
    ---------------------------------------------------------------------------------------------------

    -----------------------------------------------------------------------------------------
    /****************************  POLIZAS CAUCIÓN *****************************************/
    -----------------------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------------------
    -- CASO 4 POLIZA CAUCIÓN, UN SOLO MEDIADOR SIN TRASPASOS
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso4 , CReport, io_contador);
    

    UPDATE CT
    SET FECHA_INICIO = CASE
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		SELECT CASE WHEN MAX(FEC_INI) IS NULL OR MAX(FEC_INI) = '0000-00-00' THEN '1990-12-31'ELSE MAX(FEC_INI) END FROM EXT.RELASUJE R
		 		WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
		 	)
		 	ELSE CT.FECHA_INICIO
      END 
    , FECHA_FIN = CASE 
		WHEN EXISTS(
        		SELECT 1 FROM EXT.RELASUJE R
				WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
        	) THEN (
		 		SELECT CASE WHEN MAX(FEC_FIN) IS NULL OR MAX(FEC_FIN) = '0000-00-00' THEN '2200-01-01'ELSE MAX(FEC_FIN) END FROM EXT.RELASUJE R
		 		WHERE LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA AND R.POSITIONNAME IS NOT NULL AND CT.COD_MEDIADOR||'-'||CT.COD_SUBCLAVE = R.POSITIONNAME
		 	)
		 	ELSE CT.FECHA_FIN
      END 
    FROM EXT.CARTERA CT INNER JOIN (
        SELECT C.NUM_POLIZA
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CAUCION'
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE) = 1
    ) C2 ON CT.NUM_POLIZA = C2.NUM_POLIZA
    LEFT JOIN EXT.RELASUJE R ON LPAD(CT.NUM_POLIZA,8,0) = R.NUM_POLIZA
    WHERE CT.RAMO = 'CAUCION'
    ;		

    ---------------------------------------------------------------------------------------------------
    -- Obtener registros insertados para debug
    ---------------------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || 'REGISTROS ACTUALIZADOS' || '   ' || TO_VARCHAR(::ROWCOUNT) , cReport, io_contador);
    ---------------------------------------------------------------------------------------------------

--     INSERT INTO EXT.CARTERA(RAMO
-- 	, NUM_POLIZA
-- 	, IDPRODUCT
-- 	, IDMODALIDAD
-- 	, NUM_ANUALIDAD
-- 	, FECHA_INICIO
-- 	, FECHA_FIN
-- 	, IDPAIS
-- 	, MODIF_USER)
-- 	SELECT RAMO
-- 		, NUM_POLIZA
--     	, '' IDPRODUCT
-- 		, IDMODALIDAD
--     	, 0 NUM_ANUALIDAD
-- 		, CASE WHEN 1 = (SELECT DISTINCT 1 FROM EXT.CARTERA C
-- 		    				INNER JOIN EXT.RELASUJE R ON 
-- 		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
-- 		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
-- 		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
-- 		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA
-- 	    				)-- EXISTE
-- 	    				THEN (SELECT DISTINCT CASE WHEN MIN(FEC_INI) IS NULL OR MIN(FEC_INI) = '0000-00-00' THEN '1990-12-31'ELSE MIN(FEC_INI) END FROM EXT.CARTERA C
-- 		    				INNER JOIN EXT.RELASUJE R ON 
-- 		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
-- 		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
-- 		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
-- 		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA)
-- 		    			 --NO EXISTE 
-- 	    				ELSE MIN(CT.FECHA_INICIO)
-- 	    	END FECHA_INICIO
-- 	        , CASE WHEN 1 = (SELECT DISTINCT 1 FROM EXT.CARTERA C
-- 		    				INNER JOIN EXT.RELASUJE R ON 
-- 		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
-- 		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
-- 		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
-- 		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA
-- 	    				)-- EXISTE
-- 	    				THEN (SELECT DISTINCT CASE WHEN MAX(FEC_FIN) IS NULL OR MAX(FEC_FIN) = '0000-00-00' THEN '2200-01-01'ELSE MAX(FEC_FIN) END FROM EXT.CARTERA C
-- 		    				INNER JOIN EXT.RELASUJE R ON 
-- 		    				LPAD(C.NUM_POLIZA,8,0) = R.NUM_POLIZA 
-- 		    				AND LPAD(C.IDMODALIDAD,3,0) = R.MOD
-- 		    				AND C.COD_MEDIADOR || '-' || C.COD_SUBCLAVE = R.POSITIONNAME
-- 		    			  WHERE C.NUM_POLIZA = CT.NUM_POLIZA)
-- 		    			 --NO EXISTE 
-- 	    				ELSE MAX(CT.FECHA_FIN)
-- 	    	END FECHA_FIN
-- 		, IDPAIS
--         , 'MANUAL-2'
-- 	FROM EXT.CARTERA CT
-- 	WHERE RAMO = 'CAUCION' 
-- 	GROUP BY RAMO
-- 	 , NUM_POLIZA
-- 	 , IDMODALIDAD
-- 	 , IDPAIS
     
-- ;

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'EXPEDIENTE' || ' Insertados EXPEDIENTES ' || ::ROWCOUNT || ' registros', cReport, io_contador);
	
	-----------------------------------------------------------------------------------------
    /***********************  ACTUALIZACIÓN FECHA_INICIO_OPESP *****************************/
    -----------------------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------------------
    -- CREDITO
    -----------------------------------------------------------------------------------------
	UPDATE CT
	SET FECHA_INICIO_OPESP = 
		CASE 
			WHEN P_ESPECIAL_EMISION IS NOT NULL OR P_ESPECIAL_RENOVACION IS NOT NULL 
			THEN FECHA_EFECTO 
		END,
	FECHA_FIN_OPESP =
		CASE
			WHEN P_ESPECIAL_EMISION IS NOT NULL AND P_ESPECIAL_RENOVACION IS NOT NULL 
				AND NOT EXISTS(
					SELECT 1 FROM EXT.CARTERA C WHERE C.NUM_POLIZA = CT.NUM_POLIZA 
						AND C.NUM_ANUALIDAD = CT.NUM_ANUALIDAD + 1 
						AND C.COD_MEDIADOR = CT.COD_MEDIADOR
						AND P_ESPECIAL_EMISION IS NOT NULL AND P_ESPECIAL_RENOVACION IS NOT NULL 
					)
		THEN FECHA_VENCIMIENTO 
		END 
	FROM EXT.CARTERA CT
	WHERE RAMO = 'CREDITO' 
	;
	-- UPDATE CT
	-- SET FECHA_INICIO_OPESP =
	-- 	CASE
	-- 		WHEN P_ESPECIAL_EMISION IS NOT NULL OR P_ESPECIAL_RENOVACION IS NOT NULL
	-- 		THEN FECHA_EFECTO
	-- 	END,
	-- 	FECHA_FIN_OPESP =
	-- 	 (
	-- 	    SELECT C.FECHA_VENCIMIENTO
	-- 		FROM
	-- 		EXT.CARTERA C LEFT JOIN
	-- 		(
	-- 	        SELECT 
	-- 	            NUM_POLIZA,
	-- 	            COD_MEDIADOR,
	-- 	            FECHA_INICIO,
	-- 	            FECHA_VENCIMIENTO,
	-- 	            CREATEDATE,
	-- 	            ROW_NUMBER() OVER (
	-- 	                PARTITION BY NUM_POLIZA, COD_MEDIADOR
	-- 	                ORDER BY FECHA_INICIO DESC
	-- 	            ) AS RANK_DESC
	-- 	        FROM EXT.CARTERA
	-- 	        WHERE (P_ESPECIAL_EMISION IS NOT NULL OR P_ESPECIAL_RENOVACION IS NOT NULL)
	-- 	        AND RAMO = 'CAUCION'
	-- 	       -- AND NUM_POLIZA = 1001196
	-- 		) P ON C.NUM_POLIZA = P.NUM_POLIZA
	-- 		AND COALESCE(C.COD_MEDIADOR,'') = COALESCE(P.COD_MEDIADOR,'')
	-- 	     AND C.FECHA_INICIO = P.FECHA_INICIO
	-- 	     AND C.CREATEDATE = P.CREATEDATE
	-- 		WHERE 1=1 --AND C.NUM_POLIZA = 1001196
	-- 		AND C.RAMO = 'CAUCION'
	-- 		ORDER BY C.NUM_POLIZA, C.COD_MEDIADOR, P.RANK_DESC DESC, C.FECHA_INICIO );
	
	UPDATE EXT.CARTERA
	SET FECHA_INICIO_OPESP =
		CASE 
			WHEN (P_ESPECIAL_EMISION IS NOT NULL OR P_ESPECIAL_RENOVACION IS NOT NULL)
			THEN '2025-10-10' 
		END,
 FECHA_FIN_OPESP = (
    SELECT FECHA_VENCIMIENTO
    FROM (
        SELECT 
            NUM_POLIZA,
            COD_MEDIADOR,
            FECHA_INICIO,
            FECHA_VENCIMIENTO,
            CREATEDATE,
            ROW_NUMBER() OVER (
                PARTITION BY NUM_POLIZA, COD_MEDIADOR
                ORDER BY FECHA_INICIO,CREATEDATE DESC
            ) AS RANK_DESC
        FROM EXT.CARTERA C
        WHERE (P_ESPECIAL_EMISION IS NOT NULL OR P_ESPECIAL_RENOVACION IS NOT NULL)
        AND RAMO = 'CAUCION'
    ) AS POLIZAS_CTE
    WHERE POLIZAS_CTE.NUM_POLIZA = EXT.CARTERA.NUM_POLIZA
      AND COALESCE(POLIZAS_CTE.COD_MEDIADOR, '') = COALESCE(EXT.CARTERA.COD_MEDIADOR, '')
      AND POLIZAS_CTE.FECHA_INICIO = EXT.CARTERA.FECHA_INICIO
      AND POLIZAS_CTE.RANK_DESC = 1
      AND POLIZAS_CTE.CREATEDATE = EXT.CARTERA.CREATEDATE
)
-- WHERE EXISTS (
--     SELECT 1
--     FROM (
--         SELECT 
--             NUM_POLIZA,
--             COD_MEDIADOR,
--             FECHA_INICIO,
--             CREATEDATE,
--             ROW_NUMBER() OVER (
--                 PARTITION BY NUM_POLIZA, COD_MEDIADOR
--                 ORDER BY FECHA_INICIO DESC
--             ) AS RANK_DESC
--         FROM EXT.CARTERA
--         WHERE (P_ESPECIAL_EMISION IS NOT NULL OR P_ESPECIAL_RENOVACION IS NOT NULL) AND RAMO = 'CAUCION'
--     ) AS POLIZAS_CTE
--     WHERE POLIZAS_CTE.NUM_POLIZA = EXT.CARTERA.NUM_POLIZA
--       AND COALESCE(POLIZAS_CTE.COD_MEDIADOR, '') = COALESCE(EXT.CARTERA.COD_MEDIADOR, '')
--       AND POLIZAS_CTE.FECHA_INICIO = EXT.CARTERA.FECHA_INICIO
--       AND POLIZAS_CTE.RANK_DESC = 1
--       AND POLIZAS_CTE.CREATEDATE = EXT.CARTERA.CREATEDATE
-- )
;

	
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FECHAS ESPECIALES CREDITO' || ' ACTUALIZADOS ' || ::ROWCOUNT || ' REGITROS', cReport, io_contador);
	-----------------------------------------------------------------------------------------
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);
	END