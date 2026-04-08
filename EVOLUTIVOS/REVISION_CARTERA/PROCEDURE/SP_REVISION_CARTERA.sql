CREATE or replace PROCEDURE EXT.SP_REVISION_CARTERA LANGUAGE SQLSCRIPT AS

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
    | Versión: 3. Cálculos en tablas temporales
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
    DECLARE cReport CONSTANT VARCHAR(50) := ::CURRENT_OBJECT_NAME;
    DECLARE cVersion  CONSTANT VARCHAR(3) :='03';
    DECLARE cEsquema CONSTANT VARCHAR(3) := ::CURRENT_OBJECT_SCHEMA;
    
    DECLARE caso1 varchar(100)  := 'CASO 1 POLIZA CREDITO, UN SOLO MEDIADOR SIN TRASPASOS';    

    DECLARE caso2 varchar(100)  := 'CASO 2 POLIZA CREDITO, N MEDIADORES SIN TRASPASOS';    

    DECLARE caso3 varchar(100)  := 'CASO 3 POLIZA CREDITO, N MEDIADORES CON TRASPASOS';
    
    
    DECLARE caso4 varchar(100)  := 'CASO 4 POLIZA CAUCIÓN, UN SOLO MEDIADOR SIN TRASPASOS';
    

    DECLARE CTE_RANK TABLE  (
        NUM_POLIZA NVARCHAR(50),
        COD_MEDIADOR NVARCHAR(50),
        COD_SUBCLAVE NVARCHAR(50),
        NUM_ANUALIDAD INTEGER,
        RAMO VARCHAR(25),
        RN INTEGER
    );

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

    IF (SELECT TABLE_NAME FROM SYS.TABLES where SCHEMA_NAME= cEsquema and TABLE_NAME = 'CARTERA_BKP_'||TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')||'_CAMBIO_CARTERA') IS NULL THEN
	    
        
        EXEC('CREATE COLUMN TABLE EXT.CARTERA_BKP_'||TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')||'_CAMBIO_CARTERA AS (SELECT * FROM EXT.CARTERA)');
        --UNLOAD PRIORITY 5 AUTO MERGE;

        CALL EXT.LIB_GLOBAL_CESCE :w_debug (
            i_Tenant,
            'CREADA TABLA ' || cEsquema || '.' || 'CARTERA_BKP_'||TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')||'_CAMBIO_CARTERA',
            'cReport',
            io_contador
        );    
    END IF;
    
    

    --INICIALIZAR TABLAS TEMPORALES 
    --Ranking para cada combinación relevante
    INSERT INTO :CTE_RANK(NUM_POLIZA,COD_MEDIADOR,COD_SUBCLAVE,RN)
        WITH grupos_ordenados AS (
            SELECT 
                NUM_POLIZA,
                COD_MEDIADOR,
                COD_SUBCLAVE,
                MIN(NUM_ANUALIDAD) AS MIN_ANUALIDAD
            FROM EXT.CARTERA            
            GROUP BY NUM_POLIZA, COD_MEDIADOR, COD_SUBCLAVE
        ),
        grupos_rango AS (
            SELECT 
                NUM_POLIZA,
                COD_MEDIADOR,
                COD_SUBCLAVE,
                MIN_ANUALIDAD,
                DENSE_RANK() OVER (
                    PARTITION BY NUM_POLIZA 
                    ORDER BY MIN_ANUALIDAD, COD_MEDIADOR
                ) AS RN
            FROM grupos_ordenados
        )
        SELECT 
            a.NUM_POLIZA,
            a.COD_MEDIADOR,
            a.COD_SUBCLAVE,
            g.RN
        FROM EXT.CARTERA a
        JOIN grupos_rango g
        ON a.NUM_POLIZA = g.NUM_POLIZA
        AND a.COD_MEDIADOR = g.COD_MEDIADOR
        AND a.COD_SUBCLAVE = g.COD_SUBCLAVE
       ;
	    
	-- CTE: FECHAS INICIO y FIN desde RELASUJE
	CTE_RELASUJE =
	    SELECT
	        R.NUM_POLIZA,
	        R.POSITIONNAME,
	        R.MOD,
	        MAX(IFNULL(FEC_INI, '1990-12-31')) AS MAX_FEC_INI,
	        MAX(IFNULL(CASE WHEN FEC_FIN = '0000-00-00' THEN '2200-01-01' END, '2200-01-01')) AS MAX_FEC_FIN
	    FROM EXT.RELASUJE R
	    WHERE R.POSITIONNAME IS NOT NULL
	    GROUP BY R.NUM_POLIZA, R.POSITIONNAME, R.MOD;
	 
	   
	--CASOS TIPO 1   
	CTE_MODIF_CASE_1 = SELECT C.NUM_POLIZA
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE) = 1;
        
        
    --CASOS TIPO2
    CTE_MODIF_CASE_2 = SELECT C.NUM_POLIZA
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA
        HAVING (COUNT(DISTINCT C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE) > 1 AND COUNT(DISTINCT C.ACTIVO) = 1) 
        --OR (COUNT(DISTINCT C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE) > 1 AND COUNT(DISTINCT C.ACTIVO) > 1)
		;
		
	--CASOS TIPO3
    CTE_MODIF_CASE_3 = SELECT C.NUM_POLIZA
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CREDITO'
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE) > 1 AND COUNT(DISTINCT C.ACTIVO) > 1
    ;
    
    --CASOS TIPO4
    CTE_MODIF_CASE_4 = SELECT C.NUM_POLIZA
        FROM EXT.CARTERA C
        WHERE C.RAMO = 'CAUCION'
        GROUP BY C.NUM_POLIZA
        HAVING COUNT(DISTINCT C.COD_MEDIADOR||'-'||C.COD_SUBCLAVE) = 1
        ;

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
    	WHEN RJ.NUM_POLIZA IS NULL 
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD <> LPAD(CT.IDMODALIDAD,3,0)
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME IS NULL
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE --AND CR.RN < (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN (SELECT MIN(FECHA_EFECTO) FROM EXT.CARTERA WHERE NUM_POLIZA = CT.NUM_POLIZA AND COD_MEDIADOR = CT.COD_MEDIADOR AND COD_SUBCLAVE = CT.COD_SUBCLAVE)
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME = CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE 
    		THEN RJ.MAX_FEC_INI
    	ELSE 
    		'1990-12-31'
	END
    , CT.FECHA_FIN = CASE
		WHEN RJ.NUM_POLIZA IS NULL 
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD <> LPAD(CT.IDMODALIDAD,3,0)
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME IS NULL
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE AND CR.RN = (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE --AND CR.RN < (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN (SELECT MAX(FECHA_VENCIMIENTO) FROM EXT.CARTERA WHERE NUM_POLIZA = CT.NUM_POLIZA AND COD_MEDIADOR = CT.COD_MEDIADOR AND COD_SUBCLAVE = CT.COD_SUBCLAVE)
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME = CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE 
    		THEN RJ.MAX_FEC_FIN	
    	ELSE 
    		'2200-01-01'
	END
    FROM EXT.CARTERA CT
    INNER JOIN :CTE_MODIF_CASE_1 CMC1 ON CT.NUM_POLIZA = CMC1.NUM_POLIZA
    LEFT JOIN :CTE_RELASUJE RJ ON LPAD(CT.NUM_POLIZA,8,0) = RJ.NUM_POLIZA AND LPAD(CT.IDMODALIDAD,3) = RJ.MOD
    LEFT JOIN :CTE_RANK CR ON CT.NUM_POLIZA = CR.NUM_POLIZA 
	    AND CT.COD_MEDIADOR = CR.COD_MEDIADOR 
	    AND CT.COD_SUBCLAVE = CR.COD_SUBCLAVE 
	    AND CT.RAMO = CR.RAMO 
	    AND CT.NUM_ANUALIDAD = CR.NUM_ANUALIDAD
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
    	WHEN RJ.NUM_POLIZA IS NULL 
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD <> LPAD(CT.IDMODALIDAD,3,0)
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME IS NULL
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE --AND CR.RN < (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN (SELECT MIN(FECHA_EFECTO) FROM EXT.CARTERA WHERE NUM_POLIZA = CT.NUM_POLIZA AND COD_MEDIADOR = CT.COD_MEDIADOR AND COD_SUBCLAVE = CT.COD_SUBCLAVE)
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME = CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE 
    		THEN RJ.MAX_FEC_INI
    	ELSE 
    		'1990-12-31'
	END
    , FECHA_FIN = CASE
		WHEN RJ.NUM_POLIZA IS NULL 
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD <> LPAD(CT.IDMODALIDAD,3,0)
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME IS NULL
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE AND CR.RN = (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE --AND CR.RN < (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN (SELECT MAX(FECHA_VENCIMIENTO) FROM EXT.CARTERA WHERE NUM_POLIZA = CT.NUM_POLIZA AND COD_MEDIADOR = CT.COD_MEDIADOR AND COD_SUBCLAVE = CT.COD_SUBCLAVE)
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME = CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE 
    		THEN RJ.MAX_FEC_FIN	
    	ELSE 
    		'2200-01-01'
	END
    FROM EXT.CARTERA CT
    INNER JOIN :CTE_MODIF_CASE_2 CMC2 ON CT.NUM_POLIZA = CMC2.NUM_POLIZA
    LEFT JOIN :CTE_RELASUJE RJ ON LPAD(CT.NUM_POLIZA,8,0) = RJ.NUM_POLIZA 
    LEFT JOIN :CTE_RANK CR ON CT.NUM_POLIZA = CR.NUM_POLIZA 
	   AND CT.COD_MEDIADOR = CR.COD_MEDIADOR 
	   AND CT.COD_SUBCLAVE = CR.COD_SUBCLAVE 
	   AND CT.RAMO = CR.RAMO 
	   AND CT.NUM_ANUALIDAD = CR.NUM_ANUALIDAD
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
    	WHEN RJ.NUM_POLIZA IS NULL 
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD <> LPAD(CT.IDMODALIDAD,3,0)
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME IS NULL
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE --AND CR.RN < (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN (SELECT MIN(FECHA_EFECTO) FROM EXT.CARTERA WHERE NUM_POLIZA = CT.NUM_POLIZA AND COD_MEDIADOR = CT.COD_MEDIADOR AND COD_SUBCLAVE = CT.COD_SUBCLAVE)
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME = CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE 
    		THEN RJ.MAX_FEC_INI
    	ELSE 
    		'1990-12-31'
	
	END
    , FECHA_FIN = CASE
		WHEN RJ.NUM_POLIZA IS NULL 
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD <> LPAD(CT.IDMODALIDAD,3,0)
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME IS NULL
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE AND CR.RN = (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE --AND CR.RN < (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN (SELECT MAX(FECHA_VENCIMIENTO) FROM EXT.CARTERA WHERE NUM_POLIZA = CT.NUM_POLIZA AND COD_MEDIADOR = CT.COD_MEDIADOR AND COD_SUBCLAVE = CT.COD_SUBCLAVE)
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME = CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE 
    		THEN RJ.MAX_FEC_FIN
    	ELSE 
    		'2200-01-01'
		
	END
    FROM EXT.CARTERA CT
    INNER JOIN :CTE_MODIF_CASE_3 CMC3 ON CT.NUM_POLIZA = CMC3.NUM_POLIZA
    LEFT JOIN :CTE_RELASUJE RJ ON LPAD(CT.NUM_POLIZA,8,0) = RJ.NUM_POLIZA
    LEFT JOIN :CTE_RANK CR ON CT.NUM_POLIZA = CR.NUM_POLIZA 
	    AND CT.COD_MEDIADOR = CR.COD_MEDIADOR 
	    AND CT.COD_SUBCLAVE = CR.COD_SUBCLAVE 
	    AND CT.RAMO = CR.RAMO 
	    AND CT.NUM_ANUALIDAD = CR.NUM_ANUALIDAD
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

    -----------------------------------------------------------------------------------------
    /****************************  POLIZAS CAUCIÓN *****************************************/
    -----------------------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------------------
    -- CASO 4 POLIZA CAUCIÓN, UN SOLO MEDIADOR SIN TRASPASOS
    -----------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, caso4 , CReport, io_contador);
    
    ---------------------------------------------------------------------------------------------------
    -- INSERTAR EXPEDIENTES
    ---------------------------------------------------------------------------------------------------
    --CALL EXT.SP_CREAR_EXPEDIENTE();

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'EXPEDIENTE' || ' Insertados EXPEDIENTES ' || ::ROWCOUNT || ' registros', cReport, io_contador);
	
/*
    UPDATE CT
    SET FECHA_INICIO = CASE
    	WHEN RJ.NUM_POLIZA IS NULL 
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD <> LPAD(CT.IDMODALIDAD,3,0)
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME IS NULL
    		THEN '1990-12-31'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE --AND CR.RN < (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN (SELECT MIN(FECHA_EFECTO) FROM EXT.CARTERA WHERE NUM_POLIZA = CT.NUM_POLIZA AND COD_MEDIADOR = CT.COD_MEDIADOR AND COD_SUBCLAVE = CT.COD_SUBCLAVE)
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME = CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE 
    		THEN RJ.MAX_FEC_INI
    	ELSE 
    		'1990-12-31'
	
	END
    , FECHA_FIN = CASE
		WHEN RJ.NUM_POLIZA IS NULL 
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD <> LPAD(CT.IDMODALIDAD,3,0)
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME IS NULL
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE AND CR.RN = (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN '2200-01-01'
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME <> CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE --AND CR.RN < (SELECT MAX(RN) FROM :CTE_RANK WHERE NUM_POLIZA = CT.NUM_POLIZA)
    		THEN (SELECT MAX(FECHA_VENCIMIENTO) FROM EXT.CARTERA WHERE NUM_POLIZA = CT.NUM_POLIZA AND COD_MEDIADOR = CT.COD_MEDIADOR AND COD_SUBCLAVE = CT.COD_SUBCLAVE)
    	WHEN RJ.NUM_POLIZA IS NOT NULL AND RJ.MOD = LPAD(CT.IDMODALIDAD,3,0) AND RJ.POSITIONNAME = CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE 
    		THEN RJ.MAX_FEC_FIN
    	ELSE 
    		'2200-01-01'
	
	END
    FROM EXT.CARTERA CT 
    INNER JOIN :CTE_MODIF_CASE_4 CMC4 ON CT.NUM_POLIZA = CMC4.NUM_POLIZA
    LEFT JOIN :CTE_RELASUJE RJ ON LPAD(CT.NUM_POLIZA,8,0) = RJ.NUM_POLIZA
	LEFT JOIN :CTE_RANK CR ON CT.NUM_POLIZA = CR.NUM_POLIZA 
	    AND CT.COD_MEDIADOR = CR.COD_MEDIADOR 
	    AND CT.COD_SUBCLAVE = CR.COD_SUBCLAVE 
	    AND CT.RAMO = CR.RAMO 
	    AND CT.NUM_ANUALIDAD = CR.NUM_ANUALIDAD
    WHERE CT.RAMO = 'CAUCION'
    ;		

    ---------------------------------------------------------------------------------------------------
    -- Obtener registros insertados para debug
    ---------------------------------------------------------------------------------------------------
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,'     ' || 'REGISTROS ACTUALIZADOS' || '   ' || TO_VARCHAR(::ROWCOUNT) , cReport, io_contador);
    ---------------------------------------------------------------------------------------------------
*/	
	-----------------------------------------------------------------------------------------
    /***********************  ACTUALIZACIÓN FECHA_INICIO_OPESP *****************************/
    -----------------------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------------------
    -- CREDITO
    -----------------------------------------------------------------------------------------
	UPDATE CT
SET 
    FECHA_INICIO_OPESP = 
        CASE 
            WHEN CT.P_ESPECIAL_EMISION IS NOT NULL OR CT.P_ESPECIAL_RENOVACION IS NOT NULL 
            THEN FECHA_EFECTO 
        END,
    FECHA_FIN_OPESP = CASE 
	        	WHEN EXISTS(SELECT NUM_ANUALIDAD FROM EXT.CARTERA C WHERE CT.NUM_POLIZA = C.NUM_POLIZA
	        		AND CT.COD_MEDIADOR = C.COD_MEDIADOR AND C.NUM_ANUALIDAD = CT.NUM_ANUALIDAD + 1
	        		AND EXISTS (SELECT 1 FROM EXT.CARTERA C1 WHERE C1.NUM_POLIZA = C.NUM_POLIZA
	        			AND C1.COD_MEDIADOR = C.COD_MEDIADOR AND C1.NUM_ANUALIDAD = C.NUM_ANUALIDAD 
	        			AND (C1.P_ESPECIAL_EMISION IS  NULL OR C1.P_ESPECIAL_RENOVACION IS  NULL)
	        			AND C1.RAMO = 'CREDITO'
	        		)
	        	) AND (CT.P_ESPECIAL_EMISION IS NOT NULL OR CT.P_ESPECIAL_RENOVACION IS NOT NULL) AND CT.RAMO = 'CREDITO'
	        	THEN FECHA_VENCIMIENTO
	        	
	        END
    -- FECHA_FIN_OPESP =
    --     CASE
    --         WHEN P_ESPECIAL_EMISION IS NOT NULL 
    --              AND P_ESPECIAL_RENOVACION IS NOT NULL 
    --              AND NOT EXISTS (
    --                  SELECT 1 
    --                  FROM EXT.CARTERA C 
    --                  WHERE C.NUM_POLIZA = EXT.CARTERA.NUM_POLIZA 
    --                      AND C.NUM_ANUALIDAD = EXT.CARTERA.NUM_ANUALIDAD + 1 
    --                      AND C.COD_MEDIADOR = EXT.CARTERA.COD_MEDIADOR
    --                      AND C.P_ESPECIAL_EMISION IS NOT NULL 
    --                      AND C.P_ESPECIAL_RENOVACION IS NOT NULL 
    --                      AND C.RAMO = 'CREDITO'
    --              )
    --         THEN FECHA_VENCIMIENTO 
    --     END
    FROM EXT.CARTERA CT
WHERE RAMO = 'CREDITO' 
	;
	
	
	UPDATE EXT.CARTERA
	SET FECHA_INICIO_OPESP =
		CASE 
			WHEN (P_ESPECIAL_EMISION IS NOT NULL OR P_ESPECIAL_RENOVACION IS NOT NULL)
			THEN FECHA_EFECTO
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
	      AND EXT.CARTERA.RAMO = 'CAUCION'
	)
	WHERE EXT.CARTERA.RAMO = 'CAUCION'
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
	END;
	
	