CREATE OR REPLACE PROCEDURE EXT.SP_REVISION_CARTERA_V2 LANGUAGE SQLSCRIPT AS
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
    DECLARE v_indice Number := 1;
    DECLARE v_total_filas Number := 0;
    DECLARE v_NUM_POLIZA Number;
    DECLARE v_COD_MEDIADOR VARCHAR(10);
    DECLARE v_COD_SUBCLAVE VARCHAR(10);
    

    -- Constantes
    DECLARE cReport CONSTANT VARCHAR(50) := 'SP_REVISION_CARTERA';
    DECLARE cVersion  CONSTANT VARCHAR(3) :='01';
    DECLARE cEsquema  CONSTANT VARCHAR(3) :='EXT';

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
	    CREATE COLUMN TABLE EXT.CARTERA_OBJ_TEMP (
            NUM_POLIZA BIGINT CS_FIXED NOT NULL,
            COD_MEDIADOR NVARCHAR(10),
            COD_SUBCLAVE NVARCHAR(10)
        )UNLOAD PRIORITY 5 AUTO MERGE;

        CALL EXT.LIB_GLOBAL_CESCE :w_debug (
            i_Tenant,
            'CREADA TABLA ' || cEsquema || '.' || 'CARTERA_OBJ_TEMP',
            cReport,
            io_contador
        );
    END IF;

    TRUNCATE TABLE EXT.CARTERA_OBJ_TEMP;

    INSERT INTO EXT.CARTERA_OBJ_TEMP
    SELECT DISTINCT NUM_POLIZA, COD_MEDIADOR, COD_SUBCLAVE FROM EXT.CARTERA WHERE RAMO = 'CREDITO';
    
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, ::ROWCOUNT || ' INSERTADOS EN LA TABLA TEMPORAL', CReport, io_contador);

    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, ' RECORREMOS TABLA ', CReport, io_contador);
    
    SELECT COUNT(*) INTO v_total_filas FROM EXT.CARTERA_OBJ_TEMP;

    WHILE v_indice <= v_total_filas DO

    SELECT v_total_filas, v_indice FROM DUMMY;

	SELECT NUM_POLIZA, COD_MEDIADOR, COD_SUBCLAVE
        INTO v_num_poliza, v_cod_mediador, v_cod_subclave
        FROM (
            SELECT NUM_POLIZA, COD_MEDIADOR, COD_SUBCLAVE, 
                   ROW_NUMBER() OVER (ORDER BY NUM_POLIZA) AS row_num
            FROM EXT.CARTERA_OBJ_TEMP
        ) AS subquery
        WHERE subquery.row_num = v_indice;
    
        IF NOT EXISTS(SELECT 1 FROM EXT.RELASUJE_20241023 WHERE LPAD(v_NUM_POLIZA,8,0) = NUM_POLIZA) THEN
            INSERT INTO EXT.CARTERA_OBJ
            SELECT *, 'NO EXISTE POLIZA'
            FROM EXT.CARTERA 
            WHERE NUM_POLIZA = v_NUM_POLIZA;
        END IF;

        v_indice := v_indice + 1;

    END WHILE;
    


    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ' || cVersion || ' with SESSION_USER '|| SESSION_USER, cReport, io_contador);
END