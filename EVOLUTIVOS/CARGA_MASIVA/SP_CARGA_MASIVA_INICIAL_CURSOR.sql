CREATE OR REPLACE PROCEDURE "EXT"."SP_CARGA_MASIVA_INICIAL" (IN_FILENAME Varchar(120)) LANGUAGE SQLSCRIPT SQL SECURITY DEFINER DEFAULT SCHEMA "EXT" AS
BEGIN

-- Versiones --------------------------------------------------------------------------------------------------------
-- v01 - Versión inicial
---------------------------------------------------------------------------------------------------------------------


--Declaración de variables
DECLARE io_contador Number := 0;
DECLARE i_Tenant VARCHAR(127);
DECLARE cVersion CONSTANT VARCHAR(2) := '01';
DECLARE cReportTable CONSTANT VARCHAR(50) := 'SP_CARGA_MASIVA_INICIAL';
DECLARE i_rev Number := 0; -- Número de ejecución
DECLARE user_name VARCHAR(50);
DECLARE existe Number;
DECLARE reg_insertadas Number;


DECLARE CURSOR c_cursor1 FOR
		SELECT NUM_POLIZA,CLIENT,ESTADO,COMPANYIA,EFECTO,FECHA_VENCIMIENTO,FECHA_CREACION,TIPOLOGIA,FECHA_CONTRATACION,TO_DECIMAL(REPLACE(PRIMA_NETA, ',', '.')) AS PRIMA_NETA,RIESGO_POLIZA,COLABORADOR,CLIENTE_COLABORADOR
		FROM EXT.EXT_CARGA_MASIVA_LOAD_TEMP;

--Gestión de errores
DECLARE EXIT HANDLER FOR SQLEXCEPTION 
BEGIN 
	
    CALL LIB_GLOBAL_CESCE :w_debug (
    	i_Tenant,
    	cReportTable || '. SQL ERROR_MESSAGE: ' || IFNULL( ::SQL_ERROR_MESSAGE, '') || '. SQL_ERROR_CODE: ' ||  ::SQL_ERROR_CODE,
    	cReportTable,
    	io_contador
	);
    RESIGNAL;

END;


		

--Obtenemos tenant
SELECT TENANTID INTO i_Tenant FROM CS_TENANT;


DELETE FROM EXT.EXT_CARGA_MASIVA_HIST WHERE BATCHNAME = IN_FILENAME;

CALL LIB_GLOBAL_CESCE :w_debug(
        i_Tenant,
        'STARTING with SESSION_USER: ' || SESSION_USER || ' version ' || cVersion,
        cReportTable,
        io_contador
    );

    CALL LIB_GLOBAL_CESCE :w_debug (
        i_Tenant,
        'COMIENZA Tratamiento fichero ' || IN_FILENAME,
        cReportTable,
        io_contador
    );


    --UPDATE EXT.EXT_CARGA_MASIVA_LOAD_TEMP SET PRIMA_NETA = TO_DECIMAL(REPLACE(PRIMA_NETA, ',', '.'));

    

    

    FOR cur_row AS c_cursor1 
	DO

        CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'COMPROBACION CAMPOS NULOS', cReportTable, io_contador);
        --COMPROBACION CAMPOS CLAVE NO NULOS
		IF (cur_row.NUM_POLIZA IS NULL OR LENGTH(TRIM(cur_row.NUM_POLIZA)) = 0
            OR cur_row.CLIENT IS NULL OR LENGTH(TRIM(cur_row.CLIENT)) = 0
            OR cur_row.ESTADO IS NULL OR LENGTH(TRIM(cur_row.ESTADO)) = 0
            OR cur_row.COMPANYIA IS NULL OR LENGTH(TRIM(cur_row.COMPANYIA)) = 0
            OR cur_row.EFECTO IS NULL OR LENGTH(TRIM(cur_row.EFECTO)) = 0
            OR cur_row.FECHA_VENCIMIENTO IS NULL OR LENGTH(TRIM(cur_row.FECHA_VENCIMIENTO)) = 0
            OR cur_row.FECHA_CREACION IS NULL OR LENGTH(TRIM(cur_row.FECHA_CREACION)) = 0
            OR cur_row.TIPOLOGIA IS NULL OR LENGTH(TRIM(cur_row.TIPOLOGIA)) = 0
            OR cur_row.FECHA_CONTRATACION IS NULL OR LENGTH(TRIM(cur_row.FECHA_CONTRATACION)) = 0
            OR cur_row.PRIMA_NETA IS NULL OR LENGTH(TRIM(cur_row.PRIMA_NETA)) = 0
            OR cur_row.RIESGO_POLIZA IS NULL OR LENGTH(TRIM(cur_row.RIESGO_POLIZA)) = 0
            OR cur_row.COLABORADOR IS NULL OR LENGTH(TRIM(cur_row.COLABORADOR)) = 0
            OR cur_row.CLIENTE_COLABORADOR IS NULL OR LENGTH(TRIM(cur_row.CLIENTE_COLABORADOR)) = 0) THEN
        
            INSERT INTO EXT.EXT_CARGA_MASIVA_NO_VALIDADAS
            VALUES(cur_row.NUM_POLIZA,cur_row.CLIENT,cur_row.ESTADO,cur_row.COMPANYIA,cur_row.EFECTO,cur_row.FECHA_VENCIMIENTO,cur_row.FECHA_CREACION,
            cur_row.TIPOLOGIA,cur_row.FECHA_CONTRATACION,cur_row.PRIMA_NETA,cur_row.RIESGO_POLIZA,cur_row.COLABORADOR,cur_row.CLIENTE_COLABORADOR,
            IN_FILENAME,SESSION_USER,CURRENT_TIMESTAMP,'Algun campo clave es nulo');
            
            CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,
                'Se han movido '  || To_VARCHAR(::ROWCOUNT) || ' registros con -Algun campo clave es nulo- a EXT_CARGA_MASIVA_NO_VALIDADAS', cReportTable, io_contador);

            DELETE FROM EXT.EXT_CARGA_MASIVA_LOAD_TEMP A
            WHERE 1=1 --A.BATCHNAME = IN_FILENAME
            AND (A.NUM_POLIZA = cur_row.NUM_POLIZA
            OR A.CLIENT = cur_row.CLIENT
            OR A.ESTADO = cur_row.ESTADO
            OR A.COMPANYIA = cur_row.COMPANYIA
            OR A.EFECTO = cur_row.EFECTO
            OR A.FECHA_VENCIMIENTO = cur_row.FECHA_VENCIMIENTO
            OR A.FECHA_CREACION = cur_row.FECHA_CREACION
            OR A.TIPOLOGIA = cur_row.TIPOLOGIA
            OR A.FECHA_CONTRATACION = cur_row.FECHA_CONTRATACION
            OR A.PRIMA_NETA = cur_row.PRIMA_NETA
            OR A.RIESGO_POLIZA = cur_row.RIESGO_POLIZA
            OR A.COLABORADOR = cur_row.COLABORADOR
            OR A.CLIENTE_COLABORADOR = cur_row.CLIENTE_COLABORADOR);

        END IF;
		
		
        --COMPROBACION FORMATO DE FECHAS
        
        CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'COMPROBACION CAMPOS FECHAS VALIDAS', cReportTable, io_contador);

		IF (LENGTH(LTRIM(cur_row.FECHA_VENCIMIENTO, '0123456789-/')) != 0
            OR LENGTH(LTRIM(cur_row.FECHA_CREACION, '0123456789-/')) != 0
            OR LENGTH(LTRIM(cur_row.FECHA_CONTRATACION, '0123456789-/')) != 0) THEN

            INSERT INTO EXT.EXT_CARGA_MASIVA_NO_VALIDADAS	
            VALUES(cur_row.NUM_POLIZA,cur_row.CLIENT,cur_row.ESTADO,cur_row.COMPANYIA,cur_row.EFECTO,cur_row.FECHA_VENCIMIENTO,cur_row.FECHA_CREACION,
            cur_row.TIPOLOGIA,cur_row.FECHA_CONTRATACION,cur_row.PRIMA_NETA,cur_row.RIESGO_POLIZA,cur_row.COLABORADOR,cur_row.CLIENTE_COLABORADOR,
            IN_FILENAME,SESSION_USER,CURRENT_TIMESTAMP,'Alguna fecha con formato incorrecto');

            DELETE FROM EXT.EXT_CARGA_MASIVA_LOAD_TEMP A
            WHERE 1=1 --A.BATCHNAME = IN_FILENAME
            AND (A.NUM_POLIZA = cur_row.NUM_POLIZA
            OR A.CLIENT = cur_row.CLIENT
            OR A.ESTADO = cur_row.ESTADO
            OR A.COMPANYIA = cur_row.COMPANYIA
            OR A.EFECTO = cur_row.EFECTO
            OR A.FECHA_VENCIMIENTO = cur_row.FECHA_VENCIMIENTO
            OR A.FECHA_CREACION = cur_row.FECHA_CREACION
            OR A.TIPOLOGIA = cur_row.TIPOLOGIA
            OR A.FECHA_CONTRATACION = cur_row.FECHA_CONTRATACION
            OR A.PRIMA_NETA = cur_row.PRIMA_NETA
            OR A.RIESGO_POLIZA = cur_row.RIESGO_POLIZA
            OR A.COLABORADOR = cur_row.COLABORADOR
            OR A.CLIENTE_COLABORADOR = cur_row.CLIENTE_COLABORADOR);

	    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,
        'Se han movido '  || To_VARCHAR(::ROWCOUNT) || ' registros con -Alguna fecha con formato incorrecto- a EXT_CARGA_MASIVA_NO_VALIDADAS', cReportTable, io_contador);

        END IF;
        
    
	
    

         existe:=0;
         SELECT count(*) INTO existe FROM EXT_CARGA_MASIVA_HIST B  
         WHERE cur_row.NUM_POLIZA = B.NUM_POLIZA AND
            cur_row.CLIENT = B.CLIENT AND
            cur_row.ESTADO = B.ESTADO AND
            cur_row.COMPANYIA = B.COMPANYIA AND
            cur_row.EFECTO = B.EFECTO AND
            cur_row.FECHA_VENCIMIENTO = B.FECHA_VENCIMIENTO AND
            cur_row.FECHA_CREACION = B.FECHA_CREACION AND
            cur_row.TIPOLOGIA = B.TIPOLOGIA AND
            cur_row.FECHA_CONTRATACION = B.FECHA_CONTRATACION AND
            cur_row.PRIMA_NETA = B.PRIMA_NETA AND
            cur_row.RIESGO_POLIZA = B.RIESGO_POLIZA AND
            cur_row.COLABORADOR = B.COLABORADOR AND
            cur_row.CLIENTE_COLABORADOR = B.CLIENTE_COLABORADOR;

         IF existe = 0 THEN
            INSERT INTO EXT.EXT_CARGA_MASIVA_HIST
            VALUES(cur_row.NUM_POLIZA,cur_row.CLIENT,cur_row.ESTADO,cur_row.COMPANYIA,cur_row.EFECTO,cur_row.FECHA_VENCIMIENTO,cur_row.FECHA_CREACION,
            cur_row.TIPOLOGIA,cur_row.FECHA_CONTRATACION,cur_row.PRIMA_NETA,cur_row.RIESGO_POLIZA,cur_row.COLABORADOR,cur_row.CLIENTE_COLABORADOR,
            IN_FILENAME,SESSION_USER,CURRENT_TIMESTAMP,'PENDIENTE');

            
         END IF;
	--  IF NOT EXISTS(SELECT * FROM EXT.EXT_CARGA_MASIVA_HIST B 
    --      WHERE cur_row.NUM_POLIZA = B.NUM_POLIZA)THEN 
	--           cur_row.CLIENT = B.CLIENT AND
	--           cur_row.ESTADO = B.ESTADO AND
	--           cur_row.COMPANYIA = B.COMPANYIA AND
	--           cur_row.EFECTO = B.EFECTO AND
	--           cur_row.FECHA_VENCIMIENTO = B.FECHA_VENCIMIENTO AND
	--           cur_row.FECHA_CREACION = B.FECHA_CREACION AND
	--           cur_row.TIPOLOGIA = B.TIPOLOGIA AND
	--           cur_row.FECHA_CONTRATACION = B.FECHA_CONTRATACION AND
	--           cur_row.PRIMA_NETA = B.PRIMA_NETA AND
	--           cur_row.RIESGO_POLIZA = B.RIESGO_POLIZA AND
	--           cur_row.COLABORADOR = B.COLABORADOR AND
	--           cur_row.CLIENTE_COLABORADOR = B.CLIENTE_COLABORADOR) THEN


   
        -- INSERT INTO EXT.EXT_CARGA_MASIVA_HIST(NUM_POLIZA)
        -- VALUES(cur_row.NUM_POLIZA);--,cur_row.CLIENT,cur_row.ESTADO,cur_row.COMPANYIA,cur_row.EFECTO,cur_row.FECHA_VENCIMIENTO,cur_row.FECHA_CREACION,
    --         -- cur_row.TIPOLOGIA,cur_row.FECHA_CONTRATACION,cur_row.PRIMA_NETA,cur_row.RIESGO_POLIZA,cur_row.COLABORADOR,cur_row.CLIENTE_COLABORADOR,
    --         -- IN_FILENAME,SESSION_USER,CURRENT_TIMESTAMP,'PENDIENTE');

    -- END IF;	
	
	END FOR;

    --CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTAMOS HIST', cReportTable, io_contador);

    SELECT COUNT(*) INTO reg_insertadas FROM EXT.EXT_CARGA_MASIVA_HIST WHERE BATCHNAME = IN_FILENAME;

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant,
        'Se han insertado '  || To_VARCHAR(reg_insertadas) || ' registros en EXT_CARGA_MASIVA_HIST', cReportTable, io_contador);
        
	CALL LIB_GLOBAL_CESCE :w_debug (
        i_Tenant,
        'FIN Tratamiento fichero ' || IN_FILENAME,
        cReportTable,
        io_contador
    );


END;