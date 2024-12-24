CREATE OR REPLACE PROCEDURE "EXT"."SP_CARGA_RELASUJE" (IN IN_FILENAME Varchar(120)) LANGUAGE SQLSCRIPT 
SQL SECURITY DEFINER DEFAULT SCHEMA "EXT" AS BEGIN 

/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 23/12/2024 
	|---------------------------------------------------------------------------------------------- 
	| Procedure Purpose: Carga desde ficheros datos de relasuje
	| 
	| Version: 1.0	
	|
	----------------------------------------------------------------------------------------------- 
*/

    DECLARE io_contador Number := 0;
    DECLARE numLin Number := 0;
    DECLARE numLineasFichero Number := 0;
    DECLARE i_Tenant VARCHAR(127);
    DECLARE cVersion CONSTANT VARCHAR(2) := '01';
    DECLARE vRegistrosInsertados Number := 0;
    DECLARE vRegistrosModificados Number := 0;

    DECLARE cReportTable CONSTANT VARCHAR(50) := 'SP_CARGA_RELASUJE';
    DECLARE cTable CONSTANT VARCHAR(50) := 'RELASUJE_HIST';





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

    SELECT TENANTID INTO i_Tenant FROM CS_TENANT;

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

    -- Contar registros iniciales
    SELECT COUNT(*) INTO vRegistrosInsertados
        FROM EXT.RELASUJE_HIST;

    IF (vRegistrosInsertados <> 0) THEN
        SELECT COUNT(*) INTO vRegistrosModificados
        FROM EXT.RELASUJE_LOAD AS fuente
        WHERE NOT EXISTS(SELECT 1 
            FROM EXT.RELASUJE_HIST AS destino 
            WHERE COALESCE(destino.MOD,'') = COALESCE(fuente.MOD,'')
                AND COALESCE(destino.NUM_POLIZA,'') = COALESCE(fuente.NUM_POLIZA,'')
                AND COALESCE(destino.TIP,'') = COALESCE(fuente.TIP,'') 
                AND COALESCE(destino.COD_AGENT,'') = COALESCE(fuente.COD_AGENT,'') 
                AND COALESCE(destino.NOMBRE_AGENTE,'') = COALESCE(fuente.NOMBRE_AGENTE,'') 
                AND COALESCE(destino.PROVINCIA,'') = COALESCE(fuente.PROVINCIA,'') 
                AND COALESCE(destino.PAIS,'') = COALESCE(fuente.PAIS,'')  
                AND COALESCE(destino.FEC_INI,'1990-01-01') = COALESCE(fuente.FEC_INI,'1990-01-01')
                AND COALESCE(destino.FEC_FIN,'1990-01-01') = COALESCE(fuente.FEC_FIN,'1990-01-01')
                AND COALESCE(destino.ERROR,'') = COALESCE(fuente.ERROR ,'')
                AND COALESCE(destino.NOMBRE_SUBCLAVE,'') = COALESCE(fuente.NOMBRE_SUBCLAVE,'')  
                AND COALESCE(destino.COD_SUBCLAVE,'') = COALESCE(fuente.COD_SUBCLAVE,'')  
                AND COALESCE(destino.INTERMEDIA,'') = COALESCE(fuente.INTERMEDIA,'')  
        );
    END IF;

    MERGE INTO EXT.RELASUJE_HIST destino
    USING EXT.RELASUJE_LOAD fuente
    ON destino.COD_AGENT = fuente.COD_AGENT 
    AND destino.NUM_POLIZA = fuente.NUM_POLIZA 
    AND destino.MOD = fuente.MOD
    WHEN MATCHED AND (
        COALESCE(destino.FEC_INI,'1990-01-01') <> COALESCE(fuente.FEC_INI,'1990-01-01') 
        OR COALESCE(destino.FEC_FIN,'1990-01-01') <> COALESCE(fuente.FEC_FIN,'1990-01-01') 
        OR COALESCE(destino.COD_SUBCLAVE,'') <> COALESCE(fuente.COD_SUBCLAVE,'')
    ) THEN
        UPDATE SET 
            destino.TIP = fuente.TIP,
            destino.NOMBRE_AGENTE = fuente.NOMBRE_AGENTE,
            destino.PROVINCIA = fuente.PROVINCIA,
            destino.PAIS = fuente.PAIS,
            destino.FEC_INI = fuente.FEC_INI,
            destino.FEC_FIN = fuente.FEC_FIN,
            destino.ERROR = fuente.ERROR,
            destino.NOMBRE_SUBCLAVE = fuente.NOMBRE_SUBCLAVE,
            destino.COD_SUBCLAVE = fuente.COD_SUBCLAVE,
            destino.INTERMEDIA = fuente.INTERMEDIA,
            destino.BATCHNAME = IN_FILENAME,
            destino.CREATEDATE = CURRENT_TIMESTAMP        
    WHEN NOT MATCHED THEN
        INSERT (MOD, NUM_POLIZA, TIP, COD_AGENT, NOMBRE_AGENTE, PROVINCIA, PAIS, FEC_INI, FEC_FIN, ERROR, NOMBRE_SUBCLAVE, COD_SUBCLAVE, INTERMEDIA, BATCHNAME, CREATEDATE)
        VALUES (fuente.MOD, fuente.NUM_POLIZA, fuente.TIP, fuente.COD_AGENT, fuente.NOMBRE_AGENTE, fuente.PROVINCIA, fuente.PAIS, fuente.FEC_INI, fuente.FEC_FIN, fuente.ERROR, fuente.NOMBRE_SUBCLAVE, fuente.COD_SUBCLAVE, fuente.INTERMEDIA, IN_FILENAME, CURRENT_TIMESTAMP);
        

    -- Contar registros finales
    SELECT COUNT(*) - vRegistrosInsertados INTO vRegistrosInsertados
        FROM EXT.RELASUJE_HIST;


    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || To_VARCHAR(vRegistrosInsertados)  || ' REGISTROS EN EXT.' || cTable , cReportTable, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'MODIFICADOS ' || To_VARCHAR(vRegistrosModificados)  || ' REGISTROS EN EXT.' || cTable , cReportTable, io_contador);

    -- ACTUALIZAR POSITIONNAME
    UPDATE RH
    SET RH.POSITIONNAME = MM.COD_MEDIADOR ||'-'|| RH.COD_SUBCLAVE
    FROM EXT.RELASUJE_HIST RH LEFT JOIN EXT.MODIFICAR_MEDIADOR MM ON RH.COD_AGENT = MM.IDHOST;


    --CARGAR TABLA RELASUJE
    vRegistrosModificados:= 0;
    vRegistrosInsertados:= 0;

    -- Contar registros iniciales
    SELECT COUNT(*) INTO vRegistrosInsertados
        FROM EXT.RELASUJE;

    IF (vRegistrosInsertados <> 0) THEN
        SELECT COUNT(*) INTO vRegistrosModificados
        FROM EXT.RELASUJE_HIST AS fuente
        WHERE NOT EXISTS(SELECT 1 
            FROM EXT.RELASUJE AS destino 
            WHERE COALESCE(destino.MOD,'') = COALESCE(fuente.MOD,'')
                AND COALESCE(destino.NUM_POLIZA,'') = COALESCE(fuente.NUM_POLIZA,'')
                AND COALESCE(destino.TIP,'') = COALESCE(fuente.TIP,'') 
                AND COALESCE(destino.COD_AGENT,'') = COALESCE(fuente.COD_AGENT,'') 
                AND COALESCE(destino.NOMBRE_AGENTE,'') = COALESCE(fuente.NOMBRE_AGENTE,'') 
                AND COALESCE(destino.PROVINCIA,'') = COALESCE(fuente.PROVINCIA,'') 
                AND COALESCE(destino.PAIS,'') = COALESCE(fuente.PAIS,'')  
                AND COALESCE(destino.FEC_INI,'1990-01-01') = COALESCE(fuente.FEC_INI,'1990-01-01')
                AND COALESCE(destino.FEC_FIN,'1990-01-01') = COALESCE(fuente.FEC_FIN,'1990-01-01')
                AND COALESCE(destino.ERROR,'') = COALESCE(fuente.ERROR ,'')
                AND COALESCE(destino.NOMBRE_SUBCLAVE,'') = COALESCE(fuente.NOMBRE_SUBCLAVE,'')  
                AND COALESCE(destino.COD_SUBCLAVE,'') = COALESCE(fuente.COD_SUBCLAVE,'')  
                AND COALESCE(destino.INTERMEDIA,'') = COALESCE(fuente.INTERMEDIA,'')  
        );
    END IF;

    MERGE INTO EXT.RELASUJE destino
    USING EXT.RELASUJE_HIST fuente
    ON destino.COD_AGENT = fuente.COD_AGENT 
    AND destino.NUM_POLIZA = fuente.NUM_POLIZA 
    AND destino.MOD = fuente.MOD
    WHEN MATCHED AND (
        COALESCE(destino.FEC_INI,'1990-01-01') <> COALESCE(fuente.FEC_INI,'1990-01-01') 
        OR COALESCE(destino.FEC_FIN,'1990-01-01') <> COALESCE(fuente.FEC_FIN,'1990-01-01') 
        OR COALESCE(destino.COD_SUBCLAVE,'') <> COALESCE(fuente.COD_SUBCLAVE,'')
    ) THEN
        UPDATE SET 
            destino.TIP = fuente.TIP,
            destino.NOMBRE_AGENTE = fuente.NOMBRE_AGENTE,
            destino.PROVINCIA = fuente.PROVINCIA,
            destino.PAIS = fuente.PAIS,
            destino.FEC_INI = fuente.FEC_INI,
            destino.FEC_FIN = fuente.FEC_FIN,
            destino.ERROR = fuente.ERROR,
            destino.NOMBRE_SUBCLAVE = fuente.NOMBRE_SUBCLAVE,
            destino.COD_SUBCLAVE = fuente.COD_SUBCLAVE,
            destino.INTERMEDIA = fuente.INTERMEDIA       
    WHEN NOT MATCHED THEN
        INSERT (MOD, NUM_POLIZA, TIP, COD_AGENT, NOMBRE_AGENTE, PROVINCIA, PAIS, FEC_INI, FEC_FIN, ERROR, NOMBRE_SUBCLAVE, COD_SUBCLAVE, INTERMEDIA, POSITIONNAME)
        VALUES (fuente.MOD, fuente.NUM_POLIZA, fuente.TIP, fuente.COD_AGENT, fuente.NOMBRE_AGENTE, fuente.PROVINCIA, fuente.PAIS, fuente.FEC_INI, fuente.FEC_FIN, fuente.ERROR, fuente.NOMBRE_SUBCLAVE, fuente.COD_SUBCLAVE, fuente.INTERMEDIA, fuente.POSITIONNAME);
        
    -- Contar registros finales
    SELECT COUNT(*) - vRegistrosInsertados INTO vRegistrosInsertados
        FROM EXT.RELASUJE;


    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || To_VARCHAR(vRegistrosInsertados)  || ' REGISTROS EN EXT.RELASUJE', cReportTable, io_contador);
    CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'MODIFICADOS ' || To_VARCHAR(vRegistrosModificados)  || ' REGISTROS EN EXT.RELASUJE', cReportTable, io_contador);

        
        
    CALL LIB_GLOBAL_CESCE :w_debug (
        i_Tenant,
        cReportTable || '. Proceso Terminado Satisfactoriamente',
        cReportTable,
        io_contador
    );


END