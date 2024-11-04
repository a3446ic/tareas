CREATE OR REPLACE PROCEDURE "EXT"."SP_CARGA_POLIZAS_TOMADOR" (IN IN_FILENAME Varchar(120)) LANGUAGE SQLSCRIPT 
SQL SECURITY DEFINER DEFAULT SCHEMA "EXT" AS BEGIN 

/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 28-Oct-2024 
	|---------------------------------------------------------------------------------------------- 
	| Procedure Purpose: Carga desde ficheros datos de tomador relaciones con num. poliza
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

DECLARE cReportTable CONSTANT VARCHAR(50) := 'SP_CARGA_POLIZAS_TOMADOR';
DECLARE cTable CONSTANT VARCHAR(50) := 'CARGA_POLIZAS_TOMADOR_HIST';





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
    FROM EXT.CARGA_POLIZAS_TOMADOR_HIST;

-- Contar registros a modificar    
SELECT COUNT(*) INTO vRegistrosModificados
    FROM EXT.CARGA_POLIZAS_TOMADOR_HIST AS destino
    INNER JOIN EXT.CARGA_POLIZAS_TOMADOR_LOAD AS fuente
    ON destino.NUM_POLIZA = fuente.NUM_POLIZA AND destino.TOMADOR <> fuente.TOMADOR
    ;

MERGE INTO EXT.CARGA_POLIZAS_TOMADOR_HIST AS destino
USING (SELECT * FROM EXT.CARGA_POLIZAS_TOMADOR_LOAD) AS fuente
ON destino.NUM_POLIZA = fuente.NUM_POLIZA 
WHEN MATCHED AND destino.TOMADOR <> fuente.TOMADOR THEN
    UPDATE SET 
        destino.ID_SALESFORCE = fuente.ID_SALESFORCE,
        destino.EXTERNALID = fuente.EXTERNALID,
        destino.MODALIDAD = fuente.MODALIDAD,
        destino.TOMADOR = fuente.TOMADOR,
        destino.FECHA_EFECTO = fuente.FECHA_EFECTO,
        destino.FECHA_FINAL_CONTRATO = fuente.FECHA_FINAL_CONTRATO,
        destino.FASE = fuente.FASE,
        destino.ESTADO = fuente.ESTADO,
        destino.PROPIETARIO = fuente.PROPIETARIO,
        destino.CUENTA_MEDIADORA = fuente.CUENTA_MEDIADORA,
        destino.GERENTE_CESCE = fuente.GERENTE_CESCE,
        destino.BATCHNAME = IN_FILENAME,
        destino.CREATEDATE = CURRENT_TIMESTAMP        
WHEN NOT MATCHED THEN
    INSERT (ID_SALESFORCE, EXTERNALID, NUM_POLIZA, MODALIDAD, TOMADOR, FECHA_EFECTO, FECHA_FINAL_CONTRATO, FASE, ESTADO, PROPIETARIO, CUENTA_MEDIADORA, GERENTE_CESCE, BATCHNAME, CREATEDATE)
    VALUES (fuente.ID_SALESFORCE, fuente.EXTERNALID, fuente.NUM_POLIZA, fuente.MODALIDAD, fuente.TOMADOR, fuente.FECHA_EFECTO, fuente.FECHA_FINAL_CONTRATO, fuente.FASE, fuente.ESTADO, fuente.PROPIETARIO, fuente.CUENTA_MEDIADORA, fuente.GERENTE_CESCE, IN_FILENAME, CURRENT_TIMESTAMP)
 ;
    

-- Contar registros finales
SELECT COUNT(*) - vRegistrosInsertados INTO vRegistrosInsertados
    FROM EXT.CARGA_POLIZAS_TOMADOR_HIST;


CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INSERTADOS ' || To_VARCHAR(vRegistrosInsertados)  || ' REGISTROS EN EXT.' || cTable , cReportTable, io_contador);
CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'MODIFICADOS ' || To_VARCHAR(vRegistrosModificados)  || ' REGISTROS EN EXT.' || cTable , cReportTable, io_contador);

CALL LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    cReportTable || '. Proceso Terminado Satisfactoriamente',
    cReportTable,
    io_contador
);


END