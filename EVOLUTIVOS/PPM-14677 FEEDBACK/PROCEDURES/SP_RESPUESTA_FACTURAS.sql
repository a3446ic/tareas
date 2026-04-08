CREATE OR REPLACE PROCEDURE "EXT"."SP_RESPUESTA_FACTURAS" (IN IN_FILENAME Varchar(120)) LANGUAGE SQLSCRIPT 
SQL SECURITY DEFINER DEFAULT SCHEMA "EXT" AS BEGIN 

/*---------------------------------------------------------------------
    | Author: Samuel Miralles Manresa
    | Company: Inycom
    | Initial Version Date: 22-Enero-2026
    |----------------------------------------------------------------------
    | Procedimiento para ....  
    | Parámetros: IN_FILENAME          
    | Calendario:                                
    |
	| Version:	0.1	SMM 20260122	Initial Version.
	| Version:	0.2	SMM 20260122	La tabla RESPUESTA_FACTURAS_LOAD contiene un solo registro,
	|								hay que ir separando por campos
	|
    -----------------------------------------------------------------------
*/

DECLARE io_contador Number := 0;
DECLARE numLin Number := 0;
DECLARE numLineasFichero Number := 0;
DECLARE i_Tenant VARCHAR(127);
DECLARE cVersion CONSTANT VARCHAR(2) := '0.2';

DECLARE v_proc_name VARCHAR(50) := ::CURRENT_OBJECT_NAME;
DECLARE v_posIni INT;
DECLARE v_posFin INT;
DECLARE v_fechaCarga DATE;



DECLARE i_rev Number := 0; -- Número de ejecución


-- Versiones --------------------------------------------------------------------------------------------------------
-- v01
---------------------------------------------------------------------------------------------------------------------

DECLARE EXIT HANDLER FOR SQLEXCEPTION 
BEGIN 
	
    --Actualizamos registro status = FAILED    
    UPDATE EXT.REGISTRO_INTERFACES SET NUMREC = numLineasFichero, STATUS = 'FAILED', ENDTIME = current_timestamp, ERROR = LEFT(IFNULL( ::SQL_ERROR_MESSAGE, ''),1000) WHERE BATCHNAME = IN_FILENAME AND REV = i_rev;
	
	CALL LIB_GLOBAL_CESCE :w_debug (
    	i_Tenant,
    	v_proc_name || '. SQL ERROR_MESSAGE: ' || IFNULL( ::SQL_ERROR_MESSAGE, '') || '. SQL_ERROR_CODE: ' ||  ::SQL_ERROR_CODE,
    	v_proc_name,
    	io_contador
	);
    RESIGNAL;

END;

SELECT TENANTID INTO i_Tenant
FROM CS_TENANT;

CALL LIB_GLOBAL_CESCE :w_debug(
    i_Tenant,
    'STARTING with SESSION_USER: ' || SESSION_USER || ' version ' || cVersion,
    v_proc_name,
    io_contador
);

CALL LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'COMIENZA Tratamiento fichero ' || IN_FILENAME,
    v_proc_name,
    io_contador
);




---------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------
--Insertamos un registro en la tabla REGISTRO_INTERFACES
--Al finalizar el proceso actualizar el registro
SELECT IFNULL(MAX(REV),0) + 1 INTO i_rev FROM REGISTRO_INTERFACES WHERE BATCHNAME = IN_FILENAME;

--ELIMINAMOS REGISTROS DEL FICHERO CARGADO
DELETE FROM EXT.RESPUESTA_FACTURAS WHERE BATCHNAME = IN_FILENAME;

SELECT count(*) into numLineasFichero
    FROM (select distinct * FROM EXT.RESPUESTA_FACTURAS_LOAD) ;
    
--Obtenemos fecha del fichero
v_posIni := INSTR(IN_FILENAME, '_', 1, 2) + 1;
v_posFin := INSTR(IN_FILENAME, '_', 1, 3);


-- SELECT SUBSTR(IN_FILENAME,v_posIni,v_posFin-v_posIni) INTO  v_fechaCarga FROM DUMMY;

SELECT 
CASE WHEN SUBSTR(SUBSTR(IN_FILENAME,v_posIni,8),5,2) > '12'
	THEN TO_VARCHAR(TO_DATE(SUBSTR(IN_FILENAME,v_posIni,8), 'YYYYDDMM'), 'YYYYMMDD') 
	ELSE SUBSTR(IN_FILENAME,v_posIni,8)
	END
INTO  v_fechaCarga
FROM DUMMY;

INSERT INTO REGISTRO_INTERFACES(BATCHNAME,REV,NUMREC,STARTTIME)
VALUES(IN_FILENAME, i_rev, 0,current_timestamp);

---------------------------------------------------------

INSERT INTO EXT.RESPUESTA_FACTURAS(ESTADO,PROVEEDOR,REFERENCIA,MENSAJE,PROCESO,BATCHNAME,IDFACTURA)
SELECT ESTADO,PROVEEDOR,REFERENCIA,MENSAJE,PROCESO,IN_FILENAME,IDFACTURA
FROM (
SELECT 
  --,SUBSTR_BEFORE(C.LINEA, ';')  AS CAMPO1
  SUBSTR_BEFORE(SUBSTR_AFTER(RF.LINEA, ';'),';') AS ESTADO
  ,SUBSTR_BEFORE(SUBSTR_AFTER(SUBSTR_AFTER(RF.LINEA, ';'),';'),';') AS PROVEEDOR
  ,SUBSTR_BEFORE(SUBSTR_AFTER(SUBSTR_AFTER(SUBSTR_AFTER(RF.LINEA, ';'),';'),';'),';') AS REFERENCIA
  ,SUBSTR_BEFORE(SUBSTR_AFTER(SUBSTR_AFTER(SUBSTR_AFTER(SUBSTR_AFTER(RF.LINEA, ';'),';'),';'),';'),';') AS MENSAJE
  ,SUBSTR_AFTER(SUBSTR_AFTER(SUBSTR_AFTER(SUBSTR_AFTER(SUBSTR_AFTER(RF.LINEA, ';'),';'),';'),';'),';') AS PROCESO
FROM EXT.RESPUESTA_FACTURAS_LOAD RF
	WHERE LINEA NOT LIKE '%EOF%'
-- ) C LEFT JOIN EXT.FACTURAS F ON C.PROVEEDOR = F.CODPROVEEDOR AND C.REFERENCIA = F.REFFACTURA
) C LEFT JOIN EXT.FACTURAS F ON C.PROVEEDOR = F.CODPROVEEDOR AND C.REFERENCIA = F.IDFACTURA
WHERE ESTADO IN ('S','E')
;
-- SELECT ESTADO,PROVEEDOR,REFERENCIA,MENSAJE,PROCESO,IN_FILENAME,IDFACTURA
-- FROM EXT.RESPUESTA_FACTURAS_LOAD RF LEFT JOIN EXT.FACTURAS F ON RF.PROVEEDOR = F.CODPROVEEDOR AND RF.REFERENCIA = F.REFFACTURA
-- WHERE ESTADO IN ('S','E');


IF EXISTS(SELECT 1 FROM EXT.RESPUESTA_FACTURAS WHERE IDFACTURA IS NULL AND BATCHNAME = IN_FILENAME) THEN
	CALL LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'NO SE HA OBTENIDO IDFACTURA EN ALGÚN REGISTRO',
    v_proc_name,
    io_contador
);
ELSE
	
END IF;

IF EXISTS(SELECT 1 FROM EXT.RESPUESTA_FACTURAS WHERE ESTADO = 'E' AND BATCHNAME = IN_FILENAME) THEN
	CALL LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'EL FICHERO CONTIENE ERRORES',
    v_proc_name,
    io_contador
);

	
ELSE
	--Actualizamos registro status = SUCCESS
	UPDATE EXT.REGISTRO_INTERFACES SET NUMREC = numLineasFichero, STATUS = 'SUCCESS', ENDTIME = current_timestamp WHERE BATCHNAME = IN_FILENAME AND REV = i_rev;
	
END IF;

UPDATE F SET FECHA_CARGA = v_fechaCarga, ESTADOFACTURA = CASE WHEN RF.ESTADO = 'E' THEN 'ERROR' ELSE 'CARGADO' END, MODIF_DATE = CURRENT_TIMESTAMP 
	FROM EXT.FACTURAS F INNER JOIN EXT.RESPUESTA_FACTURAS RF ON F.IDFACTURA = RF.IDFACTURA
	WHERE RF.BATCHNAME = IN_FILENAME;



CALL LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'Proceso Terminado Satisfactoriamente',
    v_proc_name,
    io_contador
);



COMMIT;

END;