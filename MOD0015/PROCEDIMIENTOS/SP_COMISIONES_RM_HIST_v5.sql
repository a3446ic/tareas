CREATE OR REPLACE PROCEDURE "EXT"."SP_COMISIONES_RM_HIST" (IN IN_FILENAME Varchar(120)) LANGUAGE SQLSCRIPT SQL SECURITY DEFINER DEFAULT SCHEMA "EXT" AS BEGIN DECLARE io_contador Number := 0;

DECLARE i_Tenant VARCHAR(127);
DECLARE cVersion CONSTANT VARCHAR(2) := '05';
DECLARE cReportTable CONSTANT VARCHAR(50) := 'SP_COMISIONES_RM_HIST' || ' ' || cVersion;

--DECLARE cCaracterNegativoCIC CONSTANT VARCHAR(1) := 'p';
DECLARE i_rev Number := 0; -- Número de ejecución
DECLARE numLineasFichero Number := 0;

-- VERSIONES
--v02 - 
--v03 - Se añade RESIGNAL en el Exception handler y se pone como constante el caracter negativo CIC
--v04 - Se quita como constante el caracter negativo CIC
--v05 - Insertar registro en REGISTROS_INTERFACES. Actualizar estado SUCCESS/FAILED según el resultado de la carga


DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN 
	--Actualizamos registro status = FAILED    
    UPDATE EXT.REGISTRO_INTERFACES SET NUMREC = numLineasFichero, STATUS = 'FAILED', ENDTIME = current_timestamp, ERROR = LEFT(IFNULL( ::SQL_ERROR_MESSAGE, ''),1000) WHERE BATCHNAME = IN_FILENAME AND REV = i_rev;
	
    CALL LIB_GLOBAL_CESCE :w_debug ( i_Tenant,
    'SQL ERROR_MESSAGE: ' || IFNULL( ::SQL_ERROR_MESSAGE, '') || '. SQL_ERROR_CODE: ' ||  ::SQL_ERROR_CODE,
    cReportTable,
    io_contador);
	RESIGNAL;
END;

SELECT
    TENANTID INTO i_Tenant
FROM
    CS_TENANT;

CALL LIB_GLOBAL_CESCE :w_debug(
    i_Tenant,
    'STARTING with SESSION_USER: ' || SESSION_USER || ' Batchname: ' || IN_FILENAME,
    cReportTable,
    io_contador
);


-- En caso de reprocesamiento, se eliminan los registros que tienen como batchname el nombre del archivo reprocesado
DELETE FROM CS_STAGESALESTRANSACTION WHERE BATCHNAME = IN_FILENAME;
DELETE FROM CS_STAGETRANSACTIONASSIGN WHERE BATCHNAME = IN_FILENAME;
DELETE FROM EXT.COMISIONES_RM_HIST WHERE BATCHNAME = IN_FILENAME;

IF IN_FILENAME LIKE '%COMRM%' THEN DECLARE CURSOR comrm FOR
SELECT
    *
FROM
    EXT.COMISIONES_RM_LOAD;

---------------------------------------------------------
--Insertamos un registro en la tabla REGISTRO_INTERFACES
--Al finalizar el proceso actualizar el registro
SELECT IFNULL(MAX(REV),0) + 1 INTO i_rev FROM REGISTRO_INTERFACES WHERE BATCHNAME = IN_FILENAME;

INSERT INTO REGISTRO_INTERFACES(BATCHNAME,REV,NUMREC,STARTTIME)
VALUES(IN_FILENAME, i_rev, 0,current_timestamp);

SELECT count(*) into numLineasFichero
FROM (select distinct * FROM EXT.COMISIONES_RM_LOAD) ;
---------------------------------------------------------   

OPEN comrm;
FOR i AS comrm DO 
INSERT INTO 
    EXT.COMISIONES_RM_HIST
VALUES
(
    SUBSTR(i.DATOS, 1, 6),  -- MESCIERRE 6
    SUBSTR(i.DATOS, 7, 1),  -- COMPANIA 1
    (
        SELECT
            CASE
                WHEN (
                    SELECT
                        OCCURRENCES_REGEXPR('([0-9]+)' IN SUBSTR(i.DATOS, 8, 3))
                    FROM
                        DUMMY
                ) = 1 THEN TO_BIGINT(SUBSTR(i.DATOS, 8, 3))
                ELSE NULL
            END
        FROM
            DUMMY
    )   -- IDPAIS 3
    ,
    (
        SELECT
            CASE
                WHEN (
                    SELECT
                        OCCURRENCES_REGEXPR('([0-9]+)' IN SUBSTR(i.DATOS, 11, 11))
                    FROM
                        DUMMY
                ) = 1 THEN TO_BIGINT(SUBSTR(i.DATOS, 11, 11))
                ELSE NULL
            END
        FROM
            DUMMY
    )   -- NUM_POLIZA 11
	,
    (
        SELECT
            CASE
                WHEN (
                    SELECT
                        OCCURRENCES_REGEXPR('([0-9]+)' IN SUBSTR(i.DATOS, 22, 8))
                    FROM
                        DUMMY
                ) = 1 THEN TO_DATE(SUBSTR(i.DATOS, 22, 8))
                ELSE NULL
            END
        FROM
            DUMMY
    )  -- FECHA_EFECTO_ANUALIDAD 8
	,
    SUBSTR(i.DATOS, 30, 2),  -- COD_GARANTIA 2
    SUBSTR(i.DATOS, 32, 1),  -- CUE_RIESGO 1
    SUBSTR(i.DATOS, 33, 1),  -- CANAL_DISTRIB 1
    SUBSTR(i.DATOS, 34, 4),  -- IDMEDIADOR 4
    SUBSTR(i.DATOS, 38, 3),  -- TIPOMEDIADOR 3
    SUBSTR(i.DATOS, 41, 3),  -- TIPOCOMISION 3
    SUBSTR(i.DATOS, 44, 3),  -- TIPOFACTURA 3
    SUBSTR(i.DATOS, 47, 8),  -- NUMFACTURA 8
    SUBSTR(i.DATOS, 55, 3),  -- IDDIVISA 3
    (
        SELECT
            CASE
                WHEN (
                    SELECT
                        OCCURRENCES_REGEXPR('([0-9]+)' IN SUBSTR(i.DATOS, 58, 8))
                    FROM
                        DUMMY
                ) = 1 THEN TO_DATE(SUBSTR(i.DATOS, 58, 8))
                ELSE NULL
            END
        FROM
            DUMMY
    )  -- FEC_MOVIMIENTO 8
	 ,
    (
        SELECT
            CASE
                WHEN (
                    SELECT
                        OCCURRENCES_REGEXPR('([0-9]+)' IN SUBSTR(i.DATOS, 66, 4))
                    FROM
                        DUMMY
                ) = 1 THEN TO_BIGINT(SUBSTR(i.DATOS, 66, 4))
                ELSE NULL
            END
        FROM
            DUMMY
    )  -- NUM_MVTO 4
	,
    (
        SELECT
            CASE
                WHEN (
                    SELECT
                        OCCURRENCES_REGEXPR('([0-9]+)' IN SUBSTR(i.DATOS, 70, 3))
                    FROM
                        DUMMY
                ) = 1 THEN TO_BIGINT(SUBSTR(i.DATOS, 70, 3))
                ELSE NULL
            END
        FROM
            DUMMY
    )  -- TIPO_MVTO 3
	,
    SUBSTR(i.DATOS, 73, 4), -- IDSUBCLAVE 4
    (
        SELECT
            CASE
                WHEN (
                    SELECT
                        OCCURRENCES_REGEXPR('([0-9]+)' IN SUBSTR(i.DATOS, 77, 17))
                    FROM
                        DUMMY
                ) = 1 THEN 
                    CASE
                        WHEN RIGHT(SUBSTR(i.DATOS, 77, 18),1) = '0' THEN TO_DECIMAL(SUBSTR(i.DATOS, 77, 15) || '.' || SUBSTR(i.DATOS, 92, 2))
                        WHEN RIGHT(SUBSTR(i.DATOS, 77, 18),1) = 'p' THEN -1 * TO_DECIMAL(SUBSTR(i.DATOS, 77, 15) || '.' || SUBSTR(i.DATOS, 92, 2))
                        ELSE NULL
                    END
                ELSE NULL
            END
        FROM
            DUMMY
    )  -- IMPORTE_EMITIDA 18,3
	 ,
    (
        SELECT
            CASE
                WHEN (
                    SELECT
                        OCCURRENCES_REGEXPR('([0-9]+)' IN SUBSTR(i.DATOS, 95, 17))
                    FROM
                        DUMMY
                ) = 1 THEN 
                    CASE
                        WHEN RIGHT(SUBSTR(i.DATOS, 95, 18),1) = '0' THEN TO_DECIMAL(SUBSTR(i.DATOS, 95, 15) || '.' || SUBSTR(i.DATOS, 110, 2))
                        WHEN RIGHT(SUBSTR(i.DATOS, 95, 18),1) = 'p' THEN -1 * TO_DECIMAL(SUBSTR(i.DATOS, 95, 15) || '.' || SUBSTR(i.DATOS, 110, 2))
                        ELSE NULL
                    END
                ELSE NULL
            END
        FROM
            DUMMY
    )  -- IMPORTE_COM_EMITIDA 18,3
	,
    (
        SELECT
            CASE
                WHEN (
                    SELECT
                        OCCURRENCES_REGEXPR('([0-9]+)' IN SUBSTR(i.DATOS, 113, 17))
                    FROM
                        DUMMY
                ) = 1 THEN 
                    CASE
                        WHEN RIGHT(SUBSTR(i.DATOS, 113, 18),1) = '0' THEN TO_DECIMAL(SUBSTR(i.DATOS, 113, 15) || '.' || SUBSTR(i.DATOS, 128, 2))
                        WHEN RIGHT(SUBSTR(i.DATOS, 113, 18),1) = 'p' THEN -1 * TO_DECIMAL(SUBSTR(i.DATOS, 113, 15) || '.' || SUBSTR(i.DATOS, 128, 2))
                        ELSE NULL
                    END
                ELSE NULL
            END
        FROM
            DUMMY
    )    -- IMPORTE_COM_COBRADA 18,3
	,
    (
        SELECT
            CASE
                WHEN (
                    SELECT
                        OCCURRENCES_REGEXPR('([0-9]+)' IN SUBSTR(i.DATOS, 131, 17))
                    FROM
                        DUMMY
                ) = 1 THEN 
                    CASE
                        WHEN RIGHT(SUBSTR(i.DATOS, 131, 18),1) = '0' THEN TO_DECIMAL(SUBSTR(i.DATOS, 131, 15) || '.' || SUBSTR(i.DATOS, 146, 2))
                        WHEN RIGHT(SUBSTR(i.DATOS, 131, 18),1) = 'p' THEN -1 * TO_DECIMAL(SUBSTR(i.DATOS, 131, 15) || '.' || SUBSTR(i.DATOS, 146, 2))
                        ELSE NULL
                    END
                ELSE NULL
            END 
        FROM
            DUMMY
    )   -- IMPORTE_COM_COBRADA 18,3
	,
    IN_FILENAME,
    CURRENT_DATE,
    'PENDIENTE'
);
END FOR;
CLOSE comrm;
END IF;

CALL LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'Proceso de carga en COMISIONES_RM_HIST terminado',
    cReportTable,
    io_contador
);
COMMIT;

CALL EXT.SP_CARGAR_COMISIONES_RM();

--Actualizamos registro status = SUCCESS
UPDATE EXT.REGISTRO_INTERFACES SET NUMREC = numLineasFichero, STATUS = 'SUCCESS', ENDTIME = current_timestamp WHERE BATCHNAME = IN_FILENAME AND REV = :i_rev;

CALL LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    'Proceso Terminado Satisfactoriamente',
    cReportTable,
    io_contador
);
END