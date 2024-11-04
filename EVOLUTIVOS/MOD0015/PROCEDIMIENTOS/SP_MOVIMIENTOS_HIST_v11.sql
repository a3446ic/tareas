CREATE OR REPLACE PROCEDURE "EXT"."SP_MOVIMIENTOS_HIST" (IN IN_FILENAME Varchar(120)) LANGUAGE SQLSCRIPT 
SQL SECURITY DEFINER DEFAULT SCHEMA "EXT" AS BEGIN 

DECLARE io_contador Number := 0;
DECLARE numLin Number := 0;
DECLARE numLineasFichero Number := 0;
DECLARE i_Tenant VARCHAR(127);
DECLARE cVersion CONSTANT VARCHAR(2) := '11';

DECLARE cReportTable CONSTANT VARCHAR(50) := 'SP_MOVIMIENTOS_HIST';
DECLARE cRegExpFecha CONSTANT VARCHAR(10) := '([0-9]+)';
DECLARE cRegExpEntero CONSTANT VARCHAR(10) := '([0-9]+)';
DECLARE cRegExpDecimal CONSTANT VARCHAR(10) := '([0-9\,]+)';
DECLARE fechaBorrarBackups DATE;

DECLARE CURSOR tablasBorrar FOR
SELECT TABLE_NAME FROM SYS.TABLES
WHERE SCHEMA_NAME='EXT'
AND TABLE_NAME LIKE 'CARTERA_BKP'  || TO_VARCHAR(ADD_MONTHS(CURRENT_DATE,-3), 'YYYYMM') || '%'
AND TO_DATE(SUBSTR_AFTER(TABLE_NAME, '%CARTERA_BKP'), 'YYYYMMDD') < ADD_MONTHS(CURRENT_DATE, -3);

DECLARE i_rev Number := 0; -- Número de ejecución


-- Versiones --------------------------------------------------------------------------------------------------------
-- v08 - cambio fecha_ini y fecha_fin para insertarse la fecha de vencimiento cuando fecha_ini > fecha_vencimiento.
-- v09 - Se ha añadido la creación del backup de cartera y la eliminación de los backups anteriores a 3 meses
-- v10 - Para los ficheros de tipo MVCAR se realiza la llamada al procedimiento SP_CARGAR_POLIZAS_TRASPASO. Se comenta la llamada SP_DETERMINAR_CIC
-- v11 - Insertar registro en REGISTROS_INTERFACES. Actualizar estado SUCCESS/FAILED según el resultado de la carga
---------------------------------------------------------------------------------------------------------------------

DECLARE EXIT HANDLER FOR SQLEXCEPTION 
BEGIN 
	
    --Actualizamos registro status = FAILED    
    UPDATE EXT.REGISTRO_INTERFACES SET NUMREC = numLineasFichero, STATUS = 'FAILED', ENDTIME = current_timestamp, ERROR = LEFT(IFNULL( ::SQL_ERROR_MESSAGE, ''),1000) WHERE BATCHNAME = IN_FILENAME AND REV = i_rev;
	
	CALL LIB_GLOBAL_CESCE :w_debug (
    	i_Tenant,
    	cReportTable || '. SQL ERROR_MESSAGE: ' || IFNULL( ::SQL_ERROR_MESSAGE, '') || '. SQL_ERROR_CODE: ' ||  ::SQL_ERROR_CODE,
    	'SP_MOVIMIENTOS_HIST',
    	io_contador
	);
    RESIGNAL;

END;

SELECT TENANTID INTO i_Tenant
FROM CS_TENANT;

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

-- Se crea un backup de la tabla de cartera si no hubiera uno del día
IF (SELECT TABLE_NAME FROM SYS.TABLES where SCHEMA_NAME='EXT' and TABLE_NAME like ('%CARTERA_BKP_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD'))) IS NULL THEN
	EXEC('CREATE COLUMN TABLE EXT.CARTERA_BKP_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD') || ' AS (SELECT * FROM EXT.CARTERA)');
    CALL LIB_GLOBAL_CESCE :w_debug (
        i_Tenant,
        'Creado BackUp EXT.CARTERA_BKP_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD'),
        cReportTable,
        io_contador
    );
END IF;
---------------------------------------------------------------------------------------------------------------------

-- Se eliminan los backups anteriores a 3 meses
OPEN  tablasBorrar;
FOR tabla AS tablasBorrar DO
    EXEC('DROP TABLE EXT.' || tabla.TABLE_NAME);
    CALL LIB_GLOBAL_CESCE :w_debug (
        i_Tenant,
        'Borrado BackUp ' || tabla.TABLE_NAME,
        cReportTable,
        io_contador
    );
END FOR;
CLOSE tablasBorrar;
---------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------
--Insertamos un registro en la tabla REGISTRO_INTERFACES
--Al finalizar el proceso actualizar el registro
SELECT IFNULL(MAX(REV),0) + 1 INTO i_rev FROM REGISTRO_INTERFACES WHERE BATCHNAME = IN_FILENAME;

INSERT INTO REGISTRO_INTERFACES(BATCHNAME,REV,NUMREC,STARTTIME)
VALUES(IN_FILENAME, i_rev, 0,current_timestamp);

---------------------------------------------------------

IF IN_FILENAME LIKE '%MVCAR%' THEN 

    DECLARE CURSOR mvcar FOR
    SELECT DISTINCT *
    FROM EXT.EXT_MOVIMIENTO_CARTERA_CREDITO_LOAD;

    SELECT count(*) into numLineasFichero
    FROM (select distinct * FROM EXT.EXT_MOVIMIENTO_CARTERA_CREDITO_LOAD) ;

    CALL LIB_GLOBAL_CESCE :w_debug (
        i_Tenant,
        'Fichero MOVIMIENTO_CARTERA con '  || to_varchar(numLineasFichero) || ' lineas distintas. Insertando en tabla EXT_MOVIMIENTO_CARTERA_CREDITO_HIST',
        cReportTable,
        io_contador
    );

    OPEN mvcar;

    FOR i AS mvcar DO

        numLin:= numLin + 1;

        CALL LIB_GLOBAL_CESCE :w_debug (
        i_Tenant,
        'Linea '|| TO_VARCHAR (numLin) || ' NUM_POLIZA:' || COALESCE(i.NUM_POLIZA, 0) || ',IDMODALIDAD:' || COALESCE(i.IDMODALIDAD, 0) || ',IDMEDIADOR:' || COALESCE (i.IDMEDIADOR, 0) ||
        ',IDSUBCLAVE:' || COALESCE (i.IDSUBCLAVE, 0) || ',FECHA_INI:' || COALESCE (i.FECHA_INI, 0) || 
        ',FECHA_FIN' || COALESCE (i.FECHA_FIN, 0) || ',TIPO_MOV:' || COALESCE (i.IDTIPO_MOV, 0),
        cReportTable,
        io_contador
        );

        INSERT INTO EXT.EXT_MOVIMIENTO_CARTERA_CREDITO_HIST
        VALUES
        (
        i.IDMODALIDAD,
        i.NUM_POLIZA,
        i.IDFASE,
        i.DESC_FASE,
        i.IDESTADO,
        i.DESC_ESTADO,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_SIT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_SIT)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.NUM_ANUALIDAD,
        i.ID_SIT_ANUALIDAD,
        i.DESC_SIT_ANUALIDAD,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_SIT_ANUALIDAD)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_SIT_ANUALIDAD)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_EMISION)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_EMISION)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_EFECTO)
                        FROM
                            DUMMY
                    ) = 1 THEN 
                        TO_DATE(i.FECHA_EFECTO)
                ELSE 
                    TO_DATE(i.FECHA_EMISION)
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_VENCIMIENTO)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_VENCIMIENTO)
                    ELSE TO_DATE('22000101', 'yyyyMMdd')
                END
            FROM
                DUMMY
        ),
        i.IDCLIENTE,
        i.NOMBRE_CLIENTE,
        i.IND_VIP,
        i.IDGRUPO,
        i.DESC_GRUPO,
        i.IDDELEGACION,
        i.DESC_DELEGACION,
        i.IDCOMERCIAL,
        i.NOMBRE_COMERCIAL,
        i.IDOFICINA,
        i.DESC_OFICINA,
        (SELECT LPAD(i.IDMEDIADOR, 4, '0') FROM DUMMY),
        i.NOMBRE_MEDIADOR,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.IMPORTE_VENTAS_PREV_EXT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(
                        REPLACE(i.IMPORTE_VENTAS_PREV_EXT, ',', '.'),
                        30,
                        3
                    )
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.IMPORTE_VENTAS_PREV_INT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(
                        REPLACE(i.IMPORTE_VENTAS_PREV_INT, ',', '.'),
                        30,
                        3
                    )
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PRIMA_PROVISIONAL_INT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PRIMA_PROVISIONAL_INT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PRIMA_PROVISIONAL_EXT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PRIMA_PROVISIONAL_EXT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PORC_TASA_INT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PORC_TASA_INT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PORC_TASA_EXT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PORC_TASA_EXT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.TIPO_TARIFA_INT,
        i.TIPO_TARIFA_EXT,
        i.IDMODALIDAD_POL_O,
        i.NUM_POL_O,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_TRASPASO_POL_O)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_TRASPASO_POL_O)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.ID_SIT_POL_O,
        i.DESC_SIT_POL_O,
        i.IDMODALIDAD_POL_D,
        i.NUM_POL_D,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_TRASPASO_POL_D)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_TRASPASO_POL_D)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.ID_SIT_POL_D,
        i.DESC_SIT_POL_D,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_DATOS)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_DATOS)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.IDCNAE,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PR_COBRADA_RC_INT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PR_COBRADA_RC_INT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PR_COBRADA_RC_EXT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PR_COBRADA_RC_EXT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PR_COBRADA_RP)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PR_COBRADA_RP, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PR_COBRADA_TC_RC)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PR_COBRADA_TC_RC, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PR_COBRADA_TC_RP)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PR_COBRADA_TC_RP, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PR_EMITIDA_RC_INT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PR_EMITIDA_RC_INT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PR_EMITIDA_RC_EXT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PR_EMITIDA_RC_EXT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PR_EMITIDA_RP)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PR_EMITIDA_RP, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PR_EMITIDA_TC_RC)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PR_EMITIDA_TC_RC, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PR_EMITIDA_TC_RP)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PR_EMITIDA_TC_RP, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.IDMEDIADOR_2,
        i.NOMBRE_MEDIADOR_2,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.POR_COB_RC_INT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.POR_COB_RC_INT, ',', '.'), 30, 0)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.POR_COB_RC_EXT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.POR_COB_RC_EXT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.POR_COB_RP)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.POR_COB_RP, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_PRIM_EMISION)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_PRIM_EMISION)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.IDESTADO_POL,
        i.DESC_ESTADO_POL,
        i.IDFISCAL_TOMADOR,
        i.IND_20X100,
        i.IND_PRORROGA,
        i.IDDIVISA_MERCADO_INT,
        i.IDDIVISA_MERCADO_EXT,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.IMPORTE_MAX_FINANCIACION)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(
                        REPLACE(i.IMPORTE_MAX_FINANCIACION, ',', '.'),
                        30,
                        0
                    )
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.IMPORTE_FRANQUICIA)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.IMPORTE_FRANQUICIA, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.INDEMNIZACION_MAX)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.INDEMNIZACION_MAX, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PORC_TASA_MAX)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PORC_TASA_MAX, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.TARIFA_GASTOS_ANALISIS,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PRIMA_MIN_MERCADO_INT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PRIMA_MIN_MERCADO_INT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PRIMA_MIN_MERCADO_EXT)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PRIMA_MIN_MERCADO_EXT, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.COD_ACUERDO,
        i.IDTIPO_MEDIADIOR,
        i.DESC_TIPO_MEDIADIOR,
        i.IDSEGMENTO_EMP,
        i.DESC_SEGMENTO_EMP,
        i.IND_FIRMA_DIGITAL,
        i.IDAGENTE,
        (SELECT 
            CASE WHEN (i.FECHA_INI IS NULL AND (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_FIN) FROM DUMMY ) = 1) THEN
                TO_DATE(i.FECHA_FIN,'YYYYMMDD')
            ELSE
                CASE WHEN (i.FECHA_FIN IS NULL AND TO_DATE(i.FECHA_INI, 'YYYYMMDD') > TO_DATE(i.FECHA_VENCIMIENTO, 'YYYYMMDD') AND (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_VENCIMIENTO) FROM DUMMY ) = 1) THEN
                    TO_DATE(i.FECHA_VENCIMIENTO,'YYYYMMDD')
                ELSE
                    CASE WHEN (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_INI) FROM DUMMY ) = 1 THEN 
                        TO_DATE(i.FECHA_INI, 'YYYYMMDD')
                    ELSE
                        CASE WHEN (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_EFECTO) FROM DUMMY ) = 1 THEN
                            TO_DATE(i.FECHA_EFECTO, 'YYYYMMDD')
                        ELSE
                            TO_DATE(i.FECHA_EMISION, 'YYYYMMDD')
                        END
                    END
                END
            END
        FROM DUMMY),
        /*
        (SELECT CASE WHEN (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_INI) FROM DUMMY ) = 1 THEN 
            TO_DATE(i.FECHA_INI)
         ELSE
            CASE WHEN (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_EFECTO) FROM DUMMY ) = 1 THEN
                TO_DATE(i.FECHA_EFECTO)
            ELSE
                TO_DATE(i.FECHA_EMISION)
            END
         END
        FROM DUMMY),*/
        /* 
        VERSION PREVIA PARA INSERTAR FECHA_INI
        (SELECT CASE WHEN (i.FECHA_INI IS NOT NULL AND (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_INI) FROM DUMMY ) = 1) THEN
            TO_DATE(i.FECHA_INI)
        ELSE
            CASE WHEN (i.FECHA_EFECTO IS NOT NULL AND (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_EFECTO) FROM DUMMY ) = 1 AND i.FECHA_EFECTO < i.FECHA_FIN) THEN
                TO_DATE(i.FECHA_EFECTO)
            ELSE
                CASE WHEN i.FECHA_EMISION IS NOT NULL THEN
                    TO_DATE(i.FECHA_EMISION)
                ELSE
                    NULL
                END
            END
        END FROM DUMMY),*/
        (SELECT 
            CASE WHEN (i.FECHA_FIN IS NOT NULL AND (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_FIN) FROM DUMMY) = 1) THEN
                TO_DATE(i.FECHA_FIN, 'YYYYMMDD')
            ELSE
                CASE WHEN (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_VENCIMIENTO) FROM DUMMY) = 1  THEN
                    TO_DATE(i.FECHA_VENCIMIENTO, 'YYYYMMDD')
                ELSE
                    NULL
                END
            END
        FROM DUMMY),
        /*
        VERSION PREVIA PARA INSERTAR FECHA_FIN
        (SELECT CASE WHEN i.FECHA_FIN IS NOT NULL THEN
                CASE WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_FIN)
                        FROM
                            DUMMY
                    ) = 1 
                THEN 
                    TO_DATE(i.FECHA_FIN)
                ELSE 
                    CASE WHEN (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_VENCIMIENTO) FROM DUMMY) THEN 
                        TO_DATE(i.FECHA_VENCIMIENTO)
                    ELSE
                        NULL
                    END
                END
        ELSE
            CASE WHEN (SELECT OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_VENCIMIENTO) FROM DUMMY) THEN 
                TO_DATE(i.FECHA_VENCIMIENTO)
            ELSE
                NULL
            END
        END 
        FROM DUMMY),*/
        i.DESC_SUBCLAVE,
        (SELECT CASE WHEN i.IDSUBCLAVE IS NULL THEN '0000' ELSE LPAD(i.IDSUBCLAVE, 4, '0') END FROM DUMMY),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.PORC_INTERMEDIACION)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.PORC_INTERMEDIACION, ',', '.'), 30, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.IDTIPO_MOV,
        IN_FILENAME,
        CURRENT_DATE,
        (SELECT CASE WHEN ((i.IDPAIS > 0 AND i.IDPAIS <= 52) OR TO_VARCHAR(i.IDPAIS) IN (
            SELECT clas.classifierid as IDPAIS--, clas.NAME as PAIS, TO_VARCHAR(TO_INTEGER(cgc.GENERICNUMBER1)) as BUMAP, cgc.GENERICATTRIBUTE1 as BUNAME
            FROM cs_category cat, cs_category_classifiers ccc, 
            cs_classifier clas, cs_genericclassifier cgc
            WHERE cat.ruleelementseq = ccc.categoryseq
            AND clas.classifierseq = ccc.classifierseq
            AND clas.classifierseq = cgc.classifierseq
            AND cat.REMOVEDATE = TO_DATE('22000101','yyyymmdd')
            AND cat.ISLAST=1
            AND ccc.REMOVEDATE = TO_DATE('22000101','yyyymmdd') 
            AND ccc.ISLAST=1
            AND clas.REMOVEDATE = TO_DATE('22000101','yyyymmdd') 
            AND clas.ISLAST=1
            AND cgc.REMOVEDATE = TO_DATE('22000101','yyyymmdd') 
            AND cgc.ISLAST=1
            AND cat.NAME = 'ID Paises'))
        THEN 
            'PENDIENTE'
        ELSE
            'ERROR_PAIS'
        END FROM DUMMY),
        i.IDPAIS
    );

    END FOR;

    CLOSE mvcar;

        /*
        SELECT  ( SELECT EXT.LIB_GLOBAL_CESCE :getProductId( (select lpad(mvcar.IDMODALIDAD, 3, '0') from dummy), NULL, '116', mvcar.NUM_POLIZA).productId FROM DUMMY ) as productId, 
        mvcar.NUM_POLIZA, lpad(mvcar.IDMEDIADOR, 4, '0') as IDMEDIADOR, lpad(mvcar.IDSUBCLAVE, 4, '0') IDSUBCLAVE, 
        CASE WHEN mvcar.FECHA_INI is null then mvcar.FECHA_EFECTO else mvcar.FECHA_INI end  as FECHA_INI
        FROM EXT.EXT_MOVIMIENTO_CARTERA_CREDITO_HIST mvcar WHERE ESTADOREG = 'PENDIENTE' and NUM_POLIZA='3000331'
        UNION ALL select c.IDPRODUCT as productId,c.NUM_POLIZA,lpad(c.COD_MEDIADOR, 4, '0')  as IDMEDIADOR,lpad(c.COD_SUBCLAVE, 4, '0')  as IDSUBCLAVE, c.FECHA_INICIO  as FECHA_INI 
        from EXT.CARTERA c where NUM_POLIZA='3000331'
        */


    /*
    MERGE into EXT.CARTERA c using (
        SELECT DISTINCT
            mvcar.*,
            (
                SELECT
                    EXT.LIB_GLOBAL_CESCE :getProductId( (select lpad(mvcar.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (mvcar.IDPAIS > 0 AND mvcar.IDPAIS <= 52) THEN 116 ELSE mvcar.IDPAIS END), mvcar.NUM_POLIZA).productId
                FROM
                    DUMMY
            ) as productId
        FROM
            EXT.EXT_MOVIMIENTO_CARTERA_CREDITO_HIST mvcar
        WHERE
            ESTADOREG = 'PENDIENTE'
    ) src 
    ON c.IDPRODUCT = src.productId
    AND c.NUM_POLIZA = src.NUM_POLIZA
    AND c.COD_MEDIADOR = src.IDMEDIADOR
    AND c.COD_SUBCLAVE = src.IDSUBCLAVE
    AND c.FECHA_INICIO = CASE WHEN src.FECHA_INI is null then src.FECHA_EFECTO else src.FECHA_INI end
    WHEN MATCHED THEN
    UPDATE
    SET
        c.NUM_ANUALIDAD = src.NUM_ANUALIDAD,
        c.FECHA_EMISION = src.FECHA_EMISION,
        c.FECHA_EFECTO = src.FECHA_EFECTO,
        c.FECHA_VENCIMIENTO = src.FECHA_VENCIMIENTO,
        c.IDPAIS = (CASE WHEN (src.IDPAIS > 0 AND src.IDPAIS <= 52) THEN 116 ELSE src.IDPAIS END),
        c.PRIMA_PROVISIONAL_INT = src.PRIMA_PROVISIONAL_INT,
        c.PRIMA_PROVISIONAL_EXT = src.PRIMA_PROVISIONAL_EXT,
        c.IDDIVISA_INT = src.IDDIVISA_MERCADO_INT,
        c.IDDIVISA_EXT = src.IDDIVISA_MERCADO_EXT,
        c.PRIMA_MIN_INT = src.PRIMA_MIN_MERCADO_INT,
        c.PRIMA_MIN_EXT = src.PRIMA_MIN_MERCADO_EXT,
        c.COD_MEDIADOR = lpad(src.IDMEDIADOR, 4, '0'),
        --c.COD_SUBCLAVE = lpad(src.IDSUBCLAVE, 4, '0'),
        c.COD_SUBCLAVE = IFNULL(lpad(src.IDSUBCLAVE, 4, '0'), '0000'),
        c.P_INTERMEDIACION = 100 * src.PORC_INTERMEDIACION,
        --c.FECHA_INICIO = src.FECHA_INI,
        c.FECHA_INICIO = CASE WHEN src.FECHA_INI is null then src.FECHA_EFECTO else src.FECHA_INI end,
        --c.FECHA_FIN = src.FECHA_FIN,
        c.FECHA_FIN = CASE WHEN src.FECHA_FIN is null then TO_DATE('2200-01-01','YYYY-MM-DD') else src.FECHA_FIN end,
        c.NIF_TOMADOR = src.IDFISCAL_TOMADOR,
        c.NOMBRE_TOMADOR = '',  -- NO VIENE EN EL FICHERO
        --src.NOMBRE_TOMADOR,
        c.MEDIADOR_PRINCIPAL_CIC = 1,
        c.ACTIVO = 1,
        c.MODIF_DATE = CURRENT_TIMESTAMP,
        c.MODIF_USER = 'CDL',
        c.MODIF_SOURCE = IN_FILENAME
        WHEN NOT MATCHED THEN
    INSERT
    VALUES
        (
            'CREDITO',
            src.productId,
            src.NUM_POLIZA,
            src.IDMODALIDAD,
            NULL,
            0,
            NULL,
            NULL,
            src.NUM_ANUALIDAD,
            src.FECHA_EMISION,
            src.FECHA_EFECTO,
            src.FECHA_VENCIMIENTO,
            (CASE WHEN (src.IDPAIS > 0 AND src.IDPAIS <= 52) THEN 116 ELSE src.IDPAIS END),
            src.PRIMA_PROVISIONAL_INT,
            src.PRIMA_PROVISIONAL_EXT,
            src.IDDIVISA_MERCADO_INT,
            src.IDDIVISA_MERCADO_EXT,
            NULL,
            src.PRIMA_MIN_MERCADO_INT,
            src.PRIMA_MIN_MERCADO_EXT,
            lpad(src.IDMEDIADOR, 4, '0'),
            --lpad(src.IDSUBCLAVE, 4, '0'),
			IFNULL(lpad(src.IDSUBCLAVE, 4, '0'), '0000'),
            ( 100 * src.PORC_INTERMEDIACION),
            --src.FECHA_INI,
            CASE WHEN src.FECHA_INI is null then src.FECHA_EFECTO else src.FECHA_INI end,
            --src.FECHA_FIN,
			CASE WHEN src.FECHA_FIN is null then TO_DATE('2200-01-01','YYYY-MM-DD') else src.FECHA_FIN end,
            NULL,
            NULL,
            src.IDFISCAL_TOMADOR,
            '', --NOMBRE_TOMADOR  -- NO VIENE EN EL FICHERO
            NULL,
            1,
            1,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            'CDL',
            IN_FILENAME
        );
*/

    CALL EXT.SP_SET_MOVIMIENTOS_ENVIADO('mvcar', IN_FILENAME);
    CALL EXT.SP_CARGAR_POLIZAS_TRASPASO();

---------------------------------------------------------
-- MOVIMIENTOS FIANZAS - CAUCION
---------------------------------------------------------
ELSEIF IN_FILENAME LIKE '%MVFID%' THEN 

    DECLARE CURSOR mvfid FOR 
    SELECT DISTINCT * FROM EXT.EXT_MOVIMIENTO_FIANZAS_CAUCION_LOAD;

    SELECT count(*) into numLineasFichero
	FROM (select distinct * FROM EXT.EXT_MOVIMIENTO_FIANZAS_CAUCION_LOAD) ;


    CALL LIB_GLOBAL_CESCE :w_debug (
        i_Tenant,
        'Fichero MOVIMIENTO_FIANZAS con '  || to_varchar(numLineasFichero) || ' lineas distintas. Insertando en tabla EXT_MOVIMIENTO_FIANZAS_CAUCION_HIST',
        cReportTable,
        io_contador
    );

    OPEN mvfid;

    FOR i AS mvfid DO
    INSERT INTO EXT.EXT_MOVIMIENTO_FIANZAS_CAUCION_HIST
    VALUES (
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_DATOS)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_DATOS)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_EJECUCION)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_EJECUCION)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.IDPAIS,
        i.IDMODALIDAD,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpEntero IN i.NUM_POLIZA)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_BIGINT(i.NUM_POLIZA)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.IDCLIENTE,
        i.IDCOMERCIAL,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpEntero IN i.IDCNAE)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_INT(i.IDCNAE)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpEntero IN i.NUM_AVAL_HOST)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_BIGINT(i.NUM_AVAL_HOST)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpEntero IN i.NUM_EXPEDIENTE)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_BIGINT(i.NUM_EXPEDIENTE)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpEntero IN i.NUM_AVAL_FIANZA)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.NUM_AVAL_FIANZA, ',', '.'), 30, 0)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.IDSUBMODALIDAD,
        i.DESC_SUBMODALIDAD,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_EFECTO)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_EFECTO)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_VENCIMIENTO)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_VENCIMIENTO)
                    ELSE TO_DATE('22000101', 'yyyyMMdd')
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.IMPORTE_ASEG_AVAL)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.IMPORTE_ASEG_AVAL, ',', '.'), 18, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.IDCOBERTURA,
        i.DESC_COBERTURA,
        
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_INI_COBERTURA)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_INI_COBERTURA)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpFecha IN i.FECHA_FIN_COBERTURA)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DATE(i.FECHA_FIN_COBERTURA)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.IMPORTE_ASEG_COBERTURA)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.IMPORTE_ASEG_COBERTURA, ',', '.'), 18, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.IDDIVISA_COBERTURA,
        (SELECT CASE WHEN (i.IDMEDIADOR LIKE '%-%' OR i.IDMEDIADOR LIKE '%_'ESCAPE'%') THEN
            CASE WHEN i.IDMEDIADOR LIKE '%-%' THEN
                LPAD(SUBSTR_BEFORE(i.IDMEDIADOR,'-'), 4, '0') || '-' || LPAD(SUBSTR_AFTER(i.IDMEDIADOR,'-'), 4, '0')
            ELSE
                LPAD(SUBSTR_BEFORE(i.IDMEDIADOR,'_'), 4, '0') || '-' || LPAD(SUBSTR_AFTER(i.IDMEDIADOR,'_'), 4, '0')
            END
        ELSE
            CASE WHEN LENGTH(i.IDMEDIADOR) > 4 THEN
                '0000-0000'
            ELSE
                LPAD(i.IDMEDIADOR, 4, '0') || '-' || '0000'
            END
        END FROM DUMMY),
        i.IDFISCAL_ASEGURADO,
        i.NOMBRE_ASEGURADO,
        i.IDFISCAL_TOMADOR,
        (SELECT RTRIM(REPLACE(REPLACE(i.NOMBRE_TOMADOR, CHAR(9), ' '), '"','')) FROM DUMMY),
        i.IDCONDICIONADO,
        i.TIPO_COASEGURO,
        i.IDMODALIDAD_HOST,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpEntero IN i.NUM_POLIZA_HOST)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_BIGINT(i.NUM_POLIZA_HOST)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.ID_SIT,
        i.POLISITU,
        i.CAUSAPOL,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpEntero IN i.NUMSITUA)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_BIGINT(i.NUMSITUA)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.NOMBRE_MEDIADOR,
        i.IDFISCAL_MEDIADOR,
        i.TIPO_MEDIADOR,
        i.IDOFICINA,
        i.IDFASE,
        (
            SELECT
                CASE
                    WHEN (
                        SELECT
                            OCCURRENCES_REGEXPR(cRegExpDecimal IN i.IMPORTE_PRIMA_EMITIDA)
                        FROM
                            DUMMY
                    ) = 1 THEN TO_DECIMAL(REPLACE(i.IMPORTE_PRIMA_EMITIDA, ',', '.'), 18, 3)
                    ELSE NULL
                END
            FROM
                DUMMY
        ),
        i.IDTIPO_MOV,
        IN_FILENAME,
        CURRENT_DATE,
        (SELECT CASE WHEN ((i.IDPAIS > 0 AND i.IDPAIS <= 52) OR TO_VARCHAR(i.IDPAIS) IN (
            SELECT clas.classifierid as IDPAIS--, clas.NAME as PAIS, TO_VARCHAR(TO_INTEGER(cgc.GENERICNUMBER1)) as BUMAP, cgc.GENERICATTRIBUTE1 as BUNAME
            FROM cs_category cat, cs_category_classifiers ccc, 
            cs_classifier clas, cs_genericclassifier cgc
            WHERE cat.ruleelementseq = ccc.categoryseq
            AND clas.classifierseq = ccc.classifierseq
            AND clas.classifierseq = cgc.classifierseq
            AND cat.REMOVEDATE = TO_DATE('22000101','yyyymmdd')
            AND cat.ISLAST=1
            AND ccc.REMOVEDATE = TO_DATE('22000101','yyyymmdd') 
            AND ccc.ISLAST=1
            AND clas.REMOVEDATE = TO_DATE('22000101','yyyymmdd') 
            AND clas.ISLAST=1
            AND cgc.REMOVEDATE = TO_DATE('22000101','yyyymmdd') 
            AND cgc.ISLAST=1
            AND cat.NAME = 'ID Paises'))
        THEN 
            'PENDIENTE'
        ELSE
            'ERROR_PAIS'
        END FROM DUMMY)
    );

END FOR;

CLOSE mvfid;

/*
MERGE into EXT.CARTERA c using (
    SELECT
        mvfid.*,
        (
            SELECT
                EXT.LIB_GLOBAL_CESCE :getProductId( (select lpad(mvfid.IDMODALIDAD, 3, '0') from dummy), mvfid.IDSUBMODALIDAD, (CASE WHEN (mvfid.IDPAIS > 0 AND mvfid.IDPAIS <= 52) THEN 116 ELSE mvfid.IDPAIS END), mvfid.NUM_POLIZA).productId
            FROM
                DUMMY
        ) as productId
    FROM
        EXT.EXT_MOVIMIENTO_FIANZAS_CAUCION_HIST mvfid
    WHERE
        ESTADOREG = 'PENDIENTE'
) src ON c.IDPRODUCT = src.productId
AND c.NUM_POLIZA = src.NUM_POLIZA
AND c.NUM_FIANZA = src.NUM_AVAL_FIANZA
AND c.COD_MEDIADOR = SUBSTR_BEFORE(src.IDMEDIADOR,'-')
AND c.COD_SUBCLAVE = SUBSTR_AFTER(src.IDMEDIADOR,'-')
AND c.FECHA_INICIO = src.FECHA_EFECTO
AND c.NUM_AVAL_HOST = src.NUM_AVAL_HOST
WHEN MATCHED THEN
UPDATE
SET
    c.NUM_EXPEDIENTE = src.NUM_EXPEDIENTE,
    c.NUM_AVAL_HOST = src.NUM_AVAL_HOST,
    c.FECHA_EFECTO = src.FECHA_EFECTO,
    c.FECHA_VENCIMIENTO = src.FECHA_VENCIMIENTO,
    c.IDPAIS = src.IDPAIS,
    c.IDDIVISA_COBERTURA = src.IDDIVISA_COBERTURA,
    c.COD_MEDIADOR = SUBSTR_BEFORE(src.IDMEDIADOR,'-'),
    c.COD_SUBCLAVE = SUBSTR_AFTER(src.IDMEDIADOR,'-'),
    c.P_INTERMEDIACION = 100.0,
    c.FECHA_INICIO = src.FECHA_EFECTO,
    --c.FECHA_FIN = src.FECHA_VENCIMIENTO,
	c.FECHA_FIN = CASE WHEN src.FECHA_VENCIMIENTO is null or src.FECHA_VENCIMIENTO = '2099-12-31' then TO_DATE('2200-01-01','YYYY-MM-DD') else src.FECHA_VENCIMIENTO end,
    c.NIF_TOMADOR = src.IDFISCAL_TOMADOR,
    c.NOMBRE_TOMADOR = src.NOMBRE_TOMADOR,
    c.MEDIADOR_PRINCIPAL_CIC = 1,
    c.ACTIVO = 1,
    c.MODIF_DATE = CURRENT_TIMESTAMP,
    c.MODIF_USER = 'CDL',
    c.MODIF_SOURCE = IN_FILENAME
    WHEN NOT MATCHED THEN
    INSERT
    VALUES('CAUCION',productId,src.NUM_POLIZA,src.IDMODALIDAD,src.IDSUBMODALIDAD,src.NUM_AVAL_FIANZA,
            src.NUM_EXPEDIENTE,src.NUM_AVAL_HOST,0,NULL,src.FECHA_EFECTO,src.FECHA_VENCIMIENTO,
            src.IDPAIS,
            NULL,NULL,NULL,NULL,src.IDDIVISA_COBERTURA,NULL,NULL, 
            SUBSTR_BEFORE(src.IDMEDIADOR,'-'), --lpad(src.IDMEDIADOR, 4, '0'),
            SUBSTR_AFTER(src.IDMEDIADOR,'-'), --'0000',
            100.00,
            src.FECHA_EFECTO, --FECHA_INICIO
            --src.FECHA_VENCIMIENTO,
            CASE WHEN src.FECHA_VENCIMIENTO is null or src.FECHA_VENCIMIENTO = '2099-12-31' then TO_DATE('2200-01-01','YYYY-MM-DD') else src.FECHA_VENCIMIENTO end, --FECHA_FIN
            NULL,NULL,src.IDFISCAL_TOMADOR,src.NOMBRE_TOMADOR,NULL,1,1,
            CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'CDL',IN_FILENAME);
    */

    CALL EXT.SP_SET_MOVIMIENTOS_ENVIADO('mvfid', IN_FILENAME);

END IF;

-- v10. Se comenta la llamada (no se usa)
--CALL EXT.SP_DETERMINAR_CIC();



CALL LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    cReportTable || '. Proceso Terminado Satisfactoriamente',
    'SP_MOVIMIENTOS_HIST',
    io_contador
);

--Actualizamos registro status = SUCCESS
UPDATE EXT.REGISTRO_INTERFACES SET NUMREC = numLineasFichero, STATUS = 'SUCCESS', ENDTIME = current_timestamp WHERE BATCHNAME = IN_FILENAME AND REV = i_rev;

COMMIT;

END