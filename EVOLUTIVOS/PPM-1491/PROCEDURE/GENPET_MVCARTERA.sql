CREATE OR REPLACE PROCEDURE "EXT"."GENPET_MVCARTERA" (IN IN_FILENAME VARCHAR(120)) LANGUAGE SQLSCRIPT AS
BEGIN

	DECLARE io_contador Number := 0;
	DECLARE i_Tenant VARCHAR(127);
	DECLARE cVersion VARCHAR(2) := '01';
    DECLARE cReportTable CONSTANT VARCHAR(50) := 'GENPET_MVCARTERA' || '_' || cVersion;
    
    
	
-- ----------------------------- HANDLER EXCEPTION -------------------------
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'SQL ERROR_MESSAGE: ' ||
						IFNULL(::SQL_ERROR_MESSAGE,'') || '. SQL_ERROR_CODE: ' || ::SQL_ERROR_CODE, cReportTable, io_contador);
		END;
-- ---------------------------------------------------------------------------

	SELECT EXT.LIB_GLOBAL_CESCE:getTenantID() INTO i_Tenant FROM DUMMY;

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'INICIO PROCEDIMIENTO with SESSION_USER '|| SESSION_USER, cReportTable, io_contador);

	-- Borrar Peticiones previas en Estado PENDIENTE
	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Borrar Peticiones previas en Estado PENDIENTE para los movimientos del archivo: ' || IN_FILENAME, cReportTable, io_contador);

	DELETE  FROM "EXT"."PETICIONES_CAMBIO_CARTERA"
	WHERE CASEID = IN_FILENAME 
	AND ESTADOREG='PENDIENTE'; 

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Registros Borrados de PETICIONES_CAMBIO_CARTERA: ' || TO_VARCHAR(::ROWCOUNT), cReportTable, io_contador);


	-- ----------------------------------------------------------------------------------------------------
	-- Se Inserta la Petición de Traspaso
	-- ----------------------------------------------------------------------------------------------------
	INSERT INTO "EXT"."PETICIONES_CAMBIO_CARTERA" (
		RAMO,
		IDPRODUCT,
		NUM_POLIZA,
		IDMODALIDAD,
		IDSUBMODALIDAD,
		NUM_FIANZA,
		NUM_EXPEDIENTE,
		NUM_ANUALIDAD,
		COD_MEDIADOR_CEDENTE,
		COD_SUBCLAVE_CEDENTE,
		COD_MEDIADOR_RECEPTOR,
		COD_SUBCLAVE_RECEPTOR,
		P_INTERMEDIACION,
		INICIO_PERIODO,
		FECHA_EFECTO_TRASPASO,
		FECHA_INICIO,
		FECHA_FIN,
		USUARIO,
		EMAIL,
		TIPO_CAMBIO,
		TIPO_TRASPASO,
		P_EMISION,
		P_RENOVACION,
		IND_COMISION,
		ESTADOREG,
		CASEID,
		MODIF_DATE
	)
	SELECT
        C.RAMO,
        C.IDPRODUCT,
        C.NUM_POLIZA,
        C.IDMODALIDAD,
        C.IDSUBMODALIDAD,
        C.NUM_FIANZA,
        C.NUM_EXPEDIENTE,
        C.NUM_ANUALIDAD,
        C.COD_MEDIADOR,
        C.COD_SUBCLAVE,
        C.COD_MEDIADOR,
        C.COD_SUBCLAVE,
        C.P_INTERMEDIACION,
        'I' AS INICIO_PERIODO,
        C.FECHA_EFECTO_TRASPASO,
        C.FECHA_INICIO,
        C.FECHA_FIN,
        'CDL' AS USUARIO,
        'email@callidusondemand.com' AS EMAIL,
        'C' AS TIPO_CAMBIO,
        'S' AS TIPO_TRASPASO,
        F.P_EMISION_FIN AS P_EMISION,
        F.P_RENOV_FIN AS P_RENOVACION,
        'S' AS IND_COMISION,
        'PENDIENTE' AS ESTADOREG,
        MODIF_SOURCE,
        CURRENT_TIMESTAMP AS MODIF_DATE
    FROM EXT.CARTERA C
    JOIN (
        SELECT 
            CT.NUM_POLIZA,
            CT.COD_MEDIADOR,
            CT.COD_SUBCLAVE,
            CT.IDMODALIDAD,
            ROW_NUMBER() OVER (
                PARTITION BY CT.NUM_POLIZA, CT.COD_MEDIADOR, CT.COD_SUBCLAVE, CT.IDMODALIDAD
                ORDER BY CT.NUM_ANUALIDAD DESC, CT.ACTIVO DESC
            ) AS RN,
            CASE 
                WHEN CT.P_ESPECIAL_EMISION IS NULL OR CT.P_ESPECIAL_EMISION = 0 
                THEN COALESCE(PC.P_EMISION, P.P_EMISION_DEF)
                ELSE CT.P_ESPECIAL_EMISION 
            END AS P_EMISION_FIN,
            CASE 
                WHEN CT.P_ESPECIAL_RENOVACION IS NULL OR CT.P_ESPECIAL_RENOVACION = 0 
                THEN COALESCE(PC.P_RENOVACION, P.P_RENOVACION_DEF)
                ELSE CT.P_ESPECIAL_RENOVACION 
            END AS P_RENOV_FIN
        FROM EXT.CARTERA CT
        LEFT JOIN EXT.PLAN_COMISIONAMIENTO PC 
            ON CT.COD_MEDIADOR || '-' || CT.COD_SUBCLAVE = PC.POSITIONNAME 
            AND CT.IDPRODUCT = PC.IDPRODUCT 
            AND CT.FECHA_EFECTO >= PC.EFFECTIVESTARTDATE 
            AND CT.FECHA_EFECTO < PC.EFFECTIVEENDDATE
        LEFT JOIN EXT.PRODUCTOS_VW P 
            ON CT.IDPRODUCT = P.IDPRODUCT
    ) F
    ON C.IDMODALIDAD = F.IDMODALIDAD 
    AND C.NUM_POLIZA = F.NUM_POLIZA 
    AND C.COD_MEDIADOR = F.COD_MEDIADOR 
    AND C.COD_SUBCLAVE = F.COD_SUBCLAVE
    WHERE MODIF_SOURCE = IN_FILENAME
    AND F.RN = 1;

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ', cReportTable, io_contador);

-- Fin procedimiento
END