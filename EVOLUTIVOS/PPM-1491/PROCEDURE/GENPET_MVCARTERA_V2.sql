CREATE OR REPLACE PROCEDURE "EXT"."GENPET_MVCARTERA_V2" (IN IN_FILENAME VARCHAR(120)) LANGUAGE SQLSCRIPT AS
BEGIN

-- Versiones --------------------------------------------------------------------------------------------------------
-- v01 - versión incial
-- v02 - se recorre el cursor para obtener los datos de la tabla CARTERA y obtener los porcentajes de comisión
---------------------------------------------------------------------------------------------------------------------
	DECLARE io_contador Number := 0;
	DECLARE i_Tenant VARCHAR(127);
	DECLARE cVersion VARCHAR(2) := '02';
    DECLARE cReportTable CONSTANT VARCHAR(50) := 'GENPET_MVCARTERA_V2' || '_' || cVersion;
	DECLARE vPorc_Esp_Emision DECIMAL(6,3);
	DECLARE vPorc_Esp_Renovacion DECIMAL(6,3);			 
	DECLARE vPorc_Plan_Emision DECIMAL(6,3);
	DECLARE vPorc_Plan_Renovacion DECIMAL(6,3);
	DECLARE vPorc_Emision_DEF DECIMAL(6,3);
	DECLARE vPorc_Renovacion_DEF DECIMAL(6,3);
	DECLARE vPorc_Emision_Final DECIMAL(6,3);
	DECLARE vPorc_Renovacion_Final DECIMAL(6,3);
    
    -- ----------------------------------------------------------------------------------------------------
	-- Cursor para obtener todas las entradas de CARTERA filtrado por fichero 
	-- ----------------------------------------------------------------------------------------------------
	DECLARE CURSOR cur_polizas FOR
	SELECT
		RAMO,
		IDPRODUCT,
		NUM_POLIZA,
		IDMODALIDAD,
		IDSUBMODALIDAD,
		NUM_FIANZA,  --NUM_FIANZA
		NUM_EXPEDIENTE,
		NUM_ANUALIDAD,
		COD_MEDIADOR AS COD_MEDIADOR_CEDENTE, -- Código Mediador Cedente 
		COD_SUBCLAVE AS COD_SUBCLAVE_CEDENTE, --  Subclave Mediador Cedente
		COD_MEDIADOR AS COD_MEDIADOR_RECEPTOR, -- Código Mediador Receptor 
		COD_SUBCLAVE AS COD_SUBCLAVE_RECEPTOR, --  Subclave Mediador Receptor  
		P_INTERMEDIACION,
		'I' AS INICIO_PERIODO,  --INICIO_PERIODO
		FECHA_EFECTO_TRASPASO,  -- FECHA_EFECTO_TRASPASO
		FECHA_INICIO,
		FECHA_FIN,
		'CDL' AS USUARIO,
		'email@callidusondemand.com' AS EMAIL,
		'C' AS TIPO_CAMBIO, -- Con derechos y obligaciones C con derechos , S sin derechos
		'S' AS TIPO_TRASPASO, -- Traspaso parcial S si es parcial, N si es completo
		NULL AS P_EMISION, --P_EMISION
		NULL AS P_RENOVACION, --P_RENOVACION
		'S' AS IND_COMISION, -- Con derechos y obligaciones S si es con derechos y N si es sin derechos
		'PENDIENTE' AS ESTADOREG,
		IN_FILENAME AS CASEID,
		CURRENT_TIMESTAMP AS MODIF_DATE
	FROM EXT.CARTERA
	WHERE MODIF_SOURCE = IN_FILENAME;
	
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


	FOR cur_row AS cur_polizas
	DO
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'REGISTRO EXT.CARTERA ' 
		|| IFNULL(TO_CHAR(cur_row.IDMODALIDAD),'') || '-' || IFNULL(TO_CHAR(cur_row.NUM_POLIZA),'') || '-'
		|| IFNULL(TO_CHAR(cur_row.COD_MEDIADOR_RECEPTOR),'') || '-'|| IFNULL(TO_CHAR(cur_row.COD_SUBCLAVE_RECEPTOR),'') || '-' 
		|| IFNULL(TO_CHAR(cur_row.FECHA_INICIO),'') || '-' || IFNULL(TO_CHAR(cur_row.FECHA_FIN),''), cReportTable, io_contador);


		-- Select NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		-- 	   NULL, NULL, NULL, NULL, NULL, NULL,
		-- 	   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
		-- into vRAMO, vIDPRODUCT, vNUM_POLIZA, vIDMODALIDAD, vIDSUBMODALIDAD, vNUM_FIANZA, vNUM_EXPEDIENTE, vNUM_ANUALIDAD,
		-- vCOD_MEDIADOR_CEDENTE, vCOD_SUBCLAVE_CEDENTE, vCOD_MEDIADOR_RECEPTOR, vCOD_SUBCLAVE_RECEPTOR, vP_INTERMEDIACION, vINICIO_PERIODO,
		-- vFECHA_EFECTO_TRASPASO, vFECHA_INICIO, vFECHA_FIN, vUSUARIO, vEMAIL, vTIPO_CAMBIO, vTIPO_TRASPASO, vP_EMISION, vP_RENOVACION, vIND_COMISION,vESTADOREG
		-- FROM DUMMY;
		
	-- 	-- ----------------------------------------------------------------------------------------------------
	-- 	-- Se Busca el registro de CARTERA asociado con los porcentajes del plan del producto 
	-- 	-- y por defecto del producto
	-- 	-- ----------------------------------------------------------------------------------------------------
		SELECT  TOP 1 
			PC.P_EMISION, PC.P_RENOVACION,
			P.P_EMISION_DEF, P.P_RENOVACION_DEF,
			CASE WHEN C.P_ESPECIAL_EMISION is null or C.P_ESPECIAL_EMISION = 0 
    			THEN CASE WHEN PC.P_EMISION is null Then P.P_EMISION_DEF ELSE PC.P_EMISION END
    			ELSE C.P_ESPECIAL_EMISION END as P_EMISION_FIN,
			CASE WHEN C.P_ESPECIAL_RENOVACION is null or C.P_ESPECIAL_RENOVACION = 0 
    			THEN CASE WHEN PC.P_RENOVACION is null Then P.P_RENOVACION_DEF ELSE PC.P_RENOVACION END
    			ELSE C.P_ESPECIAL_RENOVACION END as P_RENOV_FIN
	 	into vPorc_Plan_Emision, vPorc_Plan_Renovacion,
			 vPorc_Emision_DEF,vPorc_Renovacion_DEF,
			 vPorc_Emision_Final, vPorc_Renovacion_Final
	 	default null, null, null, null, null, null
    	FROM EXT.CARTERA C
    		LEFT JOIN EXT.PLAN_COMISIONAMIENTO PC ON C.COD_MEDIADOR ||'-'|| C.COD_SUBCLAVE = PC.POSITIONNAME and C.IDPRODUCT = PC.IDPRODUCT 
    			and C.FECHA_EFECTO >= PC.EFFECTIVESTARTDATE and C.FECHA_EFECTO < PC.EFFECTIVEENDDATE 
    		LEFT JOIN EXT.PRODUCTOS_VW P ON C.IDPRODUCT = P.IDPRODUCT 
    	WHERE IDMODALIDAD=cur_row.IDMODALIDAD and NUM_POLIZA=cur_row.NUM_POLIZA and 
    			COD_MEDIADOR=cur_row.COD_MEDIADOR_RECEPTOR and COD_SUBCLAVE=cur_row.COD_SUBCLAVE_RECEPTOR
		ORDER BY NUM_ANUALIDAD desc, ACTIVO desc;
		
		IF ::ROWCOUNT > 0 THEN
		
	-- 	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'Buscar en EXT.CARTERA ' || IFNULL(TO_CHAR(vPIntemerdiacion),'') || '-'  || IFNULL(TO_CHAR(vNum_Anualidad),'') || '-' 
	-- 	 || IFNULL(TO_CHAR(vActivo),'') || '-'  || IFNULL(TO_CHAR(vPorc_Esp_Emision),'') || '-'  || IFNULL(TO_CHAR(vPorc_Esp_Renovacion),'') || '-'  
	-- 	 || IFNULL(TO_CHAR(vPorc_Plan_Emision),'') || '-'  || IFNULL(TO_CHAR(vPorc_Plan_Renovacion),'') || '-' 
	-- 	 || IFNULL(TO_CHAR(vPorc_Emision_DEF),'') || '-'  || IFNULL(TO_CHAR(vPorc_Renovacion_DEF),'') , cReportTable, io_contador);
	-- 	-- ----------------------------------------------------------------------------------------------------
	-- 	-- Se Inserta la Petición de Traspaso
	-- 	-- ----------------------------------------------------------------------------------------------------
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
			MODIF_DATE)
		VALUES(
			cur_row.RAMO,
			cur_row.IDPRODUCT,
			cur_row.NUM_POLIZA,
			cur_row.IDMODALIDAD,
			cur_row.IDSUBMODALIDAD,
	 		cur_row.NUM_FIANZA,  --NUM_FIANZA
			cur_row.NUM_EXPEDIENTE,
			cur_row.NUM_ANUALIDAD,
			cur_row.COD_MEDIADOR_CEDENTE,
			cur_row.COD_SUBCLAVE_CEDENTE,
			cur_row.COD_MEDIADOR_RECEPTOR,
			cur_row.COD_SUBCLAVE_RECEPTOR,
			cur_row.P_INTERMEDIACION,
			cur_row.INICIO_PERIODO, 
			cur_row.FECHA_EFECTO_TRASPASO,
	 		CASE WHEN cur_row.FECHA_INICIO = '' or cur_row.FECHA_INICIO  is null then '1990-12-31' else cur_row.FECHA_INICIO End,
			CASE WHEN cur_row.FECHA_FIN = '' or cur_row.FECHA_FIN = '0000-00-00' or cur_row.FECHA_FIN  is null  then '2200-01-01' else cur_row.FECHA_FIN End,
			cur_row.USUARIO,
			cur_row.EMAIL,
			cur_row.TIPO_CAMBIO, 
			cur_row.TIPO_TRASPASO, 
	 		vPorc_Emision_Final, --P_EMISION
	 		vPorc_Renovacion_Final, --P_RENOVACION
			cur_row.IND_COMISION,
			cur_row.ESTADOREG,
			cur_row.CASEID,
			CURRENT_TIMESTAMP 
			);
		ELSE -- No devuelve datos
			
			CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'NO ENCONTRADO', cReportTable, io_contador);	
															

		END IF;

	 END FOR;

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ', cReportTable, io_contador);

-- Fin procedimiento
END