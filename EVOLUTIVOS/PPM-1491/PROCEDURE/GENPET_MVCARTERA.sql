CREATE PROCEDURE "EXT"."GENPET_MVCARTERA" (IN IN_FILENAME VARCHAR(120)) LANGUAGE SQLSCRIPT AS
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
		RAMO,
		IDPRODUCT,
		NUM_POLIZA,
		IDMODALIDAD,
		IDSUBMODALIDAD,
		NUM_FIANZA,  --NUM_FIANZA
		NUM_EXPEDIENTE,
		NUM_ANUALIDAD,
		COD_MEDIADOR, -- Código Mediador Cedente
		COD_SUBCLAVE, -- Subclave Mediador Cedente
		COD_MEDIADOR, -- Código Mediador Receptor 
		COD_SUBCLAVE, --  Subclave Mediador Receptor  
		P_INTERMEDIACION,
		'I',  --INICIO_PERIODO
		FECHA_EFECTO_TRASPASO,  -- FECHA_EFECTO_TRASPASO
		FECHA_INICIO,
		FECHA_FIN,
		'CDL',
		'email@callidusondemand.com',
		'C', -- Con derechos y obligaciones
		'S', -- Traspaso parcial
		P_ESPECIAL_EMISION, --P_EMISION
		P_ESPECIAL_RENOVACION, --P_RENOVACION
		'S', -- Con derechos y obligaciones
		'PENDIENTE',
		IN_FILENAME,
		CURRENT_TIMESTAMP
	FROM EXT.CARTERA
	WHERE MODIF_SOURCE = IN_FILENAME;

	CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'FIN PROCEDIMIENTO ', cReportTable, io_contador);

-- Fin procedimiento
END