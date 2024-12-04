CREATE FUNCTION EXT.GET_POLIZAS_CAUCION_ERROR() 
/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 27-Nov-2024 
	|---------------------------------------------------------------------------------------------- 
	| 
    | Muestra los registros que no existen en cartera
	| 
	| Version: 1	
	| 
	|
	----------------------------------------------------------------------------------------------- 
*/

	RETURNS TABLE (
		MOTIVO NVARCHAR(250)
		, IDMODALIDAD SMALLINT
		, NUM_POLIZA BIGINT
		, NUM_FIANZA BIGINT
		, IDMEDIADOR NVARCHAR(20)
		, FECHA_EFECTO DATE
		, FECHA_VENCIMIENTO DATE
		, FECHA_INICIO DATE
		, FECHA_FIN DATE
		, BATCHNAME NVARCHAR(250)
	) LANGUAGE SQLSCRIPT 
	AS
	BEGIN
		RETURN SELECT 
				CASE 
					WHEN NOT EXISTS(SELECT 1 FROM EXT.CARTERA WHERE RAMO = 'CAUCION' AND NUM_POLIZA = M.NUM_POLIZA) THEN 'PÓLIZA NO REGISTRADA EN CARTERA'
					WHEN NOT EXISTS(SELECT 1 FROM EXT.CARTERA WHERE RAMO = 'CAUCION' AND NUM_POLIZA = M.NUM_POLIZA AND NUM_FIANZA = M.NUM_AVAL_FIANZA) THEN 'FIANZA NO REGISTRADA EN CARTERA'
					WHEN NOT EXISTS(SELECT 1 FROM EXT.CARTERA WHERE RAMO = 'CAUCION' AND NUM_POLIZA = M.NUM_POLIZA AND NUM_FIANZA = M.NUM_AVAL_FIANZA
						AND M.IDMEDIADOR = COD_MEDIADOR||'-'||COD_SUBCLAVE) THEN 'MEDIADOR DISTINTO EN CARTERA'
					WHEN NOT EXISTS(SELECT 1 FROM EXT.CARTERA WHERE RAMO = 'CAUCION' AND NUM_POLIZA = M.NUM_POLIZA AND NUM_FIANZA = M.NUM_AVAL_FIANZA
						AND M.IDMEDIADOR = COD_MEDIADOR||'-'||COD_SUBCLAVE AND M.FECHA_EFECTO = FECHA_EFECTO) THEN 'FECHA EFECTO DISTINTA EN CARTERA'
						WHEN NOT EXISTS(SELECT 1 FROM EXT.CARTERA WHERE RAMO = 'CAUCION' AND NUM_POLIZA = M.NUM_POLIZA AND NUM_FIANZA = M.NUM_AVAL_FIANZA
						AND M.IDMEDIADOR = COD_MEDIADOR||'-'||COD_SUBCLAVE AND M.FECHA_EFECTO = FECHA_EFECTO AND M.FECHA_VENCIMIENTO = FECHA_VENCIMIENTO) THEN 'FECHA VENCIMIENTO DISTINTA EN CARTERA'
					ELSE 'OK. EXISTE EN CARTERA'
				END AS MOTIVO
				, M.IDMODALIDAD
				, M.NUM_POLIZA
				, M.NUM_AVAL_FIANZA AS NUM_FIANZA
				, M.IDMEDIADOR
				, M.FECHA_EFECTO
				, M.FECHA_VENCIMIENTO
				, M.FECHA_INI_COBERTURA AS FECHA_INICIO
				, M.FECHA_FIN_COBERTURA AS FECHA_FIN
				, M.BATCHNAME
			FROM EXT.EXT_MOVIMIENTO_FIANZAS_CAUCION_HIST M
			WHERE M.BATCHNAME IN (SELECT BATCHNAME FROM EXT.REGISTRO_INTERFACES WHERE NOTIFICATION = 0 AND BATCHNAME LIKE '%MVFID%') 
			AND NOT EXISTS(SELECT 1 FROM EXT.CARTERA WHERE RAMO = 'CAUCION' AND NUM_POLIZA = M.NUM_POLIZA AND NUM_FIANZA = M.NUM_AVAL_FIANZA
						AND M.IDMEDIADOR = COD_MEDIADOR||'-'||COD_SUBCLAVE AND M.FECHA_EFECTO = FECHA_EFECTO AND M.FECHA_VENCIMIENTO = FECHA_VENCIMIENTO)

			;
		
	END