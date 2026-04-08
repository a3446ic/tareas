CREATE OR REPLACE FUNCTION EXT.GET_RESPUESTA_FACTURAS_ERROR() 
/*
	----------------------------------------------------------------------------------------------- 
	| Author: Samuel Miralles Manresa 
	| Company: Inycom 
	| Initial Version Date: 30-07-2025 
	|---------------------------------------------------------------------------------------------- 
	| fichero RESPFAC
    | Muestra los registros que contienen error
	| 
	| v01: versión inicial	
	| 
	|
	----------------------------------------------------------------------------------------------- 
*/

	RETURNS TABLE (
		MENSAJE VARCHAR(250)
		, IDFACTURA BIGINT
		, PROVEEDOR VARCHAR(12) 
		, REFERENCIA VARCHAR(12)
		, BATCHNAME VARCHAR(250)
	) LANGUAGE SQLSCRIPT 
	AS
	BEGIN
		RETURN SELECT 
				MENSAJE, IDFACTURA, PROVEEDOR, REFERENCIA, BATCHNAME
			FROM EXT.RESPUESTA_FACTURAS RF
			WHERE RF.BATCHNAME IN (SELECT BATCHNAME FROM EXT.REGISTRO_INTERFACES WHERE NOTIFICATION = 0 AND BATCHNAME LIKE '%RESPFAC%') 
			AND ESTADO = 'E'
			

			;
		
	END;
	
	DO BEGIN
	-- SELECT * FROM EXT.REGISTRO_INTERFACES WHERE 1=1
	-- -- AND NOTIFICATION = 0 
	-- AND BATCHNAME LIKE '%FAC%';
	-- --SELECT * FROM EXT.REGISTRO_INTERFACES WHERE NOTIFICATION = 0 AND BATCHNAME LIKE '%RESPFAC%';
	-- SELECT * FROM EXT.RESPUESTA_FACTURAS;
	
	-- DELETE FROM EXT.REGISTRO_INTERFACES WHERE BATCHNAME LIKE '%FAC%';
	SELECT * FROM EXT.GET_RESPUESTA_FACTURAS_ERROR();
	END;