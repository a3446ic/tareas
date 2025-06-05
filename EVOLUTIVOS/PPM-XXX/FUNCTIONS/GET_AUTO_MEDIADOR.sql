CREATE OR REPLACE FUNCTION EXT.GET_AUTO_MEDIADOR(filtro	VARCHAR(250), departamento VARCHAR(250), nombreDepartamento VARCHAR(250), tipoUsuario VARCHAR(250), nombre VARCHAR(250)) 
	RETURNS TABLE (cod_mediador NVARCHAR(250)) LANGUAGE SQLSCRIPT 
	AS
	BEGIN
		RETURN SELECT 
			COD_MEDIADOR || '-' || SUBCLAVE || ' ' || NUM_IDENTIFICACION || ' ' || IFNULL(NOMBRE, '') || ' ' || "APELLIDO/RAZON_SOCIAL" AS cod_mediador			
			FROM EXT.MODIFICAR_MEDIADOR 
			WHERE COD_MEDIADOR IS NOT NULL
			AND LOWER(COD_MEDIADOR || '-' || SUBCLAVE || ' ' || NUM_IDENTIFICACION || ' ' || IFNULL(NOMBRE, '') || ' ' || "APELLIDO/RAZON_SOCIAL") LIKE LOWER('%'||filtro||'%')
			AND (
				(departamento <> 'central_cesce' AND DIR_TERRITORIAL = nombreDepartamento)
				OR (departamento = 'central_cesce' AND tipoUsuario <> 'perfil_consulta_espana' AND DIR_TERRITORIAL LIKE '%')
				OR (departamento = 'central_cesce' AND tipoUsuario = 'perfil_consulta_espana' AND DIR_TERRITORIAL IN ('DT CENTRO','DT NORTE','DT SUR','DT CATALUÑA-BALEARES','DT LEVANTE','DT NOROESTE', 'DT PORTUGAL'))
			)
			AND UPPER(GERENTE_CANAL) LIKE CASE 
				WHEN tipoUsuario = 'gerente_canal' THEN nombre
				ELSE '%' 
			END
			ORDER BY 1
			;
		
	END