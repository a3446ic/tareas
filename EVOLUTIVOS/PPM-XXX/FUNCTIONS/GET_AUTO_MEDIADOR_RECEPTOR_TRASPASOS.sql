CREATE OR REPLACE FUNCTION EXT.GET_AUTO_MEDIADOR_RECEPTOR_TRASPASOS(filtro	VARCHAR(250), departamento VARCHAR(250), nombreDepartamento VARCHAR(250)
	, pl_tipo_movimiento VARCHAR(250), auto_mediador VARCHAR(250)) 
	RETURNS TABLE (mediador NVARCHAR(250)) LANGUAGE SQLSCRIPT 
	AS
	BEGIN
		
		RETURN 
		SELECT COD_MEDIADOR || '-' || SUBCLAVE || ' ' || NUM_IDENTIFICACION || ' ' || IFNULL(NOMBRE, '') || ' ' || "APELLIDO/RAZON_SOCIAL" as mediador
		FROM EXT.MODIFICAR_MEDIADOR MM
		LEFT JOIN TCMP.CS_POSITION POS ON POS.NAME = MM.POSITIONNAME AND REMOVEDATE = TO_DATE('22000101','yyyymmdd') AND ISLAST = 1
		LEFT JOIN TCMP.CS_PROCESSINGUNIT pu
			ON pos.PROCESSINGUNITSEQ = pu.PROCESSINGUNITSEQ AND PU.NAME IN ('Spain','Portugal')
		WHERE COD_MEDIADOR IS NOT NULL 
	AND (
	    (pl_tipo_movimiento = 'traspaso_entre_subclaves' AND auto_mediador IS NOT NULL AND COD_MEDIADOR = SUBSTRING(auto_mediador, 0, 4) AND SUBCLAVE <> SUBSTRING(auto_mediador, 6, 4))
	    OR 
	    (pl_tipo_movimiento <> 'traspaso_entre_subclaves' OR auto_mediador IS NULL
	    	AND COD_MEDIADOR = SUBSTRING(auto_mediador, 0, 4) AND SUBCLAVE <> SUBSTRING(auto_mediador, 6, 4)
	    )
	)
	AND LOWER(COD_MEDIADOR || '-' || SUBCLAVE || ' ' || NUM_IDENTIFICACION || ' ' || IFNULL(NOMBRE, '') || ' ' || "APELLIDO/RAZON_SOCIAL") LIKE LOWER('%'||filtro||'%')
	AND DIR_TERRITORIAL LIKE 
		CASE 
			WHEN departamento = 'central_cesce' 
				THEN '%' 
				ELSE nombreDepartamento
		END
	AND 1 = (CASE
			WHEN COD_MEDIADOR = SUBSTRING(auto_mediador, 0, 4) AND SUBCLAVE = SUBSTRING(auto_mediador, 6, 4) THEN 0
			ELSE 1
			END
	)
	AND PU.NAME IS NOT NULL
	;
		
	END;
	
	DO BEGIN
		SELECT * FROM EXT.GET_AUTO_MEDIADOR_RECEPTOR_TRASPASOS('30','central_cesce','CENTRAL','pl_tipo_movimiento','0004-0000');
	END;