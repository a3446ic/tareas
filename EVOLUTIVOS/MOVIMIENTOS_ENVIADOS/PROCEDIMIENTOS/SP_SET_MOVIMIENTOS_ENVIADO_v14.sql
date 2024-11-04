CREATE OR REPLACE PROCEDURE "EXT"."SP_SET_MOVIMIENTOS_ENVIADO" (IN tipoMov VARCHAR(5), IN IN_FILENAME VARCHAR(120)) LANGUAGE SQLSCRIPT SQL SECURITY DEFINER DEFAULT SCHEMA "EXT" AS
BEGIN

	----------------------------- Versiones ---------------------------------
	-- v06: Porcentajes especiales añadidos a las sentencias de insert y sentencia update para tablas hist no comprueba si registro en cartera
	-- v07: Para movimientos de fianzas, se ha modificado el insert y el update en cartera para que en el campo FECHA_EMISION se inserte o actualice con la fecha de efecto.
	-- v08: En cartera, se inserta activo a 0 si las fechas de inicio y fin no son correctas.
	-- v09: Se ha modificado el flujo para tener en cuenta el tipo de movimiento.
	-- v10: Se ha cambiado la comprobación de los registros duplicados para que detecte los de tipo de movimiento 2
	-- v11: Se añaden los nombre de campos de EXT_MOVIMIENTO_CARTERA_CREDITO_HIST para pbtener el COD_MEDIADOR y SUBCLAVE formateado a 4 digitos
	-- v12: Modificado el comportamiento cuando se reciben movimientos de tipo 2 para que contemple los casos en que una póliza es intermediada por múltiples mediadores.
	-- v13: Cambios en movimientos tipo 2 para que tenga en cuenta múltiples mediadores en una misma póliza
	-- v14: Si hay múltiples mediadores de traspaso se actualiza el registro con el nuevo mediador 
	-------------------------------------------------------------------------

	DECLARE cVersion CONSTANT VARCHAR(2) := '14';
	DECLARE i_Tenant VARCHAR2(127);
	DECLARE vProcedure VARCHAR2(127);
	DECLARE io_contador  INTEGER := 0;
	DECLARE transaccionesEncontradas INTEGER := 0;
	DECLARE numLin INTEGER := 0;
    DECLARE contador INTEGER;
    DECLARE registrosRenovacion INTEGER;
	DECLARE especialEmision DECIMAL(4,3) := 0;
	DECLARE especialRenovacion DECIMAL(4,3) := 0;
	DECLARE codigoMediador NVARCHAR(10);
	DECLARE subclaveMediador NVARCHAR(10);
	DECLARE anualidad SMALLINT;
	DECLARE fechaInicio DATE;
	DECLARE fechaFin DATE;
	DECLARE fechaVencimientoAnterior DATE;
	DECLARE fechaEfectoAnterior DATE;
	DECLARE mediadoresTraspaso TINYINT := 0;
	DECLARE registroExistente TINYINT := 0;

	DECLARE CURSOR mvcar_hist FOR
    SELECT IDMODALIDAD,NUM_POLIZA,IDFASE,DESC_FASE,IDESTADO,DESC_ESTADO,FECHA_SIT,NUM_ANUALIDAD,ID_SIT_ANUALIDAD,DESC_SIT_ANUALIDAD,
	FECHA_SIT_ANUALIDAD,FECHA_EMISION,FECHA_EFECTO,FECHA_VENCIMIENTO,IDCLIENTE,NOMBRE_CLIENTE,IND_VIP,IDGRUPO,DESC_GRUPO,IDDELEGACION,
	DESC_DELEGACION,IDCOMERCIAL,NOMBRE_COMERCIAL,IDOFICINA,DESC_OFICINA, LPAD(IDMEDIADOR, 4, '0') as IDMEDIADOR, 
	NOMBRE_MEDIADOR,IMPORTE_VENTAS_PREV_EXT,IMPORTE_VENTAS_PREV_INT,PRIMA_PROVISIONAL_INT,PRIMA_PROVISIONAL_EXT,
	PORC_TASA_INT,PORC_TASA_EXT,TIPO_TARIFA_INT,TIPO_TARIFA_EXT,IDMODALIDAD_POL_O,NUM_POL_O,FECHA_TRASPASO_POL_O,
	ID_SIT_POL_O,DESC_SIT_POL_O,IDMODALIDAD_POL_D,NUM_POL_D,FECHA_TRASPASO_POL_D,ID_SIT_POL_D,DESC_SIT_POL_D,FECHA_DATOS,
	IDCNAE,PR_COBRADA_RC_INT,PR_COBRADA_RC_EXT,PR_COBRADA_RP,PR_COBRADA_TC_RC,PR_COBRADA_TC_RP,PR_EMITIDA_RC_INT,PR_EMITIDA_RC_EXT,
	PR_EMITIDA_RP,PR_EMITIDA_TC_RC,PR_EMITIDA_TC_RP,IDMEDIADOR_2,NOMBRE_MEDIADOR_2,POR_COB_RC_INT,POR_COB_RC_EXT,POR_COB_RP,
	FECHA_PRIM_EMISION,IDESTADO_POL,DESC_ESTADO_POL,IDFISCAL_TOMADOR,IND_20X100,IND_PRORROGA,IDDIVISA_MERCADO_INT,IDDIVISA_MERCADO_EXT,
	IMPORTE_MAX_FINANCIACION,IMPORTE_FRANQUICIA,INDEMNIZACION_MAX,PORC_TASA_MAX,TARIFA_GASTOS_ANALISIS,
	PRIMA_MIN_MERCADO_INT,PRIMA_MIN_MERCADO_EXT,COD_ACUERDO,IDTIPO_MEDIADIOR,DESC_TIPO_MEDIADIOR,IDSEGMENTO_EMP,DESC_SEGMENTO_EMP,
	IND_FIRMA_DIGITAL,IDAGENTE,FECHA_INI,FECHA_FIN,DESC_SUBCLAVE,LPAD(IDSUBCLAVE, 4, '0') as IDSUBCLAVE, PORC_INTERMEDIACION,
	IDTIPO_MOV,BATCHNAME,CREATEDATE,ESTADOREG,IDPAIS 
	FROM EXT.EXT_MOVIMIENTO_CARTERA_CREDITO_HIST
    WHERE ESTADOREG = 'PENDIENTE';

	DECLARE CURSOR mvfid_hist FOR
	SELECT * FROM EXT.EXT_MOVIMIENTO_FIANZAS_CAUCION_HIST
	WHERE ESTADOREG = 'PENDIENTE';

----------------------------- HANDLER EXCEPTION -------------------------

	DECLARE EXIT HANDLER FOR SQLEXCEPTION 
	BEGIN
		CALL EXT.LIB_GLOBAL_CESCE:w_debug (i_Tenant, 'SQL_ERROR_MESSAGE: ' || 
			IFNULL(::SQL_ERROR_MESSAGE,'') || 
			'. SQL_ERROR_CODE: '||::SQL_ERROR_CODE, vProcedure , io_contador);
		RESIGNAL;
	END;
	
	vProcedure := 'SP_SET_MOVIMIENTOS_ENVIADO';

----Comienza a escribir en DEBUG

	SELECT TENANTID INTO i_Tenant FROM TCMP.CS_TENANT;

    CALL EXT.LIB_GLOBAL_CESCE :w_debug (i_Tenant, 'STARTING with SESSION_USER: ' || SESSION_USER, vProcedure || ' version ' || cVersion, io_contador);

---Comienza Procedimiento

    IF tipoMov = 'mvcar' THEN

		OPEN mvcar_hist;

    	FOR i AS mvcar_hist DO


			numLin:= numLin + 1;

			-- En caso de que no haya registros anteriores, se ponen los valores del movimiento como "por defecto"
			codigoMediador := i.IDMEDIADOR;
			subclaveMediador := i.IDSUBCLAVE;
			fechaInicio := i.FECHA_INI;
			fechaFin := i.FECHA_FIN;
			-------------------------------------------------------------------------------------------------------

			-- Se hacen distintas operaciones según el tipo de de movimiento
			IF i.IDTIPO_MOV = 2 THEN
				
				-- Se guarda la última anualidad (para posteriores queries)
				SELECT NUM_ANUALIDAD INTO anualidad DEFAULT i.NUM_ANUALIDAD
				FROM EXT.CARTERA
				WHERE NUM_POLIZA = i.NUM_POLIZA
				AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
				ORDER BY NUM_ANUALIDAD DESC
				LIMIT 1;


				-- Se busca el número de mediadores que tienen el activo = 2 (la póliza se ha traspasado)
				SELECT COALESCE(count(*), 0) INTO mediadoresTraspaso DEFAULT 0
				FROM EXT.CARTERA
				WHERE NUM_POLIZA = i.NUM_POLIZA
				AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
				AND ACTIVO = 2
				AND NUM_ANUALIDAD = anualidad;
				
				-- Se busca el número de mediadores que intermedian la póliza con activo = 1 (Se renueva sin traspaso)
				SELECT COALESCE(count(*), 1) INTO registrosRenovacion DEFAULT 1
				FROM EXT.CARTERA
				WHERE NUM_POLIZA = i.NUM_POLIZA
				AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
				AND ACTIVO = CASE WHEN :mediadoresTraspaso > 0 THEN 2 ELSE 1 END
				AND NUM_ANUALIDAD = anualidad;

				-- Se inserta o actualiza el registro de la renovación para cada mediador
				FOR contador IN 0..(registrosRenovacion-1) DO

				    -- Se obtienen las fechas de la anualidad anterior.
                    SELECT FECHA_INICIO, FECHA_FIN INTO fechaInicio, fechaFin
                        DEFAULT i.FECHA_INI, i.FECHA_FIN
					FROM EXT.CARTERA
					WHERE NUM_POLIZA = i.NUM_POLIZA
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
					AND ACTIVO = CASE WHEN :mediadoresTraspaso > 0 THEN 2 ELSE 1 END
					AND NUM_ANUALIDAD = anualidad
					AND FECHA_INICIO > :fechaInicio 
					ORDER BY COD_MEDIADOR DESC
					LIMIT 1
					OFFSET :contador;
                	-------------------------------------------------------------------------------------------------------

					-- Se obtiene el código de mediador de la anualidad anterior
					SELECT DISTINCT COD_MEDIADOR, COD_SUBCLAVE INTO codigoMediador, subclaveMediador
                    DEFAULT i.IDMEDIADOR, i.IDSUBCLAVE
					FROM EXT.CARTERA
					WHERE NUM_POLIZA = i.NUM_POLIZA
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
					AND ACTIVO = CASE WHEN :mediadoresTraspaso > 0 THEN 2 ELSE 1 END
					AND NUM_ANUALIDAD = anualidad
					ORDER BY COD_MEDIADOR DESC
					LIMIT 1
					OFFSET :contador;
					-------------------------------------------------------------------------------------------------------


                    SELECT COALESCE(COUNT(*),0) INTO registroExistente DEFAULT 0
                    FROM EXT.CARTERA
                    WHERE NUM_POLIZA = i.NUM_POLIZA 
                    AND COD_MEDIADOR = codigoMediador
                    AND COD_SUBCLAVE = subclaveMediador
                    AND NUM_ANUALIDAD = CASE WHEN ACTIVO = 2 THEN anualidad ELSE i.NUM_ANUALIDAD END
                    AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);

					IF registroExistente >= 1 THEN --UPDATE
						-- v14
						IF mediadoresTraspaso > 0 THEN	        
							UPDATE EXT.CARTERA SET
								NUM_ANUALIDAD = i.NUM_ANUALIDAD,
								FECHA_EMISION = i.FECHA_EMISION,
								FECHA_EFECTO = i.FECHA_EFECTO,
								FECHA_VENCIMIENTO = i.FECHA_VENCIMIENTO,
								IDPAIS = (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END),
								PRIMA_PROVISIONAL_INT = i.PRIMA_PROVISIONAL_INT,
								PRIMA_PROVISIONAL_EXT = i.PRIMA_PROVISIONAL_EXT,
								IDDIVISA_INT = i.IDDIVISA_MERCADO_INT,
								IDDIVISA_EXT = i.IDDIVISA_MERCADO_EXT,
								PRIMA_MIN_INT = i.PRIMA_MIN_MERCADO_INT,
								PRIMA_MIN_EXT = i.PRIMA_MIN_MERCADO_EXT,
								COD_MEDIADOR = codigoMediador,
								COD_SUBCLAVE = subclaveMediador,
								P_INTERMEDIACION = 100 * i.PORC_INTERMEDIACION,
								--FECHA_INICIO = i.FECHA_INI,
								FECHA_INICIO = fechaInicio,
								FECHA_FIN = fechaFin,
								NIF_TOMADOR = i.IDFISCAL_TOMADOR,
								NOMBRE_TOMADOR = '',  -- NO VIENE EN EL FICHERO
								--i.NOMBRE_TOMADOR,
								MEDIADOR_PRINCIPAL_CIC = 1,
								ACTIVO = 1,
								MODIF_DATE = CURRENT_TIMESTAMP,
								MODIF_USER = 'CDL',
								MODIF_SOURCE = IN_FILENAME
							WHERE NUM_POLIZA = i.NUM_POLIZA 
		                    AND COD_MEDIADOR = codigoMediador
		                    AND COD_SUBCLAVE = subclaveMediador
		                    AND NUM_ANUALIDAD = CASE WHEN ACTIVO = 2 THEN anualidad ELSE i.NUM_ANUALIDAD END
		                    AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);

						ELSE
							UPDATE EXT.CARTERA SET
								NUM_ANUALIDAD = i.NUM_ANUALIDAD,
								FECHA_EMISION = i.FECHA_EMISION,
								FECHA_EFECTO = i.FECHA_EFECTO,
								FECHA_VENCIMIENTO = i.FECHA_VENCIMIENTO,
								IDPAIS = (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END),
								PRIMA_PROVISIONAL_INT = i.PRIMA_PROVISIONAL_INT,
								PRIMA_PROVISIONAL_EXT = i.PRIMA_PROVISIONAL_EXT,
								IDDIVISA_INT = i.IDDIVISA_MERCADO_INT,
								IDDIVISA_EXT = i.IDDIVISA_MERCADO_EXT,
								PRIMA_MIN_INT = i.PRIMA_MIN_MERCADO_INT,
								PRIMA_MIN_EXT = i.PRIMA_MIN_MERCADO_EXT,
								COD_MEDIADOR = codigoMediador,
								COD_SUBCLAVE = subclaveMediador,
								P_INTERMEDIACION = 100 * i.PORC_INTERMEDIACION,
								--FECHA_INICIO = i.FECHA_INI,
								FECHA_INICIO = fechaInicio,
								FECHA_FIN = fechaFin,
								NIF_TOMADOR = i.IDFISCAL_TOMADOR,
								NOMBRE_TOMADOR = '',  -- NO VIENE EN EL FICHERO
								--i.NOMBRE_TOMADOR,
								MEDIADOR_PRINCIPAL_CIC = 1,
								ACTIVO = 1,
								MODIF_DATE = CURRENT_TIMESTAMP,
								MODIF_USER = 'CDL',
								MODIF_SOURCE = IN_FILENAME
							WHERE NUM_POLIZA = i.NUM_POLIZA 
							AND COD_MEDIADOR = i.IDMEDIADOR 
							AND COD_SUBCLAVE = i.IDSUBCLAVE 
							AND FECHA_INICIO = i.FECHA_INI
							AND FECHA_EFECTO = i.FECHA_EFECTO
							AND NUM_ANUALIDAD = i.NUM_ANUALIDAD
							--AND FECHA_VENCIMIENTO = i.FECHA_VENCIMIENTO 
							AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE :getProductId( (select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);
						END IF;

						CALL LIB_GLOBAL_CESCE :w_debug (
						i_Tenant,
						'Update línea '|| TO_VARCHAR (numLin) || ' registros actualizados '  || To_VARCHAR(::ROWCOUNT) || ' NUM_POLIZA:' || COALESCE(i.NUM_POLIZA, 0) || ',IDMODALIDAD:' || COALESCE(i.IDMODALIDAD, 0) || ',IDMEDIADOR:' || COALESCE (i.IDMEDIADOR, '0') ||
						',IDSUBCLAVE:' || COALESCE (i.IDSUBCLAVE, '0') || ',FECHA_INI:' || COALESCE (i.FECHA_INI, '0') || 
						',FECHA_FIN' || COALESCE (i.FECHA_FIN, '0') || ',TIPO_MOV:' || COALESCE (i.IDTIPO_MOV, 0),
						vProcedure,
						io_contador
						);

					ELSE --INSERT

						SELECT COALESCE(P_ESPECIAL_EMISION, 0), COALESCE(P_ESPECIAL_RENOVACION, 0) INTO especialEmision, especialRenovacion DEFAULT 0, 0
						FROM EXT.CARTERA
						WHERE NUM_POLIZA = i.NUM_POLIZA 
						AND COD_MEDIADOR = i.IDMEDIADOR 
						AND COD_SUBCLAVE = i.IDSUBCLAVE
						AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
						ORDER BY FECHA_VENCIMIENTO DESC LIMIT  1; 

						INSERT INTO EXT.CARTERA VALUES (
							'CREDITO',
							(SELECT EXT.LIB_GLOBAL_CESCE :getProductId( (select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY),
							i.NUM_POLIZA,
							i.IDMODALIDAD,
							NULL,
							0,
							NULL,
							NULL,
							i.NUM_ANUALIDAD,
							i.FECHA_EMISION,
							i.FECHA_EFECTO,
							i.FECHA_VENCIMIENTO,
							(CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END),
							i.PRIMA_PROVISIONAL_INT,
							i.PRIMA_PROVISIONAL_EXT,
							i.IDDIVISA_MERCADO_INT,
							i.IDDIVISA_MERCADO_EXT,
							NULL,
							i.PRIMA_MIN_MERCADO_INT,
							i.PRIMA_MIN_MERCADO_EXT,
							codigoMediador,
							subclaveMediador,
							( 100 * i.PORC_INTERMEDIACION),
							--i.FECHA_INI,
							fechaInicio,
							--i.FECHA_FIN,
							fechaFin,
							CASE WHEN especialEmision = 0 THEN NULL ELSE especialEmision END,
							CASE WHEN especialRenovacion = 0 THEN NULL ELSE especialRenovacion END,
							i.IDFISCAL_TOMADOR,
							'', --NOMBRE_TOMADOR  -- NO VIENE EN EL FICHERO
							NULL,
							1,
							CASE WHEN i.FECHA_INI = i.FECHA_FIN THEN 0 ELSE 1 END,
							CURRENT_TIMESTAMP,
							CURRENT_TIMESTAMP,
							'CDL',
							IN_FILENAME 
						);

						CALL LIB_GLOBAL_CESCE :w_debug (
						i_Tenant,
						'Insert línea '|| TO_VARCHAR (numLin) || ' registros insertados '  || To_VARCHAR(::ROWCOUNT) || ' NUM_POLIZA:' || COALESCE(i.NUM_POLIZA, 0) || ',IDMODALIDAD:' || COALESCE(i.IDMODALIDAD, 0) || ',IDMEDIADOR:' || COALESCE (i.IDMEDIADOR, '0') ||
						',IDSUBCLAVE:' || COALESCE (i.IDSUBCLAVE, '0') || ',FECHA_INI:' || COALESCE (i.FECHA_INI, '0') || 
						',FECHA_FIN' || COALESCE (i.FECHA_FIN, '0') || ',TIPO_MOV:' || COALESCE (i.IDTIPO_MOV, 0),
						vProcedure,
						io_contador
						);

					END IF;

				END FOR;

            ELSE

                IF i.IDTIPO_MOV = 3 THEN

                    -- Se obtiene la fecha de vencimiento y de efecto ya existentes para, posteriormente, comparar cuál se ha acortado
                    SELECT DISTINCT FECHA_VENCIMIENTO, FECHA_EFECTO INTO fechaVencimientoAnterior, fechaEfectoAnterior 
                        DEFAULT i.FECHA_VENCIMIENTO, i.FECHA_EFECTO
                    FROM EXT.CARTERA
                    WHERE NUM_POLIZA = i.NUM_POLIZA 
                    AND COD_MEDIADOR = i.IDMEDIADOR 
                    AND COD_SUBCLAVE = i.IDSUBCLAVE
                    AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
                    AND NUM_ANUALIDAD = i.NUM_ANUALIDAD
                    AND FECHA_INICIO = i.FECHA_INI;
                    -------------------------------------------------------------------------------------------------------

                    -- Si se ha acortado la fecha de vencimiento, se modifican las fechas de inicio y efecto del traspaso (en caso de que lo hubiera)
                    IF fechaVencimientoAnterior > i.FECHA_VENCIMIENTO THEN
                        UPDATE EXT.CARTERA SET 
                            FECHA_INICIO = ADD_DAYS(i.FECHA_VENCIMIENTO, 1),
                            FECHA_EFECTO = ADD_DAYS(i.FECHA_VENCIMIENTO, 1)
                        WHERE NUM_POLIZA = i.NUM_POLIZA
                        AND ACTIVO = 2
                        AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);
                    END IF;
                    -------------------------------------------------------------------------------------------------------

                END IF;
                -------------------------------------------------------------------------------------------------------
            
				-- Se busca si ya existe la póliza en Cartera
				IF i.IDTIPO_MOV = 3 THEN
					SELECT COALESCE(COUNT(*),0) INTO registroExistente DEFAULT 0
					FROM EXT.CARTERA
					WHERE NUM_POLIZA = i.NUM_POLIZA 
					AND COD_MEDIADOR = i.IDMEDIADOR 
					AND COD_SUBCLAVE = i.IDSUBCLAVE
					AND (FECHA_EFECTO = i.FECHA_EFECTO OR fechaVencimientoAnterior = i.FECHA_VENCIMIENTO)
					AND FECHA_INICIO = i.FECHA_INI
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);
				ELSE
					SELECT COALESCE(COUNT(*),0) INTO registroExistente DEFAULT 0
					FROM EXT.CARTERA
					WHERE NUM_POLIZA = i.NUM_POLIZA 
					AND COD_MEDIADOR = i.IDMEDIADOR 
					AND COD_SUBCLAVE = i.IDSUBCLAVE
					AND FECHA_EFECTO = i.FECHA_EFECTO
					AND FECHA_INICIO = fechaInicio
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);
				END IF;
				-------------------------------------------------------------------------------------------------------
			
				IF registroExistente >= 1 THEN --UPDATE
        
					UPDATE EXT.CARTERA SET
						NUM_ANUALIDAD = i.NUM_ANUALIDAD,
						FECHA_EMISION = i.FECHA_EMISION,
						FECHA_EFECTO = i.FECHA_EFECTO,
						FECHA_VENCIMIENTO = i.FECHA_VENCIMIENTO,
						IDPAIS = (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END),
						PRIMA_PROVISIONAL_INT = i.PRIMA_PROVISIONAL_INT,
						PRIMA_PROVISIONAL_EXT = i.PRIMA_PROVISIONAL_EXT,
						IDDIVISA_INT = i.IDDIVISA_MERCADO_INT,
						IDDIVISA_EXT = i.IDDIVISA_MERCADO_EXT,
						PRIMA_MIN_INT = i.PRIMA_MIN_MERCADO_INT,
						PRIMA_MIN_EXT = i.PRIMA_MIN_MERCADO_EXT,
						COD_MEDIADOR = codigoMediador,
						COD_SUBCLAVE = subclaveMediador,
						P_INTERMEDIACION = 100 * i.PORC_INTERMEDIACION,
						--FECHA_INICIO = i.FECHA_INI,
						FECHA_INICIO = fechaInicio,
						FECHA_FIN = fechaFin,
						NIF_TOMADOR = i.IDFISCAL_TOMADOR,
						NOMBRE_TOMADOR = '',  -- NO VIENE EN EL FICHERO
						--i.NOMBRE_TOMADOR,
						MEDIADOR_PRINCIPAL_CIC = 1,
						ACTIVO = CASE WHEN i.FECHA_INI = i.FECHA_FIN THEN 0 ELSE 1 END,
						MODIF_DATE = CURRENT_TIMESTAMP,
						MODIF_USER = 'CDL',
						MODIF_SOURCE = IN_FILENAME
					WHERE NUM_POLIZA = i.NUM_POLIZA 
					AND COD_MEDIADOR = i.IDMEDIADOR 
					AND COD_SUBCLAVE = i.IDSUBCLAVE 
					AND FECHA_INICIO = i.FECHA_INI
					AND FECHA_EFECTO = i.FECHA_EFECTO
					AND NUM_ANUALIDAD = i.NUM_ANUALIDAD
					--AND FECHA_VENCIMIENTO = i.FECHA_VENCIMIENTO 
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE :getProductId( (select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);


					CALL LIB_GLOBAL_CESCE :w_debug (
					i_Tenant,
					'Update línea '|| TO_VARCHAR (numLin) || ' registros actualizados '  || To_VARCHAR(::ROWCOUNT) || ' NUM_POLIZA:' || COALESCE(i.NUM_POLIZA, 0) || ',IDMODALIDAD:' || COALESCE(i.IDMODALIDAD, 0) || ',IDMEDIADOR:' || COALESCE (i.IDMEDIADOR, '0') ||
					',IDSUBCLAVE:' || COALESCE (i.IDSUBCLAVE, '0') || ',FECHA_INI:' || COALESCE (i.FECHA_INI, '0') || 
					',FECHA_FIN' || COALESCE (i.FECHA_FIN, '0') || ',TIPO_MOV:' || COALESCE (i.IDTIPO_MOV, 0),
					vProcedure,
					io_contador
					);


				ELSE --INSERT

					SELECT COALESCE(P_ESPECIAL_EMISION, 0), COALESCE(P_ESPECIAL_RENOVACION, 0) INTO especialEmision, especialRenovacion DEFAULT 0, 0
					FROM EXT.CARTERA
					WHERE NUM_POLIZA = i.NUM_POLIZA 
					AND COD_MEDIADOR = i.IDMEDIADOR 
					AND COD_SUBCLAVE = i.IDSUBCLAVE
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
					ORDER BY FECHA_VENCIMIENTO DESC LIMIT  1; 

					INSERT INTO EXT.CARTERA VALUES (
						'CREDITO',
						(SELECT EXT.LIB_GLOBAL_CESCE :getProductId( (select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY),
						i.NUM_POLIZA,
						i.IDMODALIDAD,
						NULL,
						0,
						NULL,
						NULL,
						i.NUM_ANUALIDAD,
						i.FECHA_EMISION,
						i.FECHA_EFECTO,
						i.FECHA_VENCIMIENTO,
						(CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END),
						i.PRIMA_PROVISIONAL_INT,
						i.PRIMA_PROVISIONAL_EXT,
						i.IDDIVISA_MERCADO_INT,
						i.IDDIVISA_MERCADO_EXT,
						NULL,
						i.PRIMA_MIN_MERCADO_INT,
						i.PRIMA_MIN_MERCADO_EXT,
						codigoMediador,
						subclaveMediador,
						( 100 * i.PORC_INTERMEDIACION),
						--i.FECHA_INI,
						fechaInicio,
						--i.FECHA_FIN,
						fechaFin,
						CASE WHEN especialEmision = 0 THEN NULL ELSE especialEmision END,
						CASE WHEN especialRenovacion = 0 THEN NULL ELSE especialRenovacion END,
						i.IDFISCAL_TOMADOR,
						'', --NOMBRE_TOMADOR  -- NO VIENE EN EL FICHERO
						NULL,
						1,
						CASE WHEN i.FECHA_INI = i.FECHA_FIN THEN 0 ELSE 1 END,
						CURRENT_TIMESTAMP,
						CURRENT_TIMESTAMP,
						'CDL',
						IN_FILENAME 
					);

					CALL LIB_GLOBAL_CESCE :w_debug (
					i_Tenant,
					'Insert línea '|| TO_VARCHAR (numLin) || ' registros insertados '  || To_VARCHAR(::ROWCOUNT) || ' NUM_POLIZA:' || COALESCE(i.NUM_POLIZA, 0) || ',IDMODALIDAD:' || COALESCE(i.IDMODALIDAD, 0) || ',IDMEDIADOR:' || COALESCE (i.IDMEDIADOR, '0') ||
					',IDSUBCLAVE:' || COALESCE (i.IDSUBCLAVE, '0') || ',FECHA_INI:' || COALESCE (i.FECHA_INI, '0') || 
					',FECHA_FIN' || COALESCE (i.FECHA_FIN, '0') || ',TIPO_MOV:' || COALESCE (i.IDTIPO_MOV, 0),
					vProcedure,
					io_contador
					);

        		END IF;
			END IF;
    	END FOR;

		CLOSE mvcar_hist;

		UPDATE EXT.EXT_MOVIMIENTO_CARTERA_CREDITO_HIST SET ESTADOREG = 'ENVIADO'
		WHERE ESTADOREG = 'PENDIENTE';

        COMMIT;
		
   ELSEIF tipoMov = 'mvfid' THEN

		OPEN mvfid_hist;

		FOR i AS mvfid_hist DO

			numLin:= numLin + 1;

			-- En caso de que no haya registros anteriores, se ponen los valores del movimiento como "por defecto"
			codigoMediador := SUBSTR_BEFORE(i.IDMEDIADOR,'-');
			subclaveMediador :=  SUBSTR_AFTER(i.IDMEDIADOR,'-');
			fechaInicio := i.FECHA_EFECTO;
			fechaFin := i.FECHA_VENCIMIENTO;
			-------------------------------------------------------------------------------------------------------
			
			-- Se hacen distintas operaciones seg�n el tipo de de movimiento
			IF i.IDTIPO_MOV = 2 THEN

				-- Se busca el número de mediadores que tienen el activo = 2 (la póliza se ha traspasado)
				SELECT COALESCE(count(*), 0) INTO mediadoresTraspaso DEFAULT 0
				FROM EXT.CARTERA
				WHERE NUM_POLIZA = i.NUM_POLIZA
				AND NUM_FIANZA = i.NUM_AVAL_FIANZA
				AND NUM_AVAL_HOST = i.NUM_AVAL_HOST
				AND ACTIVO = 2
				AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId( (select lpad(i.IDMODALIDAD, 3, '0') from dummy), i.IDSUBMODALIDAD, (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN '116' ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);

				-- Se busca el número de mediadores que intermedian la póliza con activo = 1 (Se renueva sin traspaso)
				SELECT COALESCE(count(*), 1) INTO registrosRenovacion DEFAULT 1
				FROM EXT.CARTERA
				WHERE NUM_POLIZA = i.NUM_POLIZA
				AND NUM_FIANZA = i.NUM_AVAL_FIANZA
				AND NUM_AVAL_HOST = i.NUM_AVAL_HOST
				AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
				AND ACTIVO = CASE WHEN :mediadoresTraspaso > 0 THEN 2 ELSE 1 END
				AND FECHA_EFECTO = i.FECHA_EFECTO
				AND FECHA_INICIO = fechaInicio;

				-- Se inserta o actualiza el registro de la renovación para cada mediador
				FOR contador IN 0..(registrosRenovacion-1) DO

					-- Se obtiene el código de mediador del registro anterior
					SELECT DISTINCT COD_MEDIADOR, COD_SUBCLAVE INTO codigoMediador, subclaveMediador
                    	DEFAULT SUBSTR_BEFORE(i.IDMEDIADOR,'-'), SUBSTR_AFTER(i.IDMEDIADOR,'-') 
					FROM EXT.CARTERA
					WHERE NUM_POLIZA = i.NUM_POLIZA
					AND NUM_FIANZA = i.NUM_AVAL_FIANZA
					AND NUM_AVAL_HOST = i.NUM_AVAL_HOST
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
					AND ACTIVO = CASE WHEN :mediadoresTraspaso > 0 THEN 2 ELSE 1 END
					ORDER BY COD_MEDIADOR DESC
					LIMIT 1
					OFFSET :contador;
					-------------------------------------------------------------------------------------------------------

                    SELECT COALESCE(COUNT(*),0) INTO registroExistente DEFAULT 0
                    FROM EXT.CARTERA
                    WHERE NUM_POLIZA = i.NUM_POLIZA 
                    AND COD_MEDIADOR = codigoMediador
                    AND COD_SUBCLAVE = subclaveMediador
					AND NUM_FIANZA = i.NUM_AVAL_FIANZA
					AND NUM_AVAL_HOST = i.NUM_AVAL_HOST
					AND FECHA_EFECTO = i.FECHA_EFECTO
					AND FECHA_INICIO = fechaInicio
                    AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);

					IF registroExistente >= 1 THEN

						UPDATE EXT.CARTERA SET
							NUM_EXPEDIENTE = i.NUM_EXPEDIENTE,
							NUM_AVAL_HOST = i.NUM_AVAL_HOST,
							FECHA_EFECTO = i.FECHA_EFECTO,
							FECHA_VENCIMIENTO = i.FECHA_VENCIMIENTO,
							FECHA_EMISION = i.FECHA_EFECTO,
							IDPAIS = i.IDPAIS,
							IDDIVISA_COBERTURA = i.IDDIVISA_COBERTURA,
							COD_MEDIADOR = codigoMediador,
							COD_SUBCLAVE = subclaveMediador,
							P_INTERMEDIACION = 100.0,
							FECHA_INICIO = fechaInicio,
							FECHA_FIN = CASE WHEN (fechaFin is null or fechaFin = '2099-12-31') THEN TO_DATE('2200-01-01','YYYY-MM-DD') ELSE fechaFin END,
							NIF_TOMADOR = i.IDFISCAL_TOMADOR,
							NOMBRE_TOMADOR = i.NOMBRE_TOMADOR,
							MEDIADOR_PRINCIPAL_CIC = 1,
							ACTIVO = 1,
							MODIF_DATE = CURRENT_TIMESTAMP,
							MODIF_USER = 'CDL',
							MODIF_SOURCE = IN_FILENAME
						WHERE IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), i.IDSUBMODALIDAD, (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN '116' ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
						AND NUM_POLIZA = i.NUM_POLIZA
						AND NUM_FIANZA = i.NUM_AVAL_FIANZA
						AND COD_MEDIADOR = SUBSTR_BEFORE(i.IDMEDIADOR,'-')
						AND COD_SUBCLAVE = SUBSTR_AFTER(i.IDMEDIADOR,'-')
						AND FECHA_INICIO = i.FECHA_EFECTO
						AND NUM_AVAL_HOST = i.NUM_AVAL_HOST;

						CALL LIB_GLOBAL_CESCE :w_debug (
						i_Tenant,
						'Update linea '|| TO_VARCHAR (numLin) || ' registros actualizados '  || To_VARCHAR(::ROWCOUNT) || ' NUM_POLIZA:' || COALESCE(i.NUM_POLIZA, 0) || ',COD_AVAL:' || COALESCE(i.NUM_AVAL_HOST, 0) || ',IDMODALIDAD:' || COALESCE(i.IDMODALIDAD, 0) || ',IDSUBMODALIDAD:' || COALESCE(i.IDSUBMODALIDAD, '0') || 
						',IDMEDIADOR:' || COALESCE (i.IDMEDIADOR, '0') || ',FECHA_EFECTO:' || COALESCE (i.FECHA_EFECTO, '0') || ',FECHA_VENCIMIENTO' || COALESCE (i.FECHA_VENCIMIENTO, '0') || 
						',TIPO_MOV:' || COALESCE (i.IDTIPO_MOV, 0),
						vProcedure,
						io_contador
						);

					ELSE

						SELECT COALESCE(P_ESPECIAL_EMISION, 0), COALESCE(P_ESPECIAL_RENOVACION, 0) INTO especialEmision, especialRenovacion DEFAULT 0, 0
						FROM EXT.CARTERA
						WHERE NUM_POLIZA = i.NUM_POLIZA 
						AND COD_MEDIADOR = SUBSTR_BEFORE(i.IDMEDIADOR,'-') 
						AND COD_SUBCLAVE = SUBSTR_AFTER(i.IDMEDIADOR,'-')
						AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
						ORDER BY FECHA_VENCIMIENTO DESC LIMIT  1; 


						INSERT INTO EXT.CARTERA VALUES(
							'CAUCION',
							(SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy) , i.IDSUBMODALIDAD, (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN '116' ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY),
							i.NUM_POLIZA,
							i.IDMODALIDAD,
							i.IDSUBMODALIDAD,
							i.NUM_AVAL_FIANZA,
							i.NUM_EXPEDIENTE,
							i.NUM_AVAL_HOST,
							0,
							i.FECHA_EFECTO, -- FECHA EMISION
							i.FECHA_EFECTO,
							i.FECHA_VENCIMIENTO,
							i.IDPAIS,
							NULL,
							NULL,
							NULL,
							NULL,
							i.IDDIVISA_COBERTURA,
							NULL,
							NULL, 
							codigoMediador,
							subclaveMediador,
							100.00,
							fechaInicio,
							fechaFin,
							CASE WHEN especialEmision = 0 THEN NULL ELSE especialEmision END,
							CASE WHEN especialRenovacion = 0 THEN NULL ELSE especialRenovacion END,
							i.IDFISCAL_TOMADOR,
							i.NOMBRE_TOMADOR,
							NULL,
							1,
							1,
							CURRENT_TIMESTAMP,
							CURRENT_TIMESTAMP,
							'CDL',
							IN_FILENAME
						);

						CALL LIB_GLOBAL_CESCE :w_debug (
						i_Tenant,
						'Insert linea '|| TO_VARCHAR (numLin) || ' registros insertados '  || To_VARCHAR(::ROWCOUNT) || ' NUM_POLIZA:' || COALESCE(i.NUM_POLIZA, 0) || ',COD_AVAL:' || COALESCE(i.NUM_AVAL_HOST, 0) || ',IDMODALIDAD:' || COALESCE(i.IDMODALIDAD, 0) || ',IDSUBMODALIDAD:' || COALESCE(i.IDSUBMODALIDAD, '0') || 
						',IDMEDIADOR:' || COALESCE (i.IDMEDIADOR, '0') || ',FECHA_EFECTO:' || COALESCE (i.FECHA_EFECTO, '0') || ',FECHA_VENCIMIENTO' || COALESCE (i.FECHA_VENCIMIENTO, '0') || 
						',TIPO_MOV:' || COALESCE (i.IDTIPO_MOV, 0),
						vProcedure,
						io_contador
						);

					END IF;

				END FOR;

			ELSE

				IF i.IDTIPO_MOV = 3 THEN

					-- Se obtiene la fecha de vencimiento y de efecto ya existentes para, posteriormente, comparar cu�l se ha acortado
					SELECT DISTINCT FECHA_VENCIMIENTO, FECHA_EFECTO INTO fechaVencimientoAnterior, fechaEfectoAnterior 
						DEFAULT i.FECHA_VENCIMIENTO, i.FECHA_EFECTO
					FROM EXT.CARTERA
					WHERE NUM_POLIZA = i.NUM_POLIZA
					AND NUM_FIANZA = i.NUM_AVAL_FIANZA
					AND NUM_AVAL_HOST = i.NUM_AVAL_HOST
					AND COD_MEDIADOR = SUBSTR_BEFORE(i.IDMEDIADOR,'-') 
					AND COD_SUBCLAVE = SUBSTR_AFTER(i.IDMEDIADOR,'-')
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId( (select lpad(i.IDMODALIDAD, 3, '0') from dummy), i.IDSUBMODALIDAD, (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN '116' ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
					AND FECHA_INICIO = i.FECHA_EFECTO;
					-------------------------------------------------------------------------------------------------------

					-- Si se ha acortado la fecha de vencimiento, se modifican las fechas de inicio y efecto del traspaso (en caso de que lo hubiera)
					IF fechaVencimientoAnterior > i.FECHA_VENCIMIENTO THEN
						UPDATE EXT.CARTERA SET 
							FECHA_INICIO = ADD_DAYS(i.FECHA_VENCIMIENTO, 1),
							FECHA_EFECTO = ADD_DAYS(i.FECHA_VENCIMIENTO, 1)
						WHERE NUM_POLIZA = i.NUM_POLIZA
						AND NUM_FIANZA = i.NUM_AVAL_FIANZA
						AND NUM_AVAL_HOST = i.NUM_AVAL_HOST
						AND ACTIVO = 2
						AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId( (select lpad(i.IDMODALIDAD, 3, '0') from dummy), i.IDSUBMODALIDAD, (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN '116' ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);
					END IF;
					-------------------------------------------------------------------------------------------------------
				END IF;
				-------------------------------------------------------------------------------------------------------

				-- Se busca si ya existe la p�liza en Cartera
				IF i.IDTIPO_MOV = 3 THEN
					SELECT COALESCE(COUNT(*),0) INTO registroExistente DEFAULT 0
					FROM EXT.CARTERA
					WHERE NUM_POLIZA = i.NUM_POLIZA 
					AND COD_MEDIADOR = SUBSTR_BEFORE(i.IDMEDIADOR,'-') 
					AND COD_SUBCLAVE = SUBSTR_AFTER(i.IDMEDIADOR,'-')
					AND NUM_FIANZA = i.NUM_AVAL_FIANZA
					AND NUM_AVAL_HOST = i.NUM_AVAL_HOST
					AND	(FECHA_EFECTO = i.FECHA_EFECTO OR fechaVencimientoAnterior = i.FECHA_VENCIMIENTO)
					AND FECHA_INICIO = i.FECHA_EFECTO
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId( (select lpad(i.IDMODALIDAD, 3, '0') from dummy), i.IDSUBMODALIDAD, (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN '116' ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);
				ELSE
					SELECT COALESCE(COUNT(*),0) INTO registroExistente DEFAULT 0
					FROM EXT.CARTERA
					WHERE NUM_POLIZA = i.NUM_POLIZA 
					AND COD_MEDIADOR = SUBSTR_BEFORE(i.IDMEDIADOR,'-')
					AND COD_SUBCLAVE = SUBSTR_AFTER(i.IDMEDIADOR,'-')
					AND NUM_FIANZA = i.NUM_AVAL_FIANZA
					AND NUM_AVAL_HOST = i.NUM_AVAL_HOST
					AND FECHA_EFECTO = i.FECHA_EFECTO
					AND FECHA_INICIO = fechaInicio
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId( (select lpad(i.IDMODALIDAD, 3, '0') from dummy), i.IDSUBMODALIDAD, (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN '116' ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY);
				END IF;
				-------------------------------------------------------------------------------------------------------

				IF registroExistente >= 1 THEN

					UPDATE EXT.CARTERA SET
						NUM_EXPEDIENTE = i.NUM_EXPEDIENTE,
						NUM_AVAL_HOST = i.NUM_AVAL_HOST,
						FECHA_EFECTO = i.FECHA_EFECTO,
						FECHA_VENCIMIENTO = i.FECHA_VENCIMIENTO,
						FECHA_EMISION = i.FECHA_EFECTO,
						IDPAIS = i.IDPAIS,
						IDDIVISA_COBERTURA = i.IDDIVISA_COBERTURA,
						COD_MEDIADOR = codigoMediador,
						COD_SUBCLAVE = subclaveMediador,
						P_INTERMEDIACION = 100.0,
						FECHA_INICIO = fechaInicio,
						FECHA_FIN = CASE WHEN (fechaFin is null or fechaFin = '2099-12-31') THEN TO_DATE('2200-01-01','YYYY-MM-DD') ELSE fechaFin END,
						NIF_TOMADOR = i.IDFISCAL_TOMADOR,
						NOMBRE_TOMADOR = i.NOMBRE_TOMADOR,
						MEDIADOR_PRINCIPAL_CIC = 1,
						ACTIVO = 1,
						MODIF_DATE = CURRENT_TIMESTAMP,
						MODIF_USER = 'CDL',
						MODIF_SOURCE = IN_FILENAME
					WHERE IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), i.IDSUBMODALIDAD, (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN '116' ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
					AND NUM_POLIZA = i.NUM_POLIZA
					AND NUM_FIANZA = i.NUM_AVAL_FIANZA
					AND COD_MEDIADOR = SUBSTR_BEFORE(i.IDMEDIADOR,'-')
					AND COD_SUBCLAVE = SUBSTR_AFTER(i.IDMEDIADOR,'-')
					AND FECHA_INICIO = i.FECHA_EFECTO
					AND NUM_AVAL_HOST = i.NUM_AVAL_HOST;

					CALL LIB_GLOBAL_CESCE :w_debug (
					i_Tenant,
					'Update linea '|| TO_VARCHAR (numLin) || ' registros actualizados '  || To_VARCHAR(::ROWCOUNT) || ' NUM_POLIZA:' || COALESCE(i.NUM_POLIZA, 0) || ',COD_AVAL:' || COALESCE(i.NUM_AVAL_HOST, 0) || ',IDMODALIDAD:' || COALESCE(i.IDMODALIDAD, 0) || ',IDSUBMODALIDAD:' || COALESCE(i.IDSUBMODALIDAD, '0') || 
					',IDMEDIADOR:' || COALESCE (i.IDMEDIADOR, '0') || ',FECHA_EFECTO:' || COALESCE (i.FECHA_EFECTO, '0') || ',FECHA_VENCIMIENTO' || COALESCE (i.FECHA_VENCIMIENTO, '0') || 
					',TIPO_MOV:' || COALESCE (i.IDTIPO_MOV, 0),
					vProcedure,
					io_contador
					);

				ELSE

					SELECT COALESCE(P_ESPECIAL_EMISION, 0), COALESCE(P_ESPECIAL_RENOVACION, 0) INTO especialEmision, especialRenovacion DEFAULT 0, 0
					FROM EXT.CARTERA
					WHERE NUM_POLIZA = i.NUM_POLIZA 
					AND COD_MEDIADOR = SUBSTR_BEFORE(i.IDMEDIADOR,'-') 
					AND COD_SUBCLAVE = SUBSTR_AFTER(i.IDMEDIADOR,'-')
					AND IDPRODUCT = (SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy), '0', (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN 116 ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY)
					ORDER BY FECHA_VENCIMIENTO DESC LIMIT  1; 


					INSERT INTO EXT.CARTERA VALUES(
						'CAUCION',
						(SELECT EXT.LIB_GLOBAL_CESCE:getProductId((select lpad(i.IDMODALIDAD, 3, '0') from dummy) , i.IDSUBMODALIDAD, (CASE WHEN (i.IDPAIS > 0 AND i.IDPAIS <= 52) THEN '116' ELSE i.IDPAIS END), i.NUM_POLIZA).productId FROM DUMMY),
						i.NUM_POLIZA,
						i.IDMODALIDAD,
						i.IDSUBMODALIDAD,
						i.NUM_AVAL_FIANZA,
						i.NUM_EXPEDIENTE,
						i.NUM_AVAL_HOST,
						0,
						i.FECHA_EFECTO, -- FECHA EMISION
						i.FECHA_EFECTO,
						i.FECHA_VENCIMIENTO,
						i.IDPAIS,
						NULL,
						NULL,
						NULL,
						NULL,
						i.IDDIVISA_COBERTURA,
						NULL,
						NULL, 
						CASE WHEN i.IDTIPO_MOV = 2 THEN codigoMediador ELSE SUBSTR_BEFORE(i.IDMEDIADOR,'-') END,
						CASE WHEN i.IDTIPO_MOV = 2 THEN subclaveMediador ELSE SUBSTR_AFTER(i.IDMEDIADOR,'-') END,
						100.00,
						CASE WHEN i.IDTIPO_MOV = 2 THEN fechaInicio ELSE i.FECHA_EFECTO END,
						CASE WHEN i.IDTIPO_MOV = 2 THEN fechaFin ELSE (CASE WHEN i.FECHA_VENCIMIENTO is null or i.FECHA_VENCIMIENTO = '2099-12-31' then TO_DATE('2200-01-01','YYYY-MM-DD') else i.FECHA_VENCIMIENTO END) END,
						CASE WHEN especialEmision = 0 THEN NULL ELSE especialEmision END,
						CASE WHEN especialRenovacion = 0 THEN NULL ELSE especialRenovacion END,
						i.IDFISCAL_TOMADOR,
						i.NOMBRE_TOMADOR,
						NULL,
						1,
						1,
						CURRENT_TIMESTAMP,
						CURRENT_TIMESTAMP,
						'CDL',
						IN_FILENAME
					);

					CALL LIB_GLOBAL_CESCE :w_debug (
					i_Tenant,
					'Insert linea '|| TO_VARCHAR (numLin) || ' registros insertados '  || To_VARCHAR(::ROWCOUNT) || ' NUM_POLIZA:' || COALESCE(i.NUM_POLIZA, 0) || ',COD_AVAL:' || COALESCE(i.NUM_AVAL_HOST, 0) || ',IDMODALIDAD:' || COALESCE(i.IDMODALIDAD, 0) || ',IDSUBMODALIDAD:' || COALESCE(i.IDSUBMODALIDAD, '0') || 
					',IDMEDIADOR:' || COALESCE (i.IDMEDIADOR, '0') || ',FECHA_EFECTO:' || COALESCE (i.FECHA_EFECTO, '0') || ',FECHA_VENCIMIENTO' || COALESCE (i.FECHA_VENCIMIENTO, '0') || 
					',TIPO_MOV:' || COALESCE (i.IDTIPO_MOV, 0),
					vProcedure,
					io_contador
					);

				END IF;
			END IF;
		END FOR;

		CLOSE mvfid_hist;

		UPDATE EXT.EXT_MOVIMIENTO_FIANZAS_CAUCION_HIST SET ESTADOREG = 'ENVIADO'
		WHERE ESTADOREG = 'PENDIENTE';

    	COMMIT;

    END IF;

	CALL LIB_GLOBAL_CESCE :w_debug (
    i_Tenant,
    vProcedure || '. Proceso Terminado Satisfactoriamente',
    'SP_SET_MOVIMIENTOS_ENVIADO',
    io_contador
);

END