//TAREA PLANIFICADA COMPROBACION Y NOTIFICACION POR CORREO DE LA CARGA DE FICHEROS DIARIA

def db = resp.dbConnect('datasource.CESCEdb');

def enviarCorreo(def resultadoConsulta, def titulo) {
    def mensaje = """
        <!DOCTYPE html 
        PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

        <html xmlns="http://www.w3.org/1999/xhtml">
            <head>
            <title></title>
            <style>
                #polizas {
                font-family: Arial, Helvetica, sans-serif;
                border-collapse: collapse;
                width: 100%;
                font-size: 12px;
                }

                #polizas td, #polizas th {
                border: 1px solid #ddd;
                padding: 8px;
                }

                #polizas th {
                padding-top: 12px;
                padding-bottom: 12px;
                text-align: left;
                background-color: #1c4cde;
                color: white;
                }

                a:hover {
                    color: #cecece !important;
                }

                a img {
                    border: none;
                    outline: none;
                }

                a:hover {
                    color: #cecece;
                }
            </style>
            </head>

            <body style="color: #000000; font-size: 12px; line-height: 14px; font-family: Arial,Verdana,Helvetica,sans-serif;">
            <p>Es necesario abrir una incidencia al grupo de soporte de SAP Commissions por errores en la carga ETL:</p>
        <div>
            <table>
                <tr>
                <td><img src="https://eusb.webcomserver.com/wpm/custom_files/mt_1689/logo/AF_CESCE_RGB_POSITIVO.png" alt="Company Logo"></td>
                <td><h3>$titulo</h3></td>
                </tr>
            </table>
        </div>
        <div>
            
            </br>
            <table id="polizas" style="white-space: nowrap;">
                <tr>
					""";
                    resultadoConsulta[0].keySet().each { campo ->
                        mensaje += """
                          <th>${campo}</th>
                        """
                    }
  					mensaje += """
                </tr>
           		""";
    def index = 0;

    resultadoConsulta.each { r ->
    	mensaje += """
                    <tr """ + ((index % 2 == 0) ? ('style="background-color: #f2f2f2"') : '') + """>
		""";
    	r.each { campo, valor ->
        	mensaje += """
                        <td>${valor}</td>					
        	""";
      	}
      mensaje += """
      </tr>
	  """;
      index += 1;
    }
    
    mensaje += """
                </table>
            </div>
        </body>
    </html>""";

    mensaje = mensaje.replace("null", "");

    logger.info(mensaje);
    
    def msg = resp.newMessage();
    msg.addRecipient('alvaro.lailla@inycom.es') 
    //msg.addRecipient(resp.getAppParam("helpdeskCESCE"))
    msg.addCC(resp.getAppParam("soporteCESCE")) 
    msg.addCC("samuel.miralles@inycom.es") 
    msg.setSubject("SAP Commissions – Error ETL - $titulo")
    def bodyMail = ""; 
    bodyMail = mensaje
    msg.setBody(bodyMail);
    resp.send(msg);
}

def consulta1 = """
SELECT
CASE WHEN SUBSTRING(COD_OPERACION, 1, 3) <> '091' and SUBSTRING(COD_OPERACION, 1, 3) <> '098'
THEN CASE WHEN (select count(*) from EXT.CARTERA where NUM_POLIZA = SUBSTRING(COD_OPERACION, 4, 8) and NUM_ANUALIDAD = NUM_PERIODO ) > 0 
     Then CASE WHEN (select to_varchar(max(FECHA_EFECTO)) from EXT.CARTERA 
                  where NUM_POLIZA = SUBSTRING(COD_OPERACION, 4, 8) and NUM_ANUALIDAD = NUM_PERIODO ) <> FECHA_EFECTO_ANUALIDAD
          Then  'Fecha Efecto no coincide: ' || (select to_varchar(max(FECHA_EFECTO)) from EXT.CARTERA 
                  where NUM_POLIZA = SUBSTRING(COD_OPERACION, 4, 8) and NUM_ANUALIDAD = NUM_PERIODO )
          Else CASE WHEN  (select min(ACTIVO) from EXT.CARTERA where NUM_POLIZA = SUBSTRING(COD_OPERACION, 4, 8) and NUM_ANUALIDAD = NUM_PERIODO ) = 0
              Then 'Poliza anulada para anualidad ' || to_varchar(NUM_PERIODO )
              else CASE WHEN (select max(IDPAIS) from EXT.CARTERA where NUM_POLIZA = SUBSTRING(COD_OPERACION, 4, 8) and NUM_ANUALIDAD = NUM_PERIODO ) <> IDPAIS
                    Then 'Pais Poliza no coincide con pais de Recibo ' || to_varchar(IDPAIS )
                    else 'Pte. Revisión manual'
                   end
              end
          end
     Else 'No hay polizas para esa anualidad' 
 END
ELSE  CASE WHEN (select count(*) from EXT.CARTERA where NUM_POLIZA = SUBSTRING(COD_OPERACION, 4, 8) and NUM_FIANZA = COD_AVAL ) > 0 
       Then CASE WHEN (select to_varchar(max(FECHA_EFECTO)) from EXT.CARTERA 
                  where NUM_POLIZA = SUBSTRING(COD_OPERACION, 4, 8) and NUM_FIANZA = COD_AVAL ) <> FECHA_EFECTO_ANUALIDAD
            Then 'Fecha Efecto no coincide: ' || (select to_varchar(max(FECHA_EFECTO)) from EXT.CARTERA 
                  where NUM_POLIZA = SUBSTRING(COD_OPERACION, 4, 8) and NUM_FIANZA = COD_AVAL )
            Else  'Pte. Revisión manual'
             End
        Else 'No hay poliza/aval'
  END
END as MOTIVO,
SUBSTRING(COD_OPERACION, 1, 3) as MODALIDAD,
SUBSTRING(COD_OPERACION, 4, 8) "NUM POLIZA",
NUM_RECIBO "NUM RECIBO",
NUM_PERIODO as ANUALIDAD,
COD_AVAL as FIANZA,
FECHA_EFECTO_ANUALIDAD "FECHA EFECTO",
COD_RIESGO as RIESGO,
IMPORTE_MVTO_RIESGO "IMPORTE TOTAL",
FECHA_DATOS "FECHA DATOS",
BATCHNAME "FICHERO CARGA", 
ID 
FROM EXT."EXT_MOVIMIENTO_RECIBOS_HIST" 
WHERE ESTADOREG = 'NO_CARTERA'  
AND BATCHNAME IN (SELECT BATCHNAME FROM EXT.REGISTRO_INTERFACES WHERE NOTIFICATION = 0 AND BATCHNAME LIKE '%MVREC%')
""";

def consulta2 = """
SELECT
CASE 
    WHEN TA.GENERICATTRIBUTE2 IS NULL THEN 'No existe el mediador'
    WHEN CR.GENERICNUMBER1 IS NULL THEN 'No tiene plan de comisionamiento'
    ELSE 'Pte. Revisión manual'
END AS MOTIVO,
TA.GENERICATTRIBUTE2 "COD MEDIADOR",
TA.GENERICATTRIBUTE3 AS SUBCLAVE,
IFNULL(MED.NOMBRE || ' ', '') || MED."APELLIDO/RAZON_SOCIAL" "NOMBRE MEDIADOR",
tit.NAME AS CARGO,
ORDERID,
LINENUMBER,
SUBLINENUMBER,
REPLACE(CAST(ROUND(TX.VALUE , 2) AS DECIMAL(10,2)), '.', ',') "IMPORTE TOTAL",
TX.COMPENSATIONDATE "FECHA DATOS"
FROM CS_SALESTRANSACTION TX
LEFT JOIN CS_CREDIT CR ON  TX.SALESTRANSACTIONSEQ = CR.SALESTRANSACTIONSEQ
LEFT JOIN CS_SALESORDER SO ON TX.SALESORDERSEQ = SO.SALESORDERSEQ AND SO.REMOVEDATE = TO_DATE('22000101', 'yyyymmdd')
LEFT JOIN CS_TRANSACTIONASSIGNMENT TA ON TX.SALESTRANSACTIONSEQ = TA.SALESTRANSACTIONSEQ
LEFT JOIN EXT.MODIFICAR_MEDIADOR MED ON  TA.POSITIONNAME = MED.POSITIONNAME
LEFT JOIN CS_POSITION pos on med.POSITIONSEQ = pos.RULEELEMENTOWNERSEQ  and pos.REMOVEDATE = TO_DATE('22000101','yyyymmdd') and pos.islast=1
LEFT JOIN CS_TITLE tit on pos.titleseq=tit.RULEELEMENTOWNERSEQ AND tit.REMOVEDATE = TO_DATE('22000101','yyyymmdd') and tit.islast=1
WHERE TO_NVARCHAR(TX.COMPENSATIONDATE, 'YYYY-MM-DD') 
    IN (SELECT FECHA_DATOS FROM EXT.EXT_MOVIMIENTO_RECIBOS_HIST WHERE BATCHNAME IN (SELECT BATCHNAME FROM EXT.REGISTRO_INTERFACES WHERE NOTIFICATION = 0 AND BATCHNAME LIKE '%MVREC%'))
AND CR.CREDITSEq is null
""";

def consulta3 = """SELECT MOTIVO, IDMODALIDAD AS MODALIDAD, NUM_POLIZA "NUM POLIZA", ANUALIDAD, FECHA_EFECTO "FECHA EFECTO", FECHA_VENCIMIENTO "FECHA VENCIMIENTO", IDMEDIADOR "COD MEDIADOR",
FECHA_INICIO "INICIO", FECHA_FIN "FIN", BATCHNAME "FICHERO CARGA" 
FROM EXT.GET_POLIZAS_CREDITO_ERROR();""";

def consulta4 = """SELECT MOTIVO, IDMODALIDAD AS MODALIDAD, NUM_POLIZA "NUM POLIZA", NUM_FIANZA AS FIANZA, FECHA_EFECTO "FECHA EFECTO", FECHA_VENCIMIENTO "FECHA VENCIMIENTO", IDMEDIADOR "COD MEDIADOR",
FECHA_INICIO "INICIO", FECHA_FIN "FIN", BATCHNAME "FICHERO CARGA" 
FROM EXT.GET_POLIZAS_CAUCION_ERROR();""";

def titulo = [];

def resultadoConsulta1 = db.queryForList(consulta1);
if(resultadoConsulta1) {
    titulo = 'Movimientos de recibo';
    enviarCorreo(resultadoConsulta1, titulo);
}

def resultadoConsulta2 = db.queryForList(consulta2);
if(resultadoConsulta2) {
    titulo = 'Transacciones sin cálculos';
    enviarCorreo(resultadoConsulta2, titulo);
}

def updateMVREC = db.execute("""UPDATE EXT.REGISTRO_INTERFACES SET NOTIFICATION = 1 WHERE NOTIFICATION = 0 AND BATCHNAME LIKE '%MVREC%'""");

def resultadoConsulta3 = db.queryForList(consulta3);
if(resultadoConsulta3) {
    titulo = 'Cartera Credito MVCAR';
    enviarCorreo(resultadoConsulta3, titulo);
}

def updateMVCAR = db.execute("""UPDATE EXT.REGISTRO_INTERFACES SET NOTIFICATION = 1 WHERE NOTIFICATION = 0 AND BATCHNAME LIKE '%MVCAR%'""");

def resultadoConsulta4 = db.queryForList(consulta4);
if(resultadoConsulta4) {
    titulo = 'Cartera Caución MVFID';
    enviarCorreo(resultadoConsulta4, titulo);
}

def updateMVFID = db.execute("""UPDATE EXT.REGISTRO_INTERFACES SET NOTIFICATION = 1 WHERE NOTIFICATION = 0 AND BATCHNAME LIKE '%MVFID%'""");

//update EXT.REGISTRO_INTERFACES set notification = 0 where batchname = '1689_MVREC_PRD_20240706_063852_MovDiarios.txt';
//update EXT.REGISTRO_INTERFACES set notification = 0 where batchname = '1689_MVCAR_PRD_20240912_061502_MovCartera.txt';
//update EXT.REGISTRO_INTERFACES set notification = 0 where batchname = '1689_MVFID_PRD_20240618_1030000_MovFianzas';