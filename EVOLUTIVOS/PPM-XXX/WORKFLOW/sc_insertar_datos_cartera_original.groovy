// Script test data: 

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

logger.info('sc_insertar_datos_cartera: Comienza script');

def db = resp.dbConnect('datasource.CESCEdb');
def tipoMovimiento = currentCase.getCustomFieldValue('pl_tipo_movimiento');
def tipoTraspaso = currentCase.getCustomFieldValue('pl_tipo_traspaso');

def mapaFormulario = currentCase.getCustomFieldValue('txt_mapa_receptores');
def mapa = evaluate(mapaFormulario);

def polizas = mapa['polizas'];
logger.info('polizas: ' + polizas)
def receptores = mapa['receptores'];
logger.info('receptores: ' + receptores)

def autoCedente;
if(tipoMovimiento == 'sin_mediador_mediador'){
    autoCedente = null;
}else{
    autoCedente = currentCase.getCustomFieldValue('auto_mediador');
}


def codigoCedenteCompleto = autoCedente ? autoCedente.split(' ')?.getAt(0) : '0000-0000';
def codigoMediadorCedente = codigoCedenteCompleto?.split('-')?.getAt(0);
def subclaveMediadorCedente = codigoCedenteCompleto?.split('-')?.getAt(1);

logger.info('Código Mediador Cedente: ' + codigoCedenteCompleto);

if(tipoMovimiento == 'modificar_relacin_intermediacin'){
  
  receptores.each{
    for(def poliza:it.'polizas'){

		def datosPoliza = datosPolizas(poliza.'num_poliza', poliza.'codigo_aval', codigoMediadorCedente, subclaveMediadorCedente);

      def updateQuery = 'UPDATE EXT.CARTERA SET';
      updateQuery += ' ACTIVO = 0';
      updateQuery += ' , MODIF_DATE = CURRENT_TIMESTAMP'
      updateQuery += " , MODIF_SOURCE = '" + currentCase.getCaseKey()?.toString() + "'";
      updateQuery += ' WHERE NUM_POLIZA = ' + datosPoliza?.'NUM_POLIZA';
      updateQuery += ' AND NUM_FIANZA = ' + datosPoliza?.'NUM_FIANZA';
      updateQuery += " AND IDPRODUCT = '" + datosPoliza?.'IDPRODUCT' + "'";
      updateQuery += " AND COD_MEDIADOR = '" + datosPoliza?.'COD_MEDIADOR' + "'";
      updateQuery += " AND COD_SUBCLAVE = '" + datosPoliza?.'COD_SUBCLAVE' + "'";
      updateQuery += " AND FECHA_INICIO = '" + datosPoliza?.'FECHA_INICIO' + "'";
	  
      logger.info('sc_insertar_datos_cartera: ' + updateQuery);
      db.execute(updateQuery);
    }
  }
  
  return;
}


// Se ejecuta el procedimiento almacenado que genera la peticion de cambio de cartera
// Los datos que genera se utilizan en la tarea planificada que hace la llamada al web service de traspasos
def caseId = currentCase.getId()?.toString();

queryGenpetTraspaso = "CALL EXT.GENPET_TRASPASO ('" + caseId + "')";
db.execute(queryGenpetTraspaso);
logger.info('Ejecutado el procedimiento EXT.GENPET_TRASPASO.');
////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Si es operaciones especiales, sólo se tienen que actualizar los porcentajes y las fechas
if(tipoMovimiento == 'operaciones_especiales'){
  logger.info('Operaciones Especiales.');

  receptores.each{
    for(def poliza:it.'polizas'){

      def datosPoliza = datosPolizas(poliza.'num_poliza', poliza.'codigo_aval', codigoMediadorCedente, subclaveMediadorCedente);

      def updateQuery = 'UPDATE EXT.CARTERA SET';
      updateQuery += ' P_ESPECIAL_EMISION = ' + poliza?.'especial_emision'.replace(',','.');
      updateQuery += ', P_ESPECIAL_RENOVACION = ' + poliza?.'especial_renovacion'.replace(',','.');
      updateQuery += ' , MODIF_DATE = CURRENT_TIMESTAMP';
      updateQuery += " , MODIF_SOURCE = '" + currentCase.getCaseKey()?.toString() + "'";    
      updateQuery += ' WHERE NUM_POLIZA = ' + datosPoliza?.'NUM_POLIZA';
      updateQuery += ' AND NUM_FIANZA = ' + datosPoliza?.'NUM_FIANZA';
      updateQuery += " AND IDPRODUCT = '" + datosPoliza?.'IDPRODUCT' + "'";
      updateQuery += " AND COD_MEDIADOR = '" + datosPoliza?.'COD_MEDIADOR' + "'";
      updateQuery += " AND COD_SUBCLAVE = '" + datosPoliza?.'COD_SUBCLAVE' + "'";
      updateQuery += " AND FECHA_INICIO = '" + datosPoliza?.'FECHA_INICIO' + "'";
	  
      logger.info('sc_insertar_datos_cartera: ' + updateQuery);
      db.execute(updateQuery);
    }
  }
  
  logger.info('sc_insertar_datos_cartera: Termina el script.');
  return;
}

logger.info('Se actualizan las pólizas del cedente.');

// Se actualizan los datos del cedente en la tabla cartera
polizas.each{

    def dateEfecto = currentCase.getCustomFieldValue('date_efecto').toString();

    def datosPoliza = datosPolizas(it.'num_poliza', it.'codigo_aval', codigoMediadorCedente, subclaveMediadorCedente);
    logger.info('datosPoliza: ' + datosPoliza)

    def fechaIntermediacion = currentCase.getCustomFieldValue('pl_date_traspaso');
    def fechaFin;
    def fechaEfecto;
	def activo;
  
    if(fechaIntermediacion == 'traspaso_con_derechos_y_obligaciones'){
      fechaFin = dateEfecto;
      activo = '0';
    }else if(fechaIntermediacion == 'fecha_de_traspaso'){ // Traspaso
      fechaFin = currentCase.getCustomFieldValue('date_efecto');
    }else{
      fechaFin = datosPoliza?.FECHA_FIN;
    }
  
    logger.info('fechaFin: ' + fechaFin)
	
    def updateQuery = "UPDATE EXT.CARTERA";
    updateQuery += " SET FECHA_FIN = TO_DATE('" + fechaFin + "','yyyy-MM-dd')";
  	updateQuery += (activo ? " , ACTIVO = $activo " : '');
    updateQuery += ' , MODIF_DATE = CURRENT_TIMESTAMP';
  //updateQuery += " , MODIF_SOURCE = '" + currentCase.getCaseKey()?.toString() + "'";
  
    if(tipoMovimiento == 'sin_mediador_mediador'){
      updateQuery += " WHERE COD_MEDIADOR = '0000' AND COD_SUBCLAVE = '0000'";
    }else{
      updateQuery += " WHERE COD_MEDIADOR = '" + codigoMediadorCedente + "'";
      updateQuery += " AND COD_SUBCLAVE = '" + subclaveMediadorCedente + "'"
    }
    updateQuery += " AND NUM_POLIZA = " + it?.num_poliza;
    updateQuery += (it?.codigo_aval || it?.codigo_aval != '') ? " AND NUM_FIANZA = " + it?.codigo_aval : '';
    updateQuery += " AND FECHA_INICIO = TO_DATE('" + datosPoliza?.FECHA_INICIO + "', 'yyyy-MM-dd')"
    updateQuery += " AND IDPRODUCT = '" + datosPoliza?.IDPRODUCT + "'";
	
    logger.info('CEDENTE. ACTUALIZAR REGISTRO POLIZA ACTUAL: ' + updateQuery);

    db.execute(updateQuery);       
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Dependiendo del tipo de traspaso se recorren los datos de una forma u otra
if(tipoTraspaso == 'parcial'){
    receptores.each{
        for(def poliza:it['polizas']){
            insertarDatosCartera(it, poliza);
        }
    }
}else{
    polizas.each{
        for(def receptor:receptores){
            insertarDatosCartera(receptor, it);
        }
    }
}
/*
// Se ejecuta el procedimiento almacenado que determina el mediador CIC
def queryCic = "CALL EXT.SP_DETERMINAR_CIC()";
db.execute(queryCic);
logger.info('sc_insertar_datos_cartera: Ejecutado el procedimiento EXT.SP_DETERMINAR_CIC.');
*/

if(tipoTraspaso == 'parcial'){
  logger.info('sc_insertar_datos_cartera: Se ejecuta el script asíncrono para llamar al WS.');
  
  def username = currentUser?.getLoginName();
  
  def asyncParams = [
    'caseId':caseId,
    'username':username,
    'caseName': currentCase.getCaseKey()?.toString()
  ];

  try {
    resp.asyncTask.execute ('sc_envio_ws_traspaso_cartera_async', asyncParams);
    
  }catch(Exception e) {
      logger.warn('Error script asíncrono: ' + e?.toString());
  }
  
}

logger.info('sc_insertar_datos_cartera: Termina el script.');

////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Funcion que obtiene todos los datos de una póliza
def datosPolizas(def num_poliza, def aval, def codMediador, def subclaveMediador){

    def db = resp.dbConnect('datasource.CESCEdb');

    def dateEfecto = currentCase.getCustomFieldValue('date_efecto').toString();

    def queryPoliza = "SELECT * FROM EXT.CARTERA";
    queryPoliza += " WHERE NUM_POLIZA = '" + num_poliza + "' ";
    queryPoliza +=  (aval && aval != '') ? " AND NUM_AVAL_HOST = '" + aval + "' " : '';
  	queryPoliza += " AND FECHA_EFECTO <= '" + dateEfecto + "'"; // 20240603 DASP se añade filtro
    queryPoliza += " AND (FECHA_FIN >= '" + dateEfecto + "'";
    queryPoliza += " OR FECHA_FIN IS NULL)";
    queryPoliza += " AND (FECHA_INICIO <= '" + dateEfecto +"'";
    queryPoliza += " OR FECHA_INICIO IS NULL)";
    queryPoliza += " AND FECHA_VENCIMIENTO > '" + dateEfecto +"'";
    queryPoliza += " AND COD_MEDIADOR = '$codMediador'";
  	queryPoliza += " AND COD_SUBCLAVE = '$subclaveMediador'";
// NO Marcos    queryPoliza += " AND ACTIVO <> 0"; // 20240603 DASP se añade filtro activo distinto de 0
  
    logger.info('queryPoliza: ' + queryPoliza)
    return db.queryForList(queryPoliza)?.getAt(0);

}
////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Función para insertar los datos del traspaso en la tabla de cartera
def insertarDatosCartera(def receptor, def poliza){
  
  def db = resp.dbConnect('datasource.CESCEdb');
  
  logger.info('Inserción póliza: ' + poliza?.num_poliza + '/' + poliza?.codigo_aval + ' para receptor: ' + receptor?.codigo_mediador + '-' + receptor?.subclave_mediador);

  def tipoMovimiento = currentCase.getCustomFieldValue('pl_tipo_movimiento');
  def tipoTraspaso = currentCase.getCustomFieldValue('pl_tipo_traspaso');
  def ramo = currentCase.getCustomFieldValue('pl_ramo');

  def autoCedente = currentCase.getCustomFieldValue('auto_mediador');
  def codigoMediadorCedente;
  def subclaveMediadorCedente;

  if(tipoMovimiento == 'sin_mediador_mediador'){
    codigoMediadorCedente = '0000';
    subclaveMediadorCedente = '0000';
  }else{
    codigoMediadorCedente = autoCedente?.split(' ')?.getAt(0)?.split('-')?.getAt(0);
    subclaveMediadorCedente = autoCedente?.split(' ')?.getAt(0)?.split('-')?.getAt(1);
  }

  
  def datosPoliza = datosPolizas(poliza.'num_poliza', poliza.'codigo_aval', codigoMediadorCedente, subclaveMediadorCedente);
	
  logger.info('Datos Póliza: ' + datosPoliza);
  

  def fechaIntermediacion = currentCase.getCustomFieldValue('pl_date_traspaso');
  def fechaEfecto = currentCase.getCustomFieldValue('date_efecto');

  def tipoFechaTraspaso = currentCase.getCustomFieldValue('pl_date_traspaso');
  def fechaInicio;
  def fechaFin;
  def activo;
  
  //TRASPASO CON DERECHOS Y OBLIGACIONES
  if(tipoFechaTraspaso == 'traspaso_con_derechos_y_obligaciones'){
    if(ramo == 'credito'){
      fechaInicio = datosPoliza?.FECHA_EFECTO;//fecha efecto de la cartera
      fechaFin = datosPoliza?.FECHA_VENCIMIENTO; // fecha vencimiento de la cartera
    }
    if(ramo == 'caucion'){
      fechaInicio = fechaEfecto;// fecha traspaso definida en el formulario
      fechaFin = '2200-01-01'; // fecha fin de los tiempos
    }
    
    activo = 1;
    
  }else if(tipoFechaTraspaso == 'sin_derechos_y_obligaciones_a_la_renovacin'){ // Renovación
    
    def formato = DateTimeFormatter.ofPattern('yyyy-MM-dd');
   
    
    if(datosPoliza.RAMO?.toUpperCase() == 'CAUCION'){
    	fechaFin = datosPoliza?.FECHA_VENCIMIENTO?.toString();
      	fechaInicio = datosPoliza?.FECHA_EFECTO?.toString();
    }else{
      	fechaFin = LocalDate.parse(datosPoliza?.FECHA_VENCIMIENTO?.toString(), formato)?.plusYears(1);
    	fechaInicio = LocalDate.parse(datosPoliza?.FECHA_VENCIMIENTO?.toString(), formato)?.plusDays(1);
    }

    activo = 2;
    
  }else{
    fechaInicio = datosPoliza?.FECHA_INICIO;
    fechaFin = datosPoliza?.FECHA_FIN;
  }
  
  logger.info('fechaInicio: ' + fechaInicio);
  logger.info('fechaFin: ' + fechaFin)
  logger.info('tipoFechaTraspaso: ' + tipoFechaTraspaso)
  logger.info('tipoMovimiento: ' + tipoMovimiento)

   
  def porcentaje = (tipoTraspaso == 'parcial') ? poliza?.porcentaje_intermediacion : receptor?.porcentaje_traspaso_total;

  def usuario = currentUser.getFirstName() + ' ' + currentUser.getLastName();
  def casename = currentCase.getCaseKey()?.toString();

  /*                  *
  *   QUERY INSERT    *
  *                   */

  def queryInsert = 'INSERT INTO EXT.CARTERA '
  def columnas = '';
  def valores = '';

  columnas += 'RAMO';
  valores += "'" + datosPoliza?.RAMO + "'";
  
  columnas += ', IDPRODUCT';
  valores += ", '" + datosPoliza.IDPRODUCT + "'";

  columnas += ', NUM_POLIZA';
  valores += ', ' + datosPoliza.NUM_POLIZA;

  columnas += ', IDMODALIDAD';
  valores += ', ' + datosPoliza.IDMODALIDAD;

  columnas += datosPoliza?.IDSUBMODALIDAD ? ', IDSUBMODALIDAD': '';
  valores += datosPoliza?.IDSUBMODALIDAD ? ", '" + datosPoliza.IDSUBMODALIDAD + "'" : '';

  columnas += ', NUM_FIANZA';
  valores += datosPoliza?.NUM_FIANZA ? ', ' + datosPoliza.NUM_FIANZA : ', 0';

  columnas += datosPoliza?.NUM_EXPEDIENTE ? ', NUM_EXPEDIENTE' : '';
  valores += datosPoliza?.NUM_EXPEDIENTE ? ', ' + datosPoliza?.NUM_EXPEDIENTE : '';

  columnas += datosPoliza?.NUM_AVAL_HOST ? ', NUM_AVAL_HOST' : '';
  valores += datosPoliza?.NUM_AVAL_HOST ? ', ' + datosPoliza?.NUM_AVAL_HOST : '';

  columnas += ', NUM_ANUALIDAD';
  valores += datosPoliza?.NUM_ANUALIDAD ? ', ' + datosPoliza.NUM_ANUALIDAD : ', 0';

  columnas += datosPoliza?.FECHA_EMISION ? ', FECHA_EMISION' : '';
  valores += datosPoliza?.FECHA_EMISION ? ", '" + datosPoliza.FECHA_EMISION + "'" : '';

  columnas += datosPoliza?.FECHA_EFECTO ? ', FECHA_EFECTO' : '';
  valores += datosPoliza?.FECHA_EFECTO ? ", '" + datosPoliza.FECHA_EFECTO + "'" : '';

  columnas += datosPoliza?.FECHA_VENCIMIENTO ? ', FECHA_VENCIMIENTO' : '';
  valores += datosPoliza?.FECHA_VENCIMIENTO ? ", '" + datosPoliza.FECHA_VENCIMIENTO + "'" : '';

  columnas += datosPoliza?.IDPAIS ? ', IDPAIS' : '';
  valores += datosPoliza?.IDPAIS ? ', ' + datosPoliza.IDPAIS : '';

  columnas += ', PRIMA_PROVISIONAL_INT';
  valores += datosPoliza?.PRIMA_PROVISIONAL_INT ? ', ' + datosPoliza?.PRIMA_PROVISIONAL_INT : ', 0';

  columnas += ', PRIMA_PROVISIONAL_EXT';
  valores += datosPoliza?.PRIMA_PROVISIONAL_EXT ? ', ' + datosPoliza?.PRIMA_PROVISIONAL_EXT : ', 0';

  columnas += ', IDDIVISA_INT';
  valores += datosPoliza?.IDDIVISA_INT ? ', ' + datosPoliza?.IDDIVISA_INT : ', 0';

  columnas += ', IDDIVISA_EXT';
  valores += datosPoliza?.IDDIVISA_EXT ? ', ' + datosPoliza?.IDDIVISA_EXT : ', 0';

  columnas += datosPoliza?.IDDIVISA_COBERTURA ? ', IDDIVISA_COBERTURA' : '';
  valores += datosPoliza?.IDDIVISA_COBERTURA ? ', ' + datosPoliza?.IDDIVISA_COBERTURA : '';

  columnas += ', PRIMA_MIN_INT';
  valores += datosPoliza?.PRIMA_MIN_INT ? ', ' + datosPoliza?.PRIMA_MIN_INT : ', 0';

  columnas += ', PRIMA_MIN_EXT';
  valores += datosPoliza?.PRIMA_MIN_EXT ? ', ' + datosPoliza?.PRIMA_MIN_EXT : ', 0';

  columnas += receptor?.codigo_mediador ? ', COD_MEDIADOR': '';
  valores += receptor?.codigo_mediador ? ", '" + receptor.codigo_mediador + "'": '';

  columnas += receptor?.subclave_mediador ? ', COD_SUBCLAVE': '';
  valores += receptor?.subclave_mediador ? ", '" + receptor.subclave_mediador + "'": '';

  columnas += porcentaje ? ', P_INTERMEDIACION': '';
  valores += porcentaje ? ', ' + porcentaje : '';

  columnas += fechaInicio ? ', FECHA_INICIO': '';
  valores += fechaInicio ? ", '" + fechaInicio + "'" : '';

  columnas += fechaFin ? ', FECHA_FIN': '';
  valores += fechaFin ? ", '" + fechaFin + "'" : '';

  
  def especial_emision = (tipoMovimiento == 'operaciones_especiales') ? poliza?.especial_emision : datosPoliza?.P_ESPECIAL_EMISION;
  especial_emision = especial_emision?.toString()?.replace(',','.');
  
  columnas += especial_emision ? ', P_ESPECIAL_EMISION': '';
  valores += especial_emision ? ", " + especial_emision : '';
	
  //Porcentaje operaciones epeciales renovación
  def especial_renovacion = (tipoMovimiento == 'operaciones_especiales') ? poliza?.especial_renovacion : datosPoliza?.P_ESPECIAL_RENOVACION;
  especial_renovacion = especial_renovacion?.toString()?.replace(',','.');
  columnas += especial_renovacion ? ', P_ESPECIAL_RENOVACION': '';
  valores += especial_renovacion ? ", " + especial_renovacion : '';

  columnas += datosPoliza?.NIF_TOMADOR ? ', NIF_TOMADOR': '';
  valores += datosPoliza?.NIF_TOMADOR ? ", '" + datosPoliza?.NIF_TOMADOR + "'" : '';

  columnas += datosPoliza?.NOMBRE_TOMADOR ? ', NOMBRE_TOMADOR': '';
  valores += datosPoliza?.NOMBRE_TOMADOR ? ", '" + datosPoliza?.NOMBRE_TOMADOR + "'" : '';

  columnas += fechaEfecto ? ', FECHA_EFECTO_TRASPASO': '';
  valores += fechaEfecto ? ", '" + fechaEfecto + "'": '';

  columnas += activo ? ', ACTIVO': '';
  valores += activo ? ", " + activo : '';

  columnas += ', CREATEDATE';
  valores += ', CURRENT_TIMESTAMP';

  columnas += ', MODIF_DATE';
  valores += ', CURRENT_TIMESTAMP';

  columnas += usuario ? ', MODIF_USER': '';
  valores += usuario ? ", '" + usuario + "'": '';

  columnas += casename ? ', MODIF_SOURCE': '';
  valores += casename ? ", '" + casename + "'": '';

  logger.info('sc_insertar_datos_cartera: ' + queryInsert + '(' + columnas + ') VALUES (' + valores + ')')
  db.execute(queryInsert + '(' + columnas + ') VALUES (' + valores + ')');

}

///////////////////////////////////////////////////////////////////////////////////////////////////////

def crearReprocesos(def db){

    //CREAR CASO WF REPROCESOS ASOCIADO

    def mediador = currentCase.getCustomFieldValue('auto_mediador').substring(0,9);
    def existeCasoRel = 0;
    def idCasoRel; 
    def idCasoOrigen = currentCase.getId();

    logger.info( " Se han obtenido " + numRegs + " registros. Se relaciona el caso o se mira si ya existe y está en estado NUEVO" );  
    listaCasosRelacionados = currentCase.getRelatedToCases();
    logger.info( 'listaCasosRelacionados '  + listaCasosRelacionados )
    for (def casoRel : listaCasosRelacionados) {
      logger.info( casoRel.getCaseKey() + ' ' + casoRel.getStatus().getName() ) 
      if ( casoRel.getStatus().getName() == 'NUEVO' ) {
        logger.info( 'Ya existe caso relacionado' + casoRel.getCaseKey() + ' ' + casoRel.getId() ); 
        idCasoRel = casoRel.getId();
        casoRel.setName( 'Reproceso Plan Comisionamiento ' + position );
        resp.update(casoRel);
        existeCasoRel=1;

        break;
      }
    }
    // SI NO EXISTE CASO PREVIO SE CREA UN NUEVO CASO RELACIONADO
    if (existeCasoRel == 0) {

      def caseType = resp.getCaseType('wf_repro');

      def newCase = resp.newCase(caseType);
      newCase.setAssignee(currentUser);  
      newCase.setName( 'Reproceso Traspasos ' + mediador)

      if (currentCase.getCustomFieldValue('pl_tipo_movimiento') == 'operaciones_especiales') {
        newCase.setCustomField('pl_tipo_reproceso', 'modificacin_porc_especial');
      }
      else if (currentCase.getCustomFieldValue('pl_tipo_movimiento') == 'error_captura') {
        newCase.setCustomField('pl_tipo_reproceso', 'traspaso');
      }
      // se asigna a proyecto GM
      def p = resp.getProject('GM');
      newCase.setProject(p);

      resp.save(newCase);
      // Crear la realción con el nuevo caso
      resp.cases.createCaseRelation(currentCase, newCase, 'related_case');

      logger.info( 'Nuevo caso relacionado' + newCase.getCaseKey() + ' ' + newCase.getId() );
      idCasoRel = newCase.getId();
    }
    // Se actulizan los registros creados con el id del caso relacionado de reproceso
    // UPDATE  "EXT"."TXN_REPROCESOS" set CASEID_REPROCESO = 1000 where CASEID_ORIGEN = 1240 and ESTADOREG = 'NUEVO'
    def queryUpdate = "UPDATE EXT.TXN_REPROCESOS SET CASEID_REPROCESO = $idCasoRel WHERE CASEID_ORIGEN =$idCasoOrigen and ESTADOREG = 'NUEVO'";
    logger.info('queryUpdate ' + queryUpdate );

    try{
      db.execute(queryUpdate);
    }catch(def e){
      resp.alert.error(e);
      return;
    }

  }

