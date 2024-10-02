//currentCase = resp.getCase('Alta Mediadores-GM-8');

// SCRIPT QUE DA DE ALTA EL PARTICIPANT, POSITION y QUOTAS ////////////////////////////////////////
def utilsValidator = resp.importScript('sc_utils_wf_mediadores');
def utilsMediador = resp.importScript('sc_lib_alta_mediador');
def planComisionamiento = resp.importScript('sc_insert_plan_comisionamiento');
def sapfi = resp.importScript('sc_metodos_sapfi');
def orquestador = resp.importScript('sc_llamada_api_orquestador');
def user = currentUser.getSystemId();
def caseName = currentCase.getCaseKey();
def idHost = currentCase.getCustomFieldValue('txt_idhost');
def codMediador = currentCase.getCustomFieldValue('txt_codigo_mediador');
def identFiscal = currentCase.getCustomFieldValue('txt_num_doc_ident_fiscal');
logger.info('currentCase: idHost: ' + idHost)
logger.info('currentCase: codMediador: ' + codMediador)

def db = resp.dbConnect('datasource.CESCEdb');

def participant = resp.importScript('sc_alta_participant'); // Participant devuelve 0 o -1 
def position = resp.importScript('sc_alta_position'); // Position devuelve positionseq o -2



if(!utilsMediador.invoke('validacionCampos', caseName, user)){
  logger.info('Faltan campos obligatorios.')
  form.preventSubmit();
  return;
  
}

// SE COMPRUEBA SI LOS DATOS NECESARIOS PARA CREAR EL COD. MEDIADOR ESTÁN INICIALIZADOS ///////////

def fechaAlta = currentCase.getCustomFieldValue('date_fecha_inicio_contratacion');

if(fechaAlta == null){
  
  form.preventSubmit();
  resp.alert.error(utilsValidator.invoke('alertMsjInfo', 'E05', user));
  return;
  
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// Código Mediador/////////////////////////////////////////////////////////////////////////////////

try{
  
  if(codMediador == null && idHost == null){
    
    codMediador = utilsMediador.invoke('checkCodMediador', caseName);
    currentCase.setCustomField('txt_codigo_mediador', codMediador);
    logger.info('CodMediador generado: ' + codMediador)
    resp.update(currentCase);

  }

}catch(def e){
  resp.alert.error('Cod. Mediador: ' + e?.toString());
  logger.warn('Error al obtener código de mediador: ' + e?.toString());
  form.preventSubmit();
  return;
  
}

///////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////

def campos =  utilsMediador.invoke('rellenarCampos', caseName);

///////////////////////////////////////////////////////////////////////////////////////////////////

// ORQUESTADOR ////////////////////////////////////////////////////////////////////////////////////

def respuestaOrquestador;

try{

  respuestaOrquestador = orquestador.invoke('datosLlamada', caseName);
  idHost = respuestaOrquestador?.getAt(1)?.idHost?:null;
  logger.info('idHost: ' + idHost)
  def codigoError = respuestaOrquestador?.getAt(0);
  logger.info('codigoError orquestador: ' + codigoError)

  if(codigoError == 500 && (!idHost || idHost == '')){
    logger.warn(respuestaOrquestador?.getAt(1)?.toString());
    resp.alert.error(respuestaOrquestador?.getAt(1)?.toString());
    
    resp.update(currentCase);
    form.preventSubmit();
    return;
  }

  if(idHost != '000000000' && idHost != null){
    currentCase.setCustomField('txt_idhost',idHost);
    logger.info('idHost ' + idHost + ' añadido correctamente al caso.')
  }

}catch(def e){

  idHost = respuestaOrquestador?.getAt(1)?.idHost?:null;
  logger.info('idHost orquestador catch: ' + idHost)

  if(idHost && idHost != '' && idHost != '000000000')
    currentCase.setCustomField('txt_idhost',idHost);
  
  resp.update(currentCase);
  resp.alert.error('Error al conseguir IdHost: ' + e?.toString());
  form.preventSubmit();
  return;
  
}

campos['idHost'] = currentCase.getCustomFieldValue('txt_idhost');
logger.info('idHost campos: ' + campos['idHost']);

///////////////////////////////////////////////////////////////////////////////////////////////////

// Si no se ha creado bien el codigo mediador o el idHost, se detiene la ejecución.
if(codMediador == null || idHost == null){
  resp.alert.error('Error: IdHost ' + idHost + ' - codMediador ' + codMediador);
  logger.error('Error: IdHost ' + idHost + ' - codMediador ' + codMediador)
  form.preventSubmit();
  return;
}

// PARTICIPANT /////////////////////////////////////////////////////////////////////////////////////

try{
  
  def participantId = campos['identFiscal'] + '-' + campos['subclave'];
  
  // Query para averiguar si ya se ha creado el participant
  def queryParticipant = """SELECT PAYEESEQ 
                            FROM TCMP.CS_PARTICIPANT 
                            WHERE GENERICATTRIBUTE2 = '"""+ campos['identFiscal'] +"""'
                            AND ISLAST = 1
                            AND REMOVEDATE = TO_DATE('22000101','yyyymmdd')""";
  
  def resultParticipant = db.queryForList(queryParticipant);

  def llamadaParticipant;

  // Si la query no ha encontrado nada, quiere decir que no hay participant hecho
  // Por tanto, se ejecuta la llamada a la API
  if(resultParticipant == null || resultParticipant == [])
    llamadaParticipant = participant.invoke('altaParticipant', campos, false, caseName);
  	logger.info('llamadaParticipant: ' + llamadaParticipant)
  
  if(llamadaParticipant == 'error'){
    resp.alert.error('Error al crear el participant. Por favor, póngase en contacto con su administrador.')
    resp.update(currentCase);
    form.preventSubmit();
  	return;
  }
 
}catch(def e){
   
  resp.update(currentCase);
  resp.alert.error('Error al crear el Participant: ' + e?.toString());
  logger.warn('Error al crear el participant: ' + e?.toString());
  form.preventSubmit();
  return;
  
}

////////////////////////////////////////////////////////////////////////////////////////////////////



// POSITION ////////////////////////////////////////////////////////////////////////////////////////

try{
  
  // Query para comprobar si ya existe una position para el mediador. 
  def positionId =  campos['codigoMediador'] + '-' + campos['subclave'];
   
  def queryPosition = """SELECT ruleelementownerseq as seq 
                         FROM TCMP.CS_POSITION 
                         WHERE NAME = '""" + positionId + """'
                       	 AND ISLAST = 1
                       	 AND REMOVEDATE = TO_DATE('22000101','yyyymmdd')""";
  
  def resultPosition = db.queryForList(queryPosition);
  
  def llamadaPosition;
  
  // Si la query devuelve null, quiere decir que el mediador no tiene position.
  // En caso contrario, sí que la tiene. Por tanto, guardo su valor por si fuera necesario
  // su uso para crear cuotas
  if(resultPosition == null || resultPosition == '' || resultPosition == []){
    llamadaPosition = position.invoke('altaPosition', campos, false, caseName);
    logger.info('llamadaPosition: ' + llamadaPosition)
    
    if(llamadaPosition == 'error'){
      resp.alert.error('Error al crear la position. Por favor, póngase en contacto con su administrador.')
      resp.update(currentCase);
      form.preventSubmit();
      return;
    }

  }else{
    llamadaPosition = resultPosition.('SEQ');
  }

}catch(def e){
  logger.warn('Error al crear la posición: ' + e?.toString());
  resp.update(currentCase);
  resp.alert.error('Error al crear la Position: ' + e?.toString());
  form.preventSubmit();
  return;
  
}

////////////////////////////////////////////////////////////////////////////////////////////////////



// PLAN DE COMISIONAMIENTO /////////////////////////////////////////////////////////////////////////
try{
  planComisionamiento.invoke('volcarDatosEnContenedor', caseName);
  planComisionamiento.invoke('mapaComisionamiento', caseName);
  planComisionamiento.invoke('insertarProductos', user, caseName);
  
  currentCase.setCustomField('txt_error_insert_plan_comisionamiento', 'Datos guardados correctamente');
  currentCase.setCustomField('txt_fecha_insert_plan_comisionamiento',new Date().format('yyyy-MM-dd'));
  logger.info('plan comisionamiento')
}catch(def e){

  currentCase.setCustomField('txt_error_insert_plan_comisionamiento', e?.toString());
  currentCase.setCustomField('txt_fecha_insert_plan_comisionamiento',new Date().format('yyyy-MM-dd'));
  resp.update(currentCase);
  resp.alert.error('Comisionamiento' + e?.toString());
  logger.error('error comisionamiento: ' + e?.toString());
  form.preventSubmit();
  return;
  
}
////////////////////////////////////////////////////////////////////////////////////////////////////

// SAPFI ///////////////////////////////////////////////////////////////////////////////////////////

try{
  
  def bu = currentCase.getCustomFieldValue('pl_business_unit')?.toUpperCase();
  
  def queryDatosSAPFI = """SELECT * FROM SAP_FINANCIERO 
						   WHERE UPPER(BUSINESSUNIT) = UPPER('$bu') 
						   AND UPPER(CANAL) = UPPER('""" + currentCase.getCustomFieldValue('txt_canal') + """')""";
  logger.info('queryDatosSAPFI: ' + queryDatosSAPFI)
  def datosSAPFI = resp.getCustomTable().findAll(queryDatosSAPFI);

  def modificacion = 'Alta';
  def caseId = currentCase.getId()
  def userName = currentUser.getFirstName()
 
  for(def i = 0; i < datosSAPFI.size(); i++){
    sapfi.invoke('proveedorSapfi', campos, datosSAPFI[i], modificacion, caseId, user) + '\n\n'
  }
  
  // Se comprueba que se ha insertado el registro:
  def identFiscal = campos['identFiscal'];
  if (bu == 'CHILE'){
    if (identFiscal && identFiscal[-2] != '-'){
       identFiscal = identFiscal.substring(0, identFiscal.length() - 1) + '-' + identFiscal[-1];
    } 
  }
  
  def registroSAPFI = "SELECT * FROM EXT.PROVEEDORES_SAPFI WHERE UPPER(CODIGO_FISCAL) = UPPER('$identFiscal') AND ESTADO = 'PTE_ENVIO' AND CASEID = $caseId"
  
  def resultSAPFI = db.queryForList(registroSAPFI);
  if(resultSAPFI){
    logger.info('Registro insertado correctamente en SAPFI: ' + resultSAPFI);
    currentCase.setCustomField('txt_error_insert_sapfi', 'Datos guardados correctamente, pendientes de envío');
  	currentCase.setCustomField('txt_fecha_insert_sapfi',new Date().format('yyyy-MM-dd'));
  }else{
    currentCase.setCustomField('txt_error_insert_sapfi', 'Error al insertar en SAPFI');
  	currentCase.setCustomField('txt_fecha_insert_sapfi',new Date().format('yyyy-MM-dd'));
    resp.alert.error('Error al insertar en SAPFI. Por favor, póngase en contacto con su administrador.');
  	form.preventSubmit();
  }
  
  
}catch(def e){
  
  currentCase.setCustomField('txt_error_insert_sapfi', e?.toString().take(256));
  currentCase.setCustomField('txt_fecha_insert_sapfi',new Date().format('yyyy-MM-dd'));
  resp.update(currentCase);
  resp.alert.error('SAPFI: ' + e?.toString());
  form.preventSubmit();
  return;
  
}

////////////////////////////////////////////////////////////////////////////////////////////////////

resp.update(currentCase);

////////////////////////////////////////////////////////////////////////////////////////////////////