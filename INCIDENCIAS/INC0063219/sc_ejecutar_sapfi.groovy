// SAPFI ///////////////////////////////////////////////////////////////////////////////////////////

def sapfi = resp.importScript('sc_metodos_sapfi');
def utilsMediador = resp.importScript('sc_lib_alta_mediador');
def caseName = currentCase.getCaseKey();
def campos =  utilsMediador.invoke('rellenarCampos', caseName);
def user = currentUser.getSystemId();
def db = resp.dbConnect('datasource.CESCEdb');

try{
  
  def bu = currentCase.getCustomFieldValue('pl_business_unit')?.toUpperCase();
  
  def queryDatosSAPFI = """SELECT * FROM SAP_FINANCIERO 
						   WHERE UPPER(BUSINESSUNIT) = '$bu'
						   AND UPPER(CANAL) = UPPER('""" + currentCase.getCustomFieldValue('txt_canal') + """')""";
  logger.info('queryDatosSAPFI: ' + queryDatosSAPFI)
  def datosSAPFI = resp.getCustomTable().findAll(queryDatosSAPFI);

  def modificacion = 'Alta';
  def caseId = currentCase.getId()
  
  logger.info('datosSAPFI: ' + datosSAPFI)
 
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
    logger.error('Error al insertar en SAPFI')
    currentCase.setCustomField('txt_error_insert_sapfi', 'Error al insertar en SAPFI');
  	currentCase.setCustomField('txt_fecha_insert_sapfi',new Date().format('yyyy-MM-dd'));
  }
  
  //currentCase.setCustomField('txt_error_insert_sapfi', 'Datos guardados correctamente, pendientes de envío');
  //currentCase.setCustomField('txt_fecha_insert_sapfi',new Date().format('yyyy-MM-dd'));
  form.getField('txt_error_insert_sapfi').setValue('Datos guardados correctamente, pendientes de envío');
  form.getField('txt_fecha_insert_sapfi').setValue(new Date().format('yyyy-MM-dd'));
  resp.update(currentCase);
  logger.info('sap fi ejecutado correctamente')
  
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