def wf = currentCase.getCaseType();
// OCULTA CIERTOS CAMPOS DEPENDIENDO DE SI ES PER. FISICA O NO ////////////////////////////
def naturaleza = currentCase.getCustomFieldValue('pl_naturaleza');
logger.info('BU: ' + currentCase.getCustomFieldValue('pl_business_unit')?.toLowerCase());

if(naturaleza == 'per. fisica' || naturaleza == 'pf'){
  form.getField('txt_razon_social')?.setHidden(true);
  form.getField('txt_domicilio_social')?.setHidden(true);
  if(wf && wf != 'wf_mod'){
    form.getField('txt_nombre_completo_rep')?.setHidden(true);
    form.getField('txt_ident_fiscal_rep')?.setHidden(true);
    form.getField('pl_calidad_representante')?.setHidden(true);
    form.getField('txt_direccion_rep')?.setHidden(true);
  }
}else{
  form.getField('txt_nombre')?.setHidden(true);
  form.getField('txt_apellidos')?.setHidden(true);
  form.getField('pl_irpf')?.setHidden(true);
  form.getField('txt_direccion')?.setHidden(true);
}

//def pl = form.addField('pl_gerente_canal');
//pl.moveBefore('pl_homologado');

///////////////////////////////////////////////////////////////////////////////////////////



// Ocultar según business unit ////////////////////////////////////////////////////////////

if(currentCase.getCustomFieldValue('pl_business_unit') == 'portugal'){
  def irpf = form.getField('pl_irpf');
  irpf.setRequired(false);
  irpf.setHidden(true);
}

///////////////////////////////////////////////////////////////////////////////////////////

if(currentCase.getStatus().toString() == 'nueva_alta' || currentCase.getStatus().toString() == 'contrato_firmado' || currentCase.getStatus().toString() == 'validado' || currentCase.getStatus().toString() == 'contrato_firmado_pte_sistemas' || currentCase.getStatus().toString() == 'completado' || currentCase.getStatus().toString() == 'pendiente_sistemas' || currentCase.getStatus().toString() == 'nuevo'){
  form.getField('date_fecha_inicio_contratacion').setHidden(false);
  form.getField('date_fecha_inicio_contratacion').setRequired(false);
}else{
  form.getField('date_fecha_inicio_contratacion').setHidden(true);
}
logger.info('CANAL: ' + currentCase.getCustomFieldValue('txt_canal'))
if(currentCase.getCustomFieldValue('txt_canal')?.toLowerCase() != 'corredor' && currentCase.getCustomFieldValue('txt_canal') != '2'){
  form.getField('cb_asociaciones').setHidden(true);
}

// OCULTA EL TAB DE OTROS DATOS SI EL USUARIO NO ES ADMIN
def datosOcultos = form.getTabContainer('tab_datos_editTable');
if(datosOcultos != null && datosOcultos != ''){
  def tabOculta = datosOcultos.getTab('tab_campos_ocultos');
  if(!currentUser.isAdmin()){
    tabOculta.hide();
  }
}



def tabEconomica = form.getTabContainer('tab_datos_editTable');

if(tabEconomica != null && tabEconomica != ''){
  
  def ocultarEconomica = tabEconomica.getTab('tab_datos_economicos');
  def bu = currentCase.getCustomFieldValue('pl_business_unit');
  
  if(bu != 'spain' && bu != 'portugal'){
    ocultarEconomica.hide();
    form.getField('pl_facturacion_propia')?.setRequired(false);
    //form.setValue('pl_facturacion_propia', 'si');
    form.getField('pl_forma_pago')?.setRequired(false);
  }
  
}

if (wf != 'wf_mod'){

  def tipoMediador = currentCase.getCustomFieldValue('pl_tipo_mediador');

  if(!['agente','sociedad agencial'].contains(tipoMediador?.toLowerCase())){
    form.getField('txt_importe_subvencion')?.setHidden(true);
    form.getField('txt_duracion_subvencion')?.setHidden(true);
  }
}


// MUESTRA EMAIL CONSULTOR SI CONSULTOR RIESGOS = SI
def consultor = form.getValue('pl_consultor_riesgos');
if(consultor == 'si'){
  form.getField('txt_email_consultor_riesgos').setHidden(false);
}else{
  form.getField('txt_email_consultor_riesgos').setHidden(true);
}

logger.info('TXT_CANAL - '+currentCase.getCustomFieldValue('txt_canal'))
//DASP 20241119 solicitan que campo dgs se vea siempre
/*
if(currentCase.getCustomFieldValue('txt_canal')?.toLowerCase() != 'corredor' && currentCase.getCustomFieldValue('txt_canal') != '2'){
	logger.info('entra hide')
	form.getField('txt_numero_registro_dgs')?.hide();
  	form.getField('date_fecha_registro_dgs')?.hide();
} else {
  	form.getField('txt_numero_registro_dgs')?.show();
  	form.getField('date_fecha_registro_dgs')?.show();
}
*/

// MUESTRA/OCULTA Y FIJA VALORES PARA IDENTIFICACION FISCAL ANTERIOR

if (wf != 'wf_mod'){

  def numeroIdentificacionAnterior = form.getValue('txt_ident_fiscal_anterior');

  if(numeroIdentificacionAnterior != '' && numeroIdentificacionAnterior != null){

    numeroIdentificacionAnterior = numeroIdentificacionAnterior.toUpperCase();

    def db = resp.dbConnect('datasource.CESCEdb');
    def queryIdentAnterior = """SELECT * FROM TCMP.CS_PARTICIPANT 
                  WHERE GENERICATTRIBUTE2 = '""" + numeroIdentificacionAnterior + """'
                  AND REMOVEDATE = TO_DATE('22000101', 'yyyymmdd')
                  AND ISLAST = 1""";
    def participant = db.queryForList(queryIdentAnterior)[0];

    if (participant){

      def nombreCompletoMedAnterior = participant.FIRSTNAME + ' ' + participant.LASTNAME;

      form.getField('txt_nombre_mediador_anterior').setHidden(false);
      form.getField('txt_precontrato_no_necesario').setHidden(false);
      form.setValue('txt_nombre_mediador_anterior', nombreCompletoMedAnterior);
      form.setValue('txt_precontrato_no_necesario', 'No necesario');
      form.getField('txt_ident_fiscal_anterior').setLabelColor('green');

    }else{
      form.getField('txt_nombre_mediador_anterior').setHidden(true);
      form.getField('txt_precontrato_no_necesario').setHidden(true);
      def numIdentAnterior = form.getField('txt_ident_fiscal_anterior');

      //numIdentAnterior?.setErrorMessage('No coincide con ningún mediador existente');
      resp.alert.error('No coincide con ningún mediador existente');
      numIdentAnterior?.setLabelColor('red');
    }

  }else{
      form.getField('txt_nombre_mediador_anterior').setHidden(true);
      form.getField('txt_precontrato_no_necesario').setHidden(true); 
      //form.getField('txt_ident_fiscal_anterior').clearErrorMessage();
      form.getField('txt_ident_fiscal_anterior').setLabelColor('none');
    }
}


// Para Alta Mediador, PONER EN READONLY EL CAMPO pl_homologado SI BU ES LATAM
// Se asigna valor 'SI' para LATAM, y para España y Portugal 'NO'

def businessUnit = currentCase.getCustomFieldValue('pl_business_unit');

logger.info('wf: ' + wf)

if (wf == 'wf_alta'){
  
  if(businessUnit != 'spain' && businessUnit != 'portugal'){
    form.getField('pl_homologado')?.setValue('SI');
    form.getField('pl_homologado')?.setReadonly(true);
  } else {
    form.getField('pl_homologado')?.setValue('NO');
    form.getField('pl_homologado')?.setReadonly(true);
  }
  
}

def tipoModificacion = currentCase.getCustomFieldValue('pl_tipo_modificacion');

if (wf == 'wf_mod'){
  if (currentUser.isAdmin() == true){
    logger.info('es admin')
    form.getWidget('section_otros_datos')?.show()
  } else {
    logger.info('no es admin')
    form.getWidget('section_otros_datos')?.hide()
  }
  
  if(tipoModificacion == 'nueva_subclave'){
    
    form.getField('date_fecha_inicio_contratacion')?.setReadonly(true);
    form.getField('pl_consultor_riesgos')?.setReadonly(true);
    form.getField('txt_email_consultor_riesgos')?.setReadonly(true);
    form.getField('pl_estado_desarrollo_comercial')?.setReadonly(true);
    form.getField('pl_segmento')?.setReadonly(true);
    form.getField('pl_irpf')?.setReadonly(true);
    form.getField('date_fecha_registro_dgs')?.setReadonly(true);
    form.getField('pl_agente_doble')?.setReadonly(true);
    form.getField('txt_numero_registro_dgs')?.setReadonly(true);
	form.getField('pl_facturacion_propia')?.setReadonly(true);
    form.getField('cb_asociaciones')?.setReadonly(true);
    form.getField('pl_homologado')?.setReadonly(true);
    form.getField('cb_asociaciones')?.setReadonly(true);
    form.getField('cb_asociaciones')?.setReadonly(true);
    
    form.getWidget('section_objetivos')?.hide();
    form.getField('txt_iban')?.hide();
    form.getField('txt_entidad_bancaria')?.hide();
    form.getField('txt_bic')?.hide();
    form.getField('pl_forma_pago')?.hide();
    form.getField('pl_ramo')?.hide();
    form.getField('pl_grupos_especiales')?.hide();
    form.getField('pl_volumen_cartera')?.hide();
  }
  
}
