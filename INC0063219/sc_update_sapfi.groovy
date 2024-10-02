//MÉTODO PARA ACTUALIZAR LOS REGISTROS EN PROVEEDORES_SAPFI AL MODIFICAR UN MEDIADOR
def modificacionMediador(def campos, def caseName, def username) {
  
  def currentCase = resp.getCase(caseName)
  def caseId = currentCase.getId()
  
  def naturaleza = currentCase.getCustomFieldValue('pl_naturaleza').toUpperCase()
  def queryNaturaleza = "SELECT * FROM NATURALEZA WHERE DESC = '$naturaleza'";
  campos['naturaleza'] = resp.getCustomTable().findFirst(queryNaturaleza).CODIGO;
  logger.info('naturaleza: ' + campos['naturaleza'])
  
  def irpf = 0; // asignamos 0 por defecto
  
  logger.info('IRPF: ' + currentCase.getCustomFieldValue('pl_irpf'));
  
  def tipoMediador = currentCase.getCustomFieldValue('pl_tipo_mediador').toUpperCase()
  tipoMediador = tipoMediador != 'PLATAFORMA' ? tipoMediador : 'Plataforma'
  def queryCanal = "SELECT * FROM TIPO_MEDIADOR WHERE DESCRIPCION = '$tipoMediador'" 
  
  def datosCanal = resp.getCustomTable().findFirst(queryCanal);
  def canal = datosCanal?.CANAL
  //DASP 20240903 Se corrige error en lógica de inserción de fact propia si/no
  campos['facturacionPropia'] = currentCase.getCustomFieldValue('pl_facturacion_propia') == 'no' ? 'false' : 'true';
  
  def bu = currentCase.getCustomFieldValue('pl_business_unit')?.capitalize();
  def pais = currentCase.getCustomFieldValue('pl_pais')?.capitalize();
  
  campos['idHost'] = currentCase.getCustomFieldValue('txt_idhost')
  
  
  def libCesce = resp.importScript('lib_cesce');
  def db = resp.dbConnect('datasource.CESCEdb');
  
  def queryDatosSAPFI = "SELECT * FROM SAP_FINANCIERO WHERE BUSINESSUNIT = '$bu' AND CANAL = '$canal'";

  
  def datosSAPFI = resp.getCustomTable().findAll(queryDatosSAPFI);
  logger.info('datosSAPFI: ' + datosSAPFI?.size());

  for (def i = 0; i < datosSAPFI.size(); i++) {
    
    def queryPais = "SELECT * FROM PAISES WHERE PAIS = '$pais'"
    def datosPais = resp.getCustomTable().findFirst(queryPais);
    
    logger.info('datosPais: ' + datosPais);
    
    def idioma = (datosPais?.PAIS == 'Portugal') ? 'P' : 'S'; 

    def datosBanco = getDatosBanco(campos['iban'], datosPais);
    def datosProveedor = getDatosProveedor(datosBanco, datosSAPFI[i], datosPais, campos);
    
    def deleteRegistroRepetido = """DELETE FROM EXT.PROVEEDORES_SAPFI 
								  	WHERE CODIGO_HOST = '""" + datosSAPFI[i].RAMO + campos['idHost']  + """'
									AND SOCIEDAD = '""" + datosSAPFI[i]?.SOCIEDAD + """'
									AND ESTADO = 'PTE_ENVIO'"""; 
    logger.info('deleteRegistroRepetido: ' + deleteRegistroRepetido);
    db.execute(deleteRegistroRepetido);
    
    def sql = query(campos, datosBanco, datosProveedor, datosSAPFI[i], datosPais, idioma, caseId, username, 'Modificacion', irpf)

    db.execute(sql);
    logger.info('sql: ' + sql)
  }
  
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

//MÉTODO PARA INSERTAR UNA NUEVA SUBCLAVE EN PROVEEDORES_SAPFI
def crearSubclave(def campos, def caseName, def username) {
  
  def currentCase = resp.getCase(caseName)
  def caseId = currentCase.getId()
  
  def libCesce = resp.importScript('lib_cesce');
  def db = resp.dbConnect('datasource.CESCEdb');
  
  def tipoMediador = currentCase.getCustomFieldValue('pl_tipo_mediador').toUpperCase()
  tipoMediador = tipoMediador != 'PLATAFORMA' ? tipoMediador : 'Plataforma'
  def queryCanal = "SELECT * FROM TIPO_MEDIADOR WHERE DESCRIPCION = '$tipoMediador'" 
  def datosCanal = resp.getCustomTable().findFirst(queryCanal);
  def canal = datosCanal?.CANAL
  
  def queryDatosSAPFI = "SELECT * FROM SAP_FINANCIERO WHERE BUSINESSUNIT = 'Spain' AND CANAL = '$canal'";
  
  def datosSAPFI = resp.getCustomTable().findAll(queryDatosSAPFI);
    
  for (def i = 0; i < datosSAPFI.size(); i++) {
    def pais = libCesce.invoke('nombrePaisFromCodigo', campos['codigoPais']);
    
  	def queryPais = "SELECT * FROM PAISES WHERE PAIS = '$pais'";
    def datosPais = resp.getCustomTable().findFirst(queryPais);
    def idioma = (pais == 'Portugal') ? 'P' : 'S'; 

    def datosBanco = getDatosBanco(campos['iban'], datosPais);
    def datosProveedor = getDatosProveedor(datosBanco, datosSAPFI[i], datosPais, campos);
    logger.debug('irpf: ' + campos['irpf'])
    def irpf;
    
  if(campos['irpf']){
     irpf = (campos['irpf']*100).toString()?.split('\\.')[0];
  }
    logger.info('irpf2 :' + irpf)
   
   	def sql = query(campos, datosBanco, datosProveedor, datosSAPFI[i], datosPais, idioma,  caseId, username, 'Alta', irpf)
	logger.info('sql: ' + sql);
    db.execute(sql);
  }
}

//QUERY QUE INSERTA EN LA TABLA PROVEEDORES_SAPFI
def query(def campos, def datosBanco, def datosProveedor, def datosSAPFI, def datosPais, def idioma, def caseId, def username, def tipoMod, def irpf) {
  //20240919 añadimos doble comilla simple para que no de errores en el insert
  def columnas = "INSERT INTO \"EXT\".\"PROVEEDORES_SAPFI\" (";
  def valores =  " VALUES( ";

  columnas += '"CODIGO_HOST"';
  valores += "'" + datosSAPFI.RAMO + campos['idHost'] + "'";
  
  columnas +=  datosProveedor['grupoCuentas'] ? ', "GRUPO_CUENTAS"' : '';
  valores +=  datosProveedor['grupoCuentas'] ? ", '" + datosProveedor['grupoCuentas']?.trim()?.take(4) + "'" : '';
  
  def nombreCompleto = (campos['nombre']?:'') + ' ' + (campos['lastName']?:'');

  columnas += ', "NOMBRE"';
  valores += ", '" + nombreCompleto?.replace("'", "''").trim()?.take(40) + "'";
  
  columnas += campos['direccion'] ? ', "CALLE"' : '';
  valores += campos['direccion'] ? ", '" + campos['direccion']?.replace("'", "''").trim()?.take(60) + "'" : '';
  columnas += campos['cPostal'] ? ', "CODIGO_POSTAL"' : '';
  valores += campos['cPostal'] ? ", '" +  campos['cPostal']?.replace("'", "''").trim()?.take(10) + "'" : '';
  columnas += campos['localidad'] ? ', "POBLACION"' : '';
  valores += campos['localidad'] ? ", '" +  campos['localidad']?.replace("'", "''").trim()?.take(40) + "'" : '';
  
  columnas += datosPais?.CODIGO_ISO ? ', "PAIS_ISO"' : '';
  valores +=  datosPais?.CODIGO_ISO ? ", '" + datosPais?.CODIGO_ISO?.trim()?.take(3) + "'" : ''; 
  
  columnas += campos['codigoProvincia'] ? ', "ID_REGION"' : '';
  // 20240805 DASP Se incluye lógica para enviar valor vacío a LATAM.
  if(datosPais?.CODIGO_ISO?.trim()?.take(3) == 'ES'){
    valores += campos['codigoProvincia'] ? ", LPAD('" +  campos['codigoProvincia'] + "', 2, '0')" : ''
  } 
  // DASP 20240821 Quieren que se envíe vacío para Portugal

	else {
    valores += campos['codigoProvincia'] ? ", '' " : ''
  }
  
  columnas += idioma ? ', "IDIOMA"' : '';
  valores += idioma ?", '" + idioma?.trim()?.take(1) + "'" : '';
  
  columnas += campos['telefono'] ? ', "TELEFONO"' : '';
  valores += campos['telefono'] ? ", '" + campos['telefono']?.trim()?.take(30) + "'" : '';
  
  columnas += campos['email'] ? ', "EMAIL"' : '';
  valores += campos['email'] ? ", '" + campos['email']?.trim()?.take(241) + "'" : '';
  
  
  ////////////////////////////////////////////////////////////////////////////////////
  //INC0063219 SMM - Para las BU de Chile añadir carácter '-' antes del último dígito
  //columnas += ', "CODIGO_FISCAL"'; 
  //valores += ", '" + campos['identFiscal']?.trim()?.take(16) + "'"; 

  def bu = campos['businessUnit'].toUpperCase();
  def identFiscal = campos['identFiscal']?.trim()?.take(16) 
  columnas += campos['identFiscal'] ? ', "CODIGO_FISCAL"' : '';
       
  if (bu == 'CHILE'){
    if (identFiscal && identFiscal[-2] != '-'){
            identFiscal = identFiscal.substring(0, identFiscal.length() - 1) + '-' + identFiscal[-1];
            //valores += campos['identFiscal'] ? ", '" + identFiscal + "'" : '';      
    } 
    valores += campos['identFiscal'] ? ", '" + identFiscal + "'" : '';         
  }
  else {
      valores += campos['identFiscal'] ? ", '" + identFiscal + "'" : '';      
  }
 ////////////////////////////////////////////////////////////////////////////////////
  
  columnas += ', "PERSONA_FISICA"';
  valores += (campos['naturaleza'] == 'PF') ? ", 'X'" : ", ''";
  
  columnas += datosSAPFI?.RAMO ? ', "RAMO"' : '';
  valores += datosSAPFI?.RAMO ? ", '" + datosSAPFI?.RAMO?.trim()?.take(4) + "'" : '';
  
  columnas += ', "HAY_PAGOS"';
  valores += campos['iban'] ? ", 'S'" : ", 'N'";
  
  columnas += campos['iban'] ? ', "CLAVE_BANCO"' : ''; 
  valores += campos['iban'] ? ", '" + campos['iban'].replaceAll('\\s', '')?.substring(4,12) + "'" : '';
  
  def nombreBanco = datosBanco?.NOMBRE_BANCO?:'NO_ENCONTRADO';
  
  columnas += ', "NOMBRE_BANCO"';
  valores += campos['iban'] ? ", '"+nombreBanco?.take(60)+"'" : ", 'NO_ENCONTRADO' ";
  
  logger.info('COD PAIS: ' + campos['iban']?.substring(0,2))
  
  columnas += campos['iban'] ? ', "CUENTA_BANCARIA"' : '';
  if(campos['iban']?.substring(0,2) == 'ES'){
    valores += campos['iban'] ? ", '"+ campos['iban']?.replaceAll('\\s', '')?.substring(14)+"'" : '';
  } else if (campos['iban']?.substring(0,2) == 'PT'){
    valores += campos['iban'] ? ", '"+ campos['iban']?.replaceAll('\\s', '')?.substring(12,23)+"'" : '';
  } else { // resto paises?
    valores += campos['iban'] ? ", '"+ campos['iban']?.replaceAll('\\s', '')?.substring(14)+"'" : '';
  }
  columnas += campos['iban'] ? ', "IBAN"' : '';
  valores += campos['iban'] ? ", '" + campos['iban']?.replaceAll('\\s', '')?.take(34) + "'" : '';
  
  columnas += campos['bic'] ? ', "SWIFT_BIC"' : '';
  valores += campos['bic'] ? ", '" + campos['bic']?.trim()?.take(11) + "'" : '';
  
  columnas += datosSAPFI?.SOCIEDAD ? ', "SOCIEDAD"' : '';
  valores += datosSAPFI?.SOCIEDAD ? ", '" + datosSAPFI?.SOCIEDAD?.trim()?.take(4) + "'" : '';
  
  columnas += datosSAPFI?.CUENTA_ASOCIADA ? ', "CUENTA_ASOCIADA"' : '';
  valores += datosSAPFI?.CUENTA_ASOCIADA ? ", '" + datosSAPFI?.CUENTA_ASOCIADA?.trim()?.take(10) + "'" : '';
  
  columnas += datosSAPFI?.GRUPO_TESORERIA ? ', "GRUPO_TESORERIA"' : '';
  valores += datosSAPFI?.GRUPO_TESORERIA ? ", '" + datosSAPFI?.GRUPO_TESORERIA?.trim()?.take(10) + "'" : '';
  
  columnas += datosProveedor['condicionPago'] ? ', "CONDICION_PAGO"' : '';
  valores += datosProveedor['condicionPago'] ? ", '" + datosProveedor['condicionPago']?.trim()?.take(4) + "'" : '';
  
  columnas += datosProveedor['viaPago'] ? ', "VIA_PAGO"' : '';
  valores += datosProveedor['viaPago'] ? ", '" + datosProveedor['viaPago']?.trim()?.take(10) + "'" : '';
  
  columnas += datosSAPFI?.TIPO_PROVEEDOR ? ', "TIPO_PROVEEDOR"' : '';
  valores += datosSAPFI?.TIPO_PROVEEDOR ? ", '" + datosSAPFI?.TIPO_PROVEEDOR?.trim()?.take(25) + "'" : '';
  
  columnas += datosSAPFI?.TIPO_SOCIEDAD ? ', "TIPO_SOCIEDAD"': '';
  valores += datosSAPFI?.TIPO_SOCIEDAD ? ", '" + datosSAPFI?.TIPO_SOCIEDAD?.trim()?.take(20) + "'" : '';
  
  columnas += ', "TIPO_PAGO"';
  valores += ", '" + datosSAPFI?.TIPO_PAGO?.trim()?.take(1) + "'";
  
  columnas += irpf ? ', "IRPF"' : '';
  valores += irpf ? ", '" + irpf?.trim()?.take(2) + "'" : '';
  
  columnas += ', "TIPO_MODIFICACION"';
  //valores += ", '$tipoMod'"
  valores += tipoMod ? ", '" + tipoMod?.trim()?.take(12) + "'" : '';
  columnas += ', "ESTADO"';
  valores += ", 'PTE_ENVIO'";
  columnas += ', "CASEID"';
  valores += caseId ? ", '" + caseId + "'" : '';
  columnas += username ? ', "USER"' : '';
  valores += username ? ", '" + username?.trim()?.take(50) + "'" : '';
  columnas += ', "CREATEDATE"'
  valores += ", NOW()"
  columnas += ', "MODIF_DATE"'
  valores += ", NOW()"

  logger.info('query sapfi: ' + columnas + '\n' + valores)
  return columnas + ') ' + valores + ')';
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

def getDatosProveedor(def datosBanco, def datosSAPFI, def datosPais, def campos){

    def datosProveedor = [:];
    datosProveedor['condicionPago'] = '';
    datosProveedor['viaPago'] = '';
    datosProveedor['grupoCuentas'] = '';

  	//España
    if(datosSAPFI.SOCIEDAD == 'CE01' || datosSAPFI.SOCIEDAD == 'CE14'){
      if(datosPais?.PAIS == 'España'){
        datosProveedor['grupoCuentas'] = 'PNAC';
      }else if(datosPais?.PERTENECE_UE == 'TRUE'){
        datosProveedor['grupoCuentas'] = 'PRUE';
      }else{ 
        datosProveedor['grupoCuentas'] = 'PEXT';
      }
    }
  
  	//Portugal
  	else if(datosSAPFI.SOCIEDAD == 'CE03' || datosSAPFI.SOCIEDAD == 'CE14'){
      if(datosPais.PAIS == 'Portugal'){
        datosProveedor['grupoCuentas'] = 'PNAC';
      }else if(datosPais.PERTENECE_UE == 'TRUE'){
        datosProveedor['grupoCuentas'] = 'PRUE';
      }else{ 
        datosProveedor['grupoCuentas'] = 'PEXT';
      }
    }
  	
  	//Resto de sociedades
  	else{
      datosProveedor['grupoCuentas'] = 'PLAT'
    }
	//España
    if(['CE01','CE14'].contains(datosSAPFI.SOCIEDAD?.toUpperCase()) && datosPais.PAIS == 'España'){
        if(campos['formaPago'] == '1'){
          datosProveedor['viaPago'] = 'E';
          datosProveedor['condicionPago'] = '000E';
        }else if(campos['formaPago'] == '2'){
          if(datosPais.PERTENECE_UE == 'FALSE'){
            datosProveedor['viaPago'] = 'O';
            datosProveedor['condicionPago'] = '000O';
          }else{
            if(campos['facturacionPropia'] == 'true'){
              datosProveedor['viaPago'] = 'X';
              datosProveedor['condicionPago'] = '000X';
            }else{
              datosProveedor['viaPago'] = 'W';
              datosProveedor['condicionPago'] = '000W';
            }
          }
        }
    }
      
    //Portugal
    else if(['CE03','CE14'].contains(datosSAPFI.SOCIEDAD?.toUpperCase()) && datosPais.PAIS == 'Portugal'){
       datosProveedor['viaPago'] = 'V';
       datosProveedor['condicionPago'] = '000V';
    }
  
  	else{
      if(campos['formaPago'] == '1'){
        datosProveedor['viaPago'] = 'C';
        datosProveedor['condicionPago'] = '000C';
      }else if(campos['formaPago'] == '2'){
        datosProveedor['viaPago'] = 'T';
        datosProveedor['condicionPago'] = '000T';
      }else if(campos['formaPago'] == '3'){
        datosProveedor['viaPago'] = 'P';
        datosProveedor['condicionPago'] = '000P';
      }
    }
  
  	logger.info('datosProveedor: ' + datosProveedor)

    return datosProveedor;

}

def getDatosBanco(def iban, def datosPais){
  // Ahora tomamos los datos de la BBDD
  
  def codBanco;
  if(iban?.substring(0,2) == 'ES' || iban?.substring(0,2) == 'PT'){
    codBanco = iban.substring(4,12);
  }

  def db = resp.dbConnect('datasource.CESCEdb');
  def queryBanco = "SELECT * FROM EXT.REGISTRO_BANCOS WHERE CLAVE_BANCO = $codBanco "
  
  logger.info('queryBanco: ' + queryBanco)
  
  def resultBanco = db.queryForList(queryBanco);
  return resultBanco[0];
}