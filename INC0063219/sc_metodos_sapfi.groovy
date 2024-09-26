def proveedorSapfi(def campos, def datosSAPFI, def modificacion, def caseId, def user){

  def libCesce = resp.importScript('lib_cesce');
  def db = resp.dbConnect('datasource.CESCEdb');
    
  def pais = libCesce.invoke('nombrePaisFromCodigo', campos['codigoPais']);
  def queryPais = "SELECT * FROM PAISES WHERE IDPAIS = '" + campos['codigoPais'] + "'";
  def datosPais = resp.getCustomTable().findFirst(queryPais);
  def idioma = (datosPais.PAIS == 'Portugal') ? 'P' : 'S'; 

  def datosBanco = getDatosBanco(campos['iban'], datosPais);
  logger.info('datosBanco: ' + datosBanco)
  def datosProveedor = getDatosProveedor(datosBanco, datosSAPFI, datosPais, campos);
  logger.info('datosProveedor: ' + datosProveedor)
  def nombreCompleto = (campos['nombre']?:'') + ' ' + (campos['lastName']?:'');
  
  // ELIMINAR REGISTRO SI YA EXISTE:
  def codFiscal = campos['identFiscal'];
  def queryDelete = "DELETE FROM EXT.PROVEEDORES_SAPFI WHERE CODIGO_FISCAL = '$codFiscal' AND ESTADO = 'PTE_ENVIO'"
  logger.info('queryDelete: ' + queryDelete)
  db.execute(queryDelete)
  
  //INSERT EN EXT.PROVEEDORES_SAPFI
  def queryInsertDatos = queryDatosSAPFI(campos, datosBanco, datosProveedor, nombreCompleto, datosSAPFI, datosPais, idioma, modificacion, caseId, user);
  
  logger.info('queryInsertDatos SAPFI: ' + queryInsertDatos)

  def insert = db.execute(queryInsertDatos);
  	
  return insert;
}



def getDatosProveedor(def datosBanco, def datosSAPFI, def datosPais, def campos){

    def datosProveedor = [:];
    datosProveedor['condicionPago'] = '';
    datosProveedor['viaPago'] = '';
    datosProveedor['grupoCuentas'] = '';
	
  	//España
    if(datosSAPFI.SOCIEDAD == 'CE01' || datosSAPFI.SOCIEDAD == 'CE14'){
      if(datosPais.PAIS == 'España'){
        datosProveedor['grupoCuentas'] = 'PNAC';
      }else if(datosPais.PERTENECE_UE == 'TRUE'){
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
    if(['CE01','CE14'].contains(datosSAPFI.SOCIEDAD?.toUpperCase())){
      if(datosPais.PAIS == 'España'){
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
    }
      
    //Portugal
    else if(['CE03','CE14'].contains(datosSAPFI.SOCIEDAD?.toUpperCase())){
  	  if(datosPais.PAIS == 'Portugal'){
       datosProveedor['viaPago'] = 'V';
       datosProveedor['condicionPago'] = '000V';
      }
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

    return datosProveedor;

}

def getDatosBanco(def iban, def datosPais){
  
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


def queryDatosSAPFI(def campos, def datosBanco, def datosProveedor, def nombreCompleto, def datosSAPFI, def datosPais, def idioma, def modificacion, def caseId, def username){
  logger.info('COMIENZO INSERT PROVEEDORES_SAPFI')
  //20240919 añadimos doble comilla simple para que no de errores en el insert
  def columnas = "INSERT INTO \"EXT\".\"PROVEEDORES_SAPFI\" (";
  def valores =  " VALUES( ";

  columnas += '"CODIGO_HOST"';
  valores += "'" + datosSAPFI.RAMO + campos['idHost'] + "'"
  columnas += ', "GRUPO_CUENTAS"';
  valores += ", '" + datosProveedor['grupoCuentas']?.trim()?.take(4) + "'";
  
  columnas += ', "NOMBRE"';
  valores += ", '" + nombreCompleto?.replace("'", "''").trim()?.take(40) + "'";
  
  columnas += ', "CALLE"';
  valores += ", '" + campos['direccion']?.replace("'", "''").trim()?.take(60) + "'";
  columnas += ', "CODIGO_POSTAL"';
  valores += ", '" +  campos['cPostal']?.replace("'", "''").trim()?.take(10) + "'";
  columnas += ', "POBLACION"';
  valores += ", '" +  campos['localidad']?.replace("'", "''").trim()?.take(40) + "'";
  columnas += ', "PAIS_ISO"';
  valores += ", '" + datosPais?.CODIGO_ISO?.trim()?.take(3) + "'"; 
  columnas += ', "ID_REGION"';

  // 20240805 DASP Se incluye lógica para enviar valor vacío a LATAM.
  if(datosPais?.CODIGO_ISO?.trim()?.take(3) == 'ES'){
    valores += ", LPAD('" +  campos['codigoProvincia'] + "', 2, '0')"
  } 
  // DASP 20240821 Quieren que se envíe vacío para Portugal

  else {
    valores += ", '' "
  }
  
  columnas += ', "IDIOMA"';
  valores += ", '" + idioma?.trim()?.take(1) + "'";
  columnas += ', "TELEFONO"';
  valores += ", '" + campos['telefono']?.trim()?.take(30) + "'";
  
  def email = (campos['email']?:'noemail@noemail.com');
  
  columnas += ', "EMAIL"';
  valores += ", '" + email + "'";
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
  valores += (campos['naturaleza'] == 'PF') ? ", 'X'" : ", '' "; 
  columnas += ', "RAMO"';
  valores += ", '" + datosSAPFI?.RAMO?.trim()?.take(4) + "'";
  columnas += ', "HAY_PAGOS"';
  valores += campos['iban'] ? ", 'S'" : ", 'N' ";
  columnas += ', "CLAVE_BANCO"';
  valores += campos['iban'] ? ", '"+ campos['iban']?.replaceAll('\\s', '')?.substring(4,12) +"'" : ", '' ";
  
  def nombreBanco = datosBanco?.NOMBRE_BANCO?:'NO_ENCONTRADO';
  
  columnas += ', "NOMBRE_BANCO"';
  valores += campos['iban'] ? ", '"+nombreBanco?.take(60)+"'" : ", 'NO_ENCONTRADO' ";
  
  columnas += ', "CUENTA_BANCARIA"';
  if(campos['iban']?.substring(0,2) == 'ES'){
    valores += ", '" + campos['iban']?.replaceAll('\\s', '')?.substring(14) + "'";
  } else if (campos['iban']?.substring(0,2) == 'PT'){
    valores += ", '" + campos['iban']?.replaceAll('\\s', '')?.substring(12,23) + "'";
  } else { // resto paises
    valores += campos['iban'] ? ", '"+ campos['iban']?.replaceAll('\\s', '')?.substring(14) +"'" : ", '' ";
  }
  columnas += ', "IBAN"';
  valores += campos['iban'] ? ", '"+ campos['iban']?.trim()?.take(34) +"'" : ", '' ";
  columnas += ', "SWIFT_BIC"';
  valores += campos['iban'] ? ", '"+ campos['bic']?.trim()?.take(11) +"'" : ", '' ";
  columnas += ', "SOCIEDAD"';
  valores += ", '" + datosSAPFI?.SOCIEDAD?.trim()?.take(4) + "'";
  columnas += ', "CUENTA_ASOCIADA"';
  valores += ", '" + datosSAPFI?.CUENTA_ASOCIADA?.trim()?.take(10) + "'";
  columnas += ', "GRUPO_TESORERIA"';
  valores += ", '" + datosSAPFI?.GRUPO_TESORERIA?.trim()?.take(10) + "'";
  columnas += ', "CONDICION_PAGO"';
  valores += ", '" + datosProveedor['condicionPago']?.trim()?.take(4) + "'";
  columnas += ', "VIA_PAGO"';
  valores += ", '" + datosProveedor['viaPago']?.trim()?.take(10) + "'";
  columnas += ', "BLOQUEO_EMBARGO"';
  valores += ", '" + "'";
  columnas += ', "TIPO_PROVEEDOR"';
  valores += ", '" + datosSAPFI?.TIPO_PROVEEDOR?.trim()?.take(25) + "'";
  columnas += ', "TIPO_SOCIEDAD"';
  valores += ", '" + datosSAPFI?.TIPO_SOCIEDAD?.trim()?.take(20) + "'";
  columnas += ', "TIPO_PAGO"';
  valores += ", '" + datosSAPFI?.TIPO_PAGO?.trim()?.take(1) + "'";
  
  if(campos['irpf']){
    def irpf = (campos['irpf']*100).toString()?.split('\\.')[0];
    columnas += ', "IRPF"';
    valores += ", '" + irpf?.trim()?.take(2) + "'";
  }
  columnas += ', "TIPO_MODIFICACION"';
  valores += ", '" + modificacion?.trim()?.take(12) + "'";
  columnas += ', "ESTADO"';
  valores += ", 'PTE_ENVIO'";
  columnas += ', "CASEID"'
  valores += ", '" + caseId?.toString().take(50) + "'";
  columnas += ', "USER"'
  valores += ", '" + username?.trim()?.take(50) + "'";
  columnas += ', "CREATEDATE"'
  valores += ", NOW()"
  columnas += ', "MODIF_DATE"'
  valores += ", NOW()"
  logger.info('valores: ' + valores)
  return columnas + ') ' + valores + ')';

}
