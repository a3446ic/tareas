def db = resp.dbConnect('datasource.CESCEdb')

def tabla = form.getTableController('tbl_cartera')

def departamento = currentUser.getDepartment()?.getName()?.toUpperCase();
logger.info('departamento: ' + departamento)
userName = currentUser.getName().toUpperCase();
logger.info('Nombre: ' + userName);
userType = currentUser.getUserType();
logger.info('userType: ' + userType)

def filtros = ''

def ramo = form.getField('mult_pl_ramo').getValue()
if(ramo) {
  ramo = ramo.collect {
    it = "'" + it + "'"
  }
  def ram = '(' + ramo.join(',') + ')'
  ram = ram.toUpperCase()
  filtros += / AND ctr.RAMO IN $ram /
}

def sin_mediador = form.getField('cb_sin_mediador').getValue() ?: ''

           
def mediador = form.getField('au_mediador').getValue() ?: ''

def numPoliza = form.getField('txt_poliza').getValue() ?: ''
if(numPoliza){
	filtros += "AND ctr.NUM_POLIZA = '$numPoliza'"  
}


// RITM0053155 - sustituir Num. Aval por Número Fianza en Consulta Cartera
/*
def numAval = form.getField('txt_aval').getValue() ?: ''
if(numAval){
	filtros += "AND ctr.NUM_AVAL_HOST = '$numAval'"  
}
*/

def numFianza = form.getField('txt_fianza').getValue() ?: ''
if(numFianza){
	filtros += "AND ctr.NUM_FIANZA = '$numFianza'"  
}

def numAval = form.getField('txt_aval').getValue() ?: ''
if(numAval){
	filtros += "AND ctr.NUM_AVAL_HOST = '$numAval'"  
}

def nifTomador = form.getField('txt_nif_tomador').getValue() ?: ''
if(nifTomador){
	filtros += "AND ctr.NIF_TOMADOR = '$nifTomador'"  
}

def date = form.getField('date_fecha_actividad').getValue() ?: ''
if(date) {
  logger.debug('date', date)
  filtros += "AND TO_DATE('$date') > ctr.FECHA_INICIO AND TO_DATE('$date') <  ctr.FECHA_FIN "
}

// Si está marcado Sin Mediador se busca por POSITIONNAME='0000-0000' - canal Directo
if(sin_mediador) {
   filtros += / AND ctr.COD_MEDIADOR='0000' /
} else {

	if(mediador) {
	 /*
	  filtros += /AND (CONCAT(CONCAT(CONCAT_NAZ(CONCAT(CONCAT(CONCAT(CONCAT(mom.COD_MEDIADOR, '-'), mom.SUBCLAVE), ' '),mom.NUM_IDENTIFICACION), mom.NOMBRE), ' '), mom."APELLIDO\/RAZON_SOCIAL")) = '$mediador'/
  */
	  // Se simplifica el filtro del codigo de mediador
	  filtros += /AND LOCATE( '$mediador', mom.POSITIONNAME, 1 ) > 0 /
	}  
  
}


if(!filtros) {
  resp.alert.info("Debe introducir al menos un filtro para la consulta")
} else {
  
  if(departamento != 'CENTRAL CESCE'){

    filtros += / AND mom.DIR_TERRITORIAL = '$departamento'/

    if(userType == 'gerente_canal'){
      filtros += / AND UPPER(mom.GERENTE_CANAL) LIKE '%$userName%'/
    }

  }
  if(departamento == 'CENTRAL CESCE' && userType == 'perfil_consulta_espana'){

    //filtros += / AND mom.DIR_TERRITORIAL = '$departamento'/
    filtros += / AND mom.DIR_TERRITORIAL IN ('DT CENTRO','DT NORTE','DT SUR','DT CATALUÑA-BALEARES','DT LEVANTE','DT NOROESTE')/

    if(userType == 'gerente_canal'){
      filtros += / AND UPPER(mom.GERENTE_CANAL) LIKE '%$userName%'/
    }

  }
  

  def results = []
  def limDown = 1
  def limUp = 1001
  def continuarBucle = true



  while(continuarBucle) {
    def sql = """SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY ctr.COD_MEDIADOR) RN, ctr.RAMO, ctr.NUM_POLIZA, ctr.NUM_AVAL_HOST, ctr.IDPRODUCT, IFNULL(mom.PositionName, ctr.COD_MEDIADOR || '-' || ctr.COD_SUBCLAVE ) as PositionName, IFNULL(CONCAT_NAZ(CONCAT_NAZ(mom.NOMBRE, ' '), mom."APELLIDO/RAZON_SOCIAL"), '') as NOMBRE, CTR.P_INTERMEDIACION
      , CASE WHEN ctr.FECHA_INICIO = '1990-12-31' THEN '' ELSE FECHA_INICIO END FECHA_INICIO 
      , CASE WHEN ctr.FECHA_FIN = '2200-01-01' THEN '' END FECHA_FIN
      , CASE WHEN ctr.ACTIVO = 0 THEN 'DESHABILITADO' 
        WHEN ctr.ACTIVO = 1 THEN 'ACTIVO'
        WHEN ctr.ACTIVO = 2 THEN 'ESPERA DE RENOVACIÓN'
        END ACTIVO
      , ctr.FECHA_EFECTO_TRASPASO, ctr.NOMBRE_TOMADOR, ctr.NIF_TOMADOR, 
      ctr.PRIMA_MIN_EXT, ctr.PRIMA_MIN_INT, ctr.IDPAIS, ctr.FECHA_VENCIMIENTO, ctr.FECHA_EFECTO, 
      ctr.FECHA_EMISION, ctr.NUM_ANUALIDAD, ctr.NUM_EXPEDIENTE, ctr.NUM_FIANZA, ctr.P_ESPECIAL_EMISION, ctr.P_ESPECIAL_RENOVACION,
      ctr.FECHA_INICIO_OPESP, ctr.FECHA_FIN_OPESP
    FROM EXT.CARTERA ctr
    LEFT JOIN EXT.MODIFICAR_MEDIADOR mom ON  mom.COD_MEDIADOR = ctr.COD_MEDIADOR AND mom.SUBCLAVE = ctr.COD_SUBCLAVE
    WHERE 1=1 
    $filtros) WHERE RN >= $limDown AND RN < $limUp
    ORDER BY RAMO DESC,NUM_POLIZA,PositionName,NUM_FIANZA,ACTIVO,NUM_ANUALIDAD,FECHA_INICIO"""

    def resultsBucle = db.queryForList(sql)
    
    logger.info("Consulta Cartera" , sql +  "\n" + "RESULTADOS: " + resultsBucle.size().toString() + " registros ")
    
    if(resultsBucle.size() == 0) {
      continuarBucle = false
    } else {
      limDown += 1000
      limUp += 1000
      results.addAll(resultsBucle)
    }
  }

    def list = []


    results?.each {
      Map elementMap = new java.util.HashMap()

      elementMap.put("RAMO", it.RAMO)

      elementMap.put("NUM_POLIZA", it.NUM_POLIZA)

      elementMap.put("NUM_AVAL_HOST", it.NUM_AVAL_HOST)

      elementMap.put("IDPRODUCT", it.IDPRODUCT)

      elementMap.put("PositionName", it.PositionName)

      elementMap.put("NOMBRE", it.NOMBRE)

      elementMap.put("P_INTERMEDIACION", it.P_INTERMEDIACION)

      elementMap.put("FECHA_INICIO", it.FECHA_INICIO)

      elementMap.put("FECHA_FIN", it.FECHA_FIN)

      elementMap.put("ACTIVO", it.ACTIVO)

      elementMap.put("NUM_FIANZA", it.NUM_FIANZA)

      elementMap.put("NUM_EXPEDIENTE", it.NUM_EXPEDIENTE)

      elementMap.put("NUM_ANUALIDAD", it.NUM_ANUALIDAD)

      elementMap.put("FECHA_EMISION", it.FECHA_EMISION)

      elementMap.put("FECHA_EFECTO", it.FECHA_EFECTO)

      elementMap.put("FECHA_VENCIMIENTO", it.FECHA_VENCIMIENTO)

      elementMap.put("IDPAIS", it.IDPAIS)

      elementMap.put("PRIMA_MIN_INT", it.PRIMA_MIN_INT)

      elementMap.put("PRIMA_MIN_EXT", it.PRIMA_MIN_EXT)

      elementMap.put("NIF_TOMADOR", it.NIF_TOMADOR)

      elementMap.put("NOMBRE_TOMADOR", it.NOMBRE_TOMADOR)

      elementMap.put("FECHA_EFECTO_TRASPASO", it.FECHA_EFECTO_TRASPASO)

      elementMap.put("P_ESPECIAL_EMISION", it.P_ESPECIAL_EMISION);
      
      elementMap.put("P_ESPECIAL_RENOVACION", it.P_ESPECIAL_RENOVACION);
      
      elementMap.put("FECHA_INICIO_OPESP", it.FECHA_INICIO_OPESP);
      
      elementMap.put("FECHA_FIN_OPESP", it.FECHA_FIN_OPESP);
      
      list.add(elementMap);
    }

  tabla.setSource(list)
  form.refreshElement('tbl_cartera')
}