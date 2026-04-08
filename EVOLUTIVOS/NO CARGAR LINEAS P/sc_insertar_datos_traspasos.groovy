logger.info('Inicio: sc_insertar_datos_traspasos');
// Script test data: 
//currentCase = resp.getCase('Traspaso Pólizas y Avales-GM-15');

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

def db = resp.dbConnect('datasource.CESCEdb');

def estado = '';

logger.info('ACCION - '+scriptRuntimeContext.getActionDefnName())
if (scriptRuntimeContext.getActionDefnName() == 'Aprobar Solicitud'){
  estado = 'aprobado';
} else if (scriptRuntimeContext.getActionDefnName() == 'Rechazar Solicitud'){
  estado = 'rechazado';
} else {
  estado = 'creado';
}

def mapaFormulario = currentCase.getCustomFieldValue('txt_mapa_receptores');
def mapa = mapaFormulario ? evaluate(mapaFormulario) : [:];

def polizas = mapa['polizas'];
logger.info('polizas: ' + polizas);
def receptores = mapa['receptores'];
logger.info('receptores: ' + receptores);

def tipoTraspaso = currentCase.getCustomFieldValue('pl_tipo_traspaso');
def tipoMovimiento = currentCase.getCustomFieldValue('pl_tipo_movimiento');

def caseId = currentCase?.getId()?.toString();


def tipoTraspasoCaucion = currentCase.getCustomFieldValue('pl_traspasos_caucion'); 
def ramo = currentCase.getCustomFieldValue('pl_ramo'); 
def dateEfecto = currentCase.getCustomFieldValue('date_efecto').toString(); 
  
def autoCedente; 
if (tipoMovimiento == 'sin_mediador_mediador') {
    autoCedente = null;
} else  {
    autoCedente = currentCase.getCustomFieldValue('auto_mediador');
}
def codigoCedente = autoCedente ? autoCedente.split(' ')?.getAt(0): '0000-0000'; 
def codigoMediadorCedente = codigoCedente?.split('-')?.getAt(0); 
def subclaveMediadorCedente = codigoCedente?.split('-')?.getAt(1);


def eliminarRegistrosMismoCaseId = "DELETE FROM EXT.SOLICITUD_TRASPASO WHERE CASEID = '" + caseId + "'";
db.execute(eliminarRegistrosMismoCaseId);

if(tipoTraspaso == 'parcial'){
    receptores.each{
        for(def poliza:it.'polizas'){
            insertSolicitud(it, poliza, '');
            logger.info('insertSolicitud: $it, $poliza')
        }
    }
}

//TRASPASO TOTAL (NO VIENE NADA EN EL MAPA DE POLIZAS POR LO QUE CONSULTAMOS LAS POLIZAS DEL CEDENTE)
else{
    //si en el traspaso total el ramo seleccionado es ambos, lo ponemos como nulo
    if (ramo == 'ambos') {
        ramo = '';
    }
    queryBusquedaTotal = "SELECT NUM_POLIZA as num_poliza, CODIGO_AVAL as codigo_aval, P_INTERMEDIACION AS participacion, P_ESPECIAL_EMISION AS especial_emision, P_ESPECIAL_RENOVACION AS especial_renovacion FROM EXT.GET_BUSCAR_POLIZAS('$codigoMediadorCedente','$subclaveMediadorCedente','$ramo','$tipoTraspasoCaucion','$dateEfecto','', 0, 0)"; 
    logger.info(queryBusquedaTotal)
    logger.info('sc_insertar_datos_traspasos: Query búsqueda pólizas total: ' + queryBusquedaTotal); 
    def polizasTraspasoTotal = '';
    polizasTraspasoTotal = db.queryForList(queryBusquedaTotal);
    logger.info(polizasTraspasoTotal)
   /* polizasTraspasoTotal.each{
        for(def receptor:receptores)
            insertSolicitud(receptor, it);
    }*/
    for(def receptor:receptores) {
        polizasTraspasoTotal.each{
            if (it == polizasTraspasoTotal.last()){
                insertSolicitud(receptor, it, 'total');
            }
            else {
                insertSolicitud(receptor, it, '');
            }
        }
    }
}

if(estado == 'rechazado'){
  currentCase.setStatus(resp.getStatus('solicitud_rechazada'));
  resp.update(currentCase);
} else if (estado == 'aprobado') {
  currentCase.setStatus(resp.getStatus('solicitud_aprobada'));
  resp.update(currentCase);
}

//////////////////////////////////////////////////////////////////////

def insertSolicitud(def receptor, def poliza, def registroInfoTotal){

    logger.info('receptor: ' + receptor)
    logger.info('poliza: ' + poliza)

    def db = resp.dbConnect('datasource.CESCEdb');

    def tipoMovimiento = currentCase.getCustomFieldValue('pl_tipo_movimiento');
    def tipoTraspaso = currentCase.getCustomFieldValue('pl_tipo_traspaso');
  	def tipoTraspasoCaucion = currentCase.getCustomFieldValue('pl_traspasos_caucion');
  	def ramo = currentCase.getCustomFieldValue('pl_ramo');
	def dateEfecto = currentCase.getCustomFieldValue('date_efecto').toString();
  
    def autoCedente;
    if(tipoMovimiento == 'sin_mediador_mediador'){
        autoCedente = null;
    }else{
        autoCedente = currentCase.getCustomFieldValue('auto_mediador');
    }
	def codigoCedente = autoCedente ? autoCedente.split(' ')?.getAt(0) : '0000-0000';
    def codigoMediadorCedente = codigoCedente?.split('-')?.getAt(0);
    def subclaveMediadorCedente = codigoCedente?.split('-')?.getAt(1);
    logger.info('codigoCedente: ' + codigoCedente)

    def datosPoliza = datosPolizas(poliza.'num_poliza', poliza.'codigo_aval' != '0' ? poliza.'codigo_aval': '', codigoMediadorCedente, subclaveMediadorCedente);
    logger.info('datosPoliza: ' + datosPoliza)

    if(datosPoliza){
  
        logger.info('datosPoliza: ' + datosPoliza)

        def tipoCambio = [
            'sin_mediador_mediador':'1',
            'mediador_mediador':'2',
            'traspaso_otro_mediador':'3',
            'error_captura':'4',
            'mediador_canal_directo':'5',
            'operaciones_especiales':'6',
            'modificar_relacin_intermediacin':'7',
            'traspaso_entre_subclaves':'8'
        ];

        def tiposFechaTraspaso =  [
            'traspaso_con_derechos_y_obligaciones': 'I',
            'sin_derechos_y_obligaciones_a_la_renovacin': 'R',
            'sin_derechos_y_obligaciones_caucion': 'R',
            'sin_derechos_y_obligaciones_al_inicio_de_la_anuali':'A',
            'aplicar':'C',
            'finalizar':'F'
        ];

        // Cálculo de fechas según si el traspaso es CON o SIN derechos y obligaciones
        def tipoFechaTraspaso = currentCase.getCustomFieldValue('pl_date_traspaso');
        def fechaInicio;
        def fechaFin;
        /*
        if(tipoFechaTraspaso == 'traspaso_con_derechos_y_obligaciones'){ // Inicio
            fechaInicio = datosPoliza?.FECHA_EFECTO;//fechaEfecto;
            fechaFin = '2200-01-01';
        }
        else if(tipoFechaTraspaso == 'fecha_de_traspaso'){ // Traspaso
            fechaInicio = currentCase.getCustomFieldValue('date_efecto');
            fechaFin = '2200-01-01';
        }
        else{
            fechaInicio = datosPoliza?.FECHA_INICIO;
            fechaFin = datosPoliza?.FECHA_FIN;
        }
        */
        //CAMBIO_FECHA_INICIO_INTERMEDIACION
        //fechaInicio = currentCase.getCustomFieldValue('date_efecto');

        def numeroPoliza = poliza.'num_poliza';

        //Consultamos la fecha efecto de la anualidad vigente

        def fechaInicioInsertar = '';

        if(datosPoliza?.RAMO?.toUpperCase() == 'CREDITO'){
            def queryFechaAnualidadVigente = "SELECT FECHA_EFECTO, ADD_DAYS(FECHA_VENCIMIENTO,1) AS FECHA_VENCIMIENTO FROM EXT.GET_ANUALIDAD_OP_ESP($codigoMediadorCedente,$subclaveMediadorCedente,$numeroPoliza) WHERE VIGENTE = 1"; 
            logger.info(queryFechaAnualidadVigente)
            def fechaAnualidadVigente = db.queryForList(queryFechaAnualidadVigente)[0]; 
            if(tipoFechaTraspaso == 'sin_derechos_y_obligaciones_a_la_renovacin'){
                fechaInicioInsertar = fechaAnualidadVigente?.FECHA_VENCIMIENTO?.toString();
            } else {
                //Consultamos la fecha efecto de la anualidad vigente
                fechaInicioInsertar = fechaAnualidadVigente?.FECHA_EFECTO?.toString();
            }
        }
        else {
            fechaInicioInsertar = dateEfecto;
        }

        fechaInicio = (tipoMovimiento == 'traspaso_entre_subclaves') ? datosPoliza?.FECHA_INICIO : fechaInicioInsertar;
        fechaFin = '2200-01-01';

        /*                  *
        *   DATOS INSERT    *
        *                   */
        
        def motivo = currentCase.getCustomFieldValue('txt_motivo_traspaso'); 
        def fechaEfecto = currentCase.getCustomFieldValue('date_efecto').toString(); 
        def fechaTraspaso = currentCase.getCustomFieldValue('pl_date_traspaso'); 
        def tipo_anualidad_traspaso_opesp = currentCase.getCustomFieldValue('txt_tipo_anualidad_traspaso_opesp'); 
        def cedente = datosMediador(codigoMediadorCedente + '-' + subclaveMediadorCedente); 
        def nombreCedente = ((cedente?.NOMBRE ?: '') + ' ' + (cedente?.APELLIDO ?: ''))?.trim(); 
        def datosReceptor = datosMediador(receptor.codigo_mediador + '-' + receptor.subclave_mediador); 
        def nombreReceptor = ((datosReceptor?.NOMBRE ?: '') + ' ' + (datosReceptor?.APELLIDO ?: ''))?.trim(); 
        def porcentaje = (tipoTraspaso == 'parcial') ? poliza?.'porcentaje_intermediacion': receptor?.'porcentaje_traspaso_total'; 
        def caseId = currentCase?.getId()?.toString(); 
        def numAnualidad = currentCase.getCustomFieldValue('pl_anualidad_opesp')?.substring(0, 2); 
        def valor = ''; 

        //SE PINTA UN REGISTRO POR CADA POLIZA
        def campos = ''; 
        def valores = ''; 

        if (scriptRuntimeContext.getActionDefnName() == 'Aprobar Solicitud') {
            valor = 'APROBADA';
        } else if (scriptRuntimeContext.getActionDefnName() == 'Rechazar Solicitud') {
            valor = 'RECHAZADA';
        } else {
            valor = 'PENDIENTE';
        }

        /*   QUERY INSERT    *
                    *                   */
        //Tipo traspaso (siempre P. Un traspaso total es N traspasos parciales, P).
        campos += 'TIPO_TRASPASO'; 
        valores += "'P'"; 
        //Número Pólia
        campos += ', NUM_POLIZA'; 
        valores += ', ' + poliza?.num_poliza; 
        //Código Aval
        campos += datosPoliza?.NUM_AVAL_HOST ? ', COD_AVAL ': ''; 
        valores += datosPoliza?.NUM_AVAL_HOST ? ", $datosPoliza.NUM_AVAL_HOST": ''; 
        //Asegurado
        campos += datosPoliza?.NOMBRE_TOMADOR ? ', ASEGURADO': ''; 
        valores += datosPoliza?.NOMBRE_TOMADOR ? ", '$datosPoliza.NOMBRE_TOMADOR'": ''; 
        //RAMO
        campos += datosPoliza?.RAMO ? ', RAMO': ''; 
        valores += datosPoliza?.RAMO ? ", '$datosPoliza.RAMO'": ''; 
        //Motivo Traspaso
        campos += motivo ? ', MOTIVO': ''; 
        valores += motivo ? ", '$motivo'": ''; 
        //Fecha de efecto
        campos += fechaEfecto ? ', FECHA_EFECTO_SOLICITUD': ''; 
        valores += fechaEfecto ? ", '$fechaEfecto'": ''; 
        //Fecha de inicio
        if (tipoMovimiento == 'operaciones_especiales') {
            campos += tiposFechaTraspaso[tipo_anualidad_traspaso_opesp] ? ', FECHA_INICIO_TRASPASO': ''; 
            valores += tiposFechaTraspaso[tipo_anualidad_traspaso_opesp] ? ", '" + tiposFechaTraspaso[tipo_anualidad_traspaso_opesp] + "'": '';
        } else {
            campos += tiposFechaTraspaso[fechaTraspaso] ? ', FECHA_INICIO_TRASPASO': ''; 
            valores += tiposFechaTraspaso[fechaTraspaso] ? ", '" + tiposFechaTraspaso[fechaTraspaso] + "'": '';
        }
        //Num anualidad
        campos += numAnualidad ? ', NUM_ANUALIDAD': ''; 
        valores += numAnualidad ? ", '$numAnualidad'": ''; 
        //Tipo de movimiento
        campos += tipoCambio[tipoMovimiento] ? ', TIPO_MOVIMIENTO': ''; 
        valores += tipoCambio[tipoMovimiento] ? ", '" + tipoCambio[tipoMovimiento] + "'": ''; 
        //Código mediador cedente
        campos += datosPoliza?.COD_MEDIADOR ? ', COD_MEDIADOR_CEDENTE': ''; 
        valores += datosPoliza?.COD_MEDIADOR ? ", '$datosPoliza.COD_MEDIADOR'": ''; 
        //Subclave mediador cedente
        campos += datosPoliza?.COD_SUBCLAVE ? ', SUBCLAVE_CEDENTE': ''; 
        valores += datosPoliza?.COD_SUBCLAVE ? ", '$datosPoliza.COD_SUBCLAVE'": ''; 
        //Dirección Territorial mediador cedente
        campos += cedente?.DT ? ', DIR_TERRITORIAL_CEDENTE': ''; 
        valores += cedente?.DT ? ", '$cedente.DT'": ''; 
        //Nombre del mediador cedente
        campos += nombreCedente ? ', NOMBRE_MEDIADOR_CEDENTE': ''; 
        valores += nombreCedente ? ", '$nombreCedente'": ''; 
        //Número identificación fiscal mediador cedente
        campos += cedente?.NIF ? ', IDENTIFICACION_FISCAL_CEDENTE': ''; 
        valores += cedente?.NIF ? ", '$cedente.NIF'": ''; 
        //Fecha de creación
        campos += ', FECHA_CREACION'; 
        valores += ', CURRENT_DATE'; 
        //Fecha de modificación
        campos += ', FECHA_MODIFICACION'; 
        valores += ', CURRENT_DATE'; 
        //Código mediador receptor
        campos += receptor?.codigo_mediador ? ', COD_MEDIADOR_RECEPTOR': ''; 
        valores += receptor?.codigo_mediador ? ", '$receptor.codigo_mediador'": ''; 
        //Subclave receptor
        campos += receptor?.subclave_mediador ? ', SUBCLAVE_RECEPTOR': ''; 
        valores += receptor?.subclave_mediador ? ", '$receptor.subclave_mediador'": ''; 
        //Dirección territorial del receptor
        campos += datosReceptor?.DT ? ', DIR_TERRITORIAL_RECEPTOR': ''; 
        valores += datosReceptor?.DT ? ", '$datosReceptor.DT'": ''; 
        //Nombre del receptor
        campos += nombreReceptor ? ', NOMBRE_MEDIADOR_RECEPTOR': ''; 
        valores += nombreReceptor ? ", '$nombreReceptor'": ''; 
        //Número identificación fiscal del receptor
        campos += datosReceptor?.NIF ? ', IDENTIFICACION_FISCAL_RECEPTOR': ''; 
        valores += datosReceptor?.NIF ? ", '$datosReceptor.NIF'": ''; 
        //Porcentaje intermediación mediador receptor
        if (tipoMovimiento == 'operaciones_especiales') {
            campos += datosPoliza?.P_INTERMEDIACION ? ', INTERMEDIACION_RECEPTOR': ''; 
            valores += datosPoliza?.P_INTERMEDIACION ? ", '$datosPoliza.P_INTERMEDIACION'": ''; 
        } else {
            campos += porcentaje ? ', INTERMEDIACION_RECEPTOR': ''; 
            valores += porcentaje ? ", $porcentaje": ''; 
        }
                                    
        //Porcentaje operaciones epeciales emsión
        def especial_emision = (tipoMovimiento == 'operaciones_especiales') ? poliza?.especial_emision: datosPoliza?.P_ESPECIAL_EMISION; 
        logger.info('especial_emision: ' + especial_emision)
        especial_emision = especial_emision?.toString()?.replace(',', '.'); 

        campos += especial_emision ? ', P_ESPECIAL_EMISION': ''; 
        valores += especial_emision ? ", $especial_emision": ''; 
                                    
        //Porcentaje operaciones epeciales renovación
        def especial_renovacion = (tipoMovimiento == 'operaciones_especiales') ? poliza?.especial_renovacion: datosPoliza?.P_ESPECIAL_RENOVACION; 
        especial_renovacion = especial_renovacion?.toString()?.replace(',', '.'); 
        logger.info('especial_renovacion: ' + especial_renovacion)
        campos += especial_renovacion ? ', P_ESPECIAL_RENOVACION': ''; 
        valores += especial_renovacion ? ", $especial_renovacion": ''; 
                                    
        //Estado de la solicitud
        campos += ', ESTADOREG'; 
        valores += ", '$valor'"; 
        //Id del caso de Workflow
        campos += caseId ? ', CASEID': ''; 
        valores += caseId ? ", '$caseId'": ''; 
        //Fecha Inicio
        campos += fechaInicio ? ', FECHA_INICIO': ''; 
        valores += fechaInicio ? ", '$fechaInicio'": ''; 
        //Fecha Fin
        campos += fechaFin ? ', FECHA_FIN': ''; 
        valores += fechaFin ? ", '$fechaFin'": ''; 
        //Usuario que ejecuta la acción
        campos += ', USUARIO'; 
        valores += ", '" + currentUser.getFirstName() + ' ' + currentUser.getLastName() + "'"; 
        //Email del usuario que ejecuta la acción
        campos += ', EMAIL'; 
        valores += ", '" + currentUser.getEmail() + "'"; 

        //PPM-14896
        //IDPRODUCT
        campos += datosPoliza?.IDPRODUCT ? ', IDPRODUCT': ''; 
        valores += datosPoliza?.IDPRODUCT ? ", '$datosPoliza.IDPRODUCT'": ''; 
        //FECHA_INICIO_OPESP
        campos += datosPoliza?.FECHA_INICIO_OPESP ? ', FECHA_INICIO_OPESP': ''; 
        valores += datosPoliza?.FECHA_INICIO_OPESP ? ", '$datosPoliza.FECHA_INICIO_OPESP'": ''; 
        //FECHA_FIN_OPESP
        campos += datosPoliza?.FECHA_FIN_OPESP ? ', FECHA_FIN_OPESP': ''; 
        valores += datosPoliza?.FECHA_FIN_OPESP ? ", '$datosPoliza.FECHA_FIN_OPESP'": ''; 

        logger.info('preinsert')
        def queryInsert = "INSERT INTO EXT.SOLICITUD_TRASPASO (" + campos.toString() + ") VALUES (" + valores.toString() + ")"; 
        logger.info('queryInsert: ' + queryInsert)
        db.execute(queryInsert);

        campos = ''; 
        valores = ''; 

        //SE PINTA UNA LINEA INFORMATIVA POR RECEPTOR SI ES TRASPASO TOTAL
        if(registroInfoTotal == 'total'){
            logger.info('ENTRA LINEA TRASPASO TOTAL')
            //valor = 'PENDIENTE';

            /*   QUERY INSERT    *
            *                   */
            //Tipo traspaso (siempre P. Un traspaso total es N traspasos parciales, P).
            campos += 'TIPO_TRASPASO'; 
            valores += "'T'"; 
            //Asegurado
            campos += datosPoliza?.NOMBRE_TOMADOR ? ', ASEGURADO': ''; 
            valores += datosPoliza?.NOMBRE_TOMADOR ? ", '$datosPoliza.NOMBRE_TOMADOR'": ''; 
            //Fecha de efecto
            campos += fechaEfecto ? ', FECHA_EFECTO_SOLICITUD': ''; 
            valores += fechaEfecto ? ", '$fechaEfecto'": ''; 
            logger.info('1')
            //Fecha de inicio
            campos += tiposFechaTraspaso[fechaTraspaso] ? ', FECHA_INICIO_TRASPASO': ''; 
            valores += tiposFechaTraspaso[fechaTraspaso] ? ", '" + tiposFechaTraspaso[fechaTraspaso] + "'": '';
            logger.info('2')
            //Tipo de movimiento
            campos += tipoCambio[tipoMovimiento] ? ', TIPO_MOVIMIENTO': ''; 
            valores += tipoCambio[tipoMovimiento] ? ", '" + tipoCambio[tipoMovimiento] + "'": ''; 
            logger.info('3')
            //Código mediador cedente
            campos += datosPoliza?.COD_MEDIADOR ? ', COD_MEDIADOR_CEDENTE': ''; 
            valores += datosPoliza?.COD_MEDIADOR ? ", '$datosPoliza.COD_MEDIADOR'": ''; 
            //Subclave mediador cedente
            campos += datosPoliza?.COD_SUBCLAVE ? ', SUBCLAVE_CEDENTE': ''; 
            valores += datosPoliza?.COD_SUBCLAVE ? ", '$datosPoliza.COD_SUBCLAVE'": ''; 
            //Dirección Territorial mediador cedente
            campos += cedente?.DT ? ', DIR_TERRITORIAL_CEDENTE': ''; 
            valores += cedente?.DT ? ", '$cedente.DT'": ''; 
            //Nombre del mediador cedente
            logger.info('4')
            campos += nombreCedente ? ', NOMBRE_MEDIADOR_CEDENTE': ''; 
            valores += nombreCedente ? ", '$nombreCedente'": ''; 
            //Número identificación fiscal mediador cedente
            campos += cedente?.NIF ? ', IDENTIFICACION_FISCAL_CEDENTE': ''; 
            valores += cedente?.NIF ? ", '$cedente.NIF'": ''; 
            //Fecha de creación
            campos += ', FECHA_CREACION'; 
            valores += ', CURRENT_DATE'; 
            //Fecha de modificación
            campos += ', FECHA_MODIFICACION'; 
            valores += ', CURRENT_DATE'; 
            logger.info('5')
            //Código mediador receptor
            campos += receptor?.codigo_mediador ? ', COD_MEDIADOR_RECEPTOR': ''; 
            valores += receptor?.codigo_mediador ? ", '$receptor.codigo_mediador'": ''; 
            //Subclave receptor
            campos += receptor?.subclave_mediador ? ', SUBCLAVE_RECEPTOR': ''; 
            valores += receptor?.subclave_mediador ? ", '$receptor.subclave_mediador'": ''; 
            //Dirección territorial del receptor
            campos += datosReceptor?.DT ? ', DIR_TERRITORIAL_RECEPTOR': ''; 
            valores += datosReceptor?.DT ? ", '$datosReceptor.DT'": ''; 
            //Nombre del receptor
            campos += nombreReceptor ? ', NOMBRE_MEDIADOR_RECEPTOR': ''; 
            valores += nombreReceptor ? ", '$nombreReceptor'": ''; 
            //Número identificación fiscal del receptor
            campos += datosReceptor?.NIF ? ', IDENTIFICACION_FISCAL_RECEPTOR': ''; 
            valores += datosReceptor?.NIF ? ", '$datosReceptor.NIF'": ''; 
            logger.info('6')
            //Porcentaje intermediación mediador receptor
            campos += porcentaje ? ', INTERMEDIACION_RECEPTOR': ''; 
            valores += porcentaje ? ", $porcentaje": '';                   
            //Estado de la solicitud
            campos += ', ESTADOREG'; 
            valores += ", '$valor'"; 
            //Id del caso de Workflow
            campos += caseId ? ', CASEID': ''; 
            valores += caseId ? ", '$caseId'": ''; 
            logger.info('7')
            //Fecha Inicio
            campos += fechaInicio ? ', FECHA_INICIO': ''; 
            valores += fechaInicio ? ", '$fechaInicio'": ''; 
            //Fecha Fin
            campos += fechaFin ? ', FECHA_FIN': ''; 
            valores += fechaFin ? ", '$fechaFin'": ''; 
            logger.info('8')
            //Usuario que ejecuta la acción
            campos += ', USUARIO'; 
            valores += ", '" + currentUser.getFirstName() + ' ' + currentUser.getLastName() + "'"; 
            //Email del usuario que ejecuta la acción
            campos += ', EMAIL'; 
            valores += ", '" + currentUser.getEmail() + "'"; 
            //RAMO
            campos += ', RAMO';
            if(ramo == 'ambos'){
                valores += ', ' + null;
            }
            else{
                valores += datosPoliza?.RAMO ? ", '$datosPoliza.RAMO'": ''; 
                

                
            logger.info('RAMO::: ' + datosPoliza?.RAMO?.toUpperCase())
            }
            
            logger.info('TERMINA LINEA TRASPASO TOTAL')

            def queryInsertTotal = "INSERT INTO EXT.SOLICITUD_TRASPASO (" + campos.toString() + ") VALUES (" + valores.toString() + ")"; 
            logger.info('queryInsertTotal: ' + queryInsertTotal)
            db.execute(queryInsertTotal);
        }
    }
}

//////////////////////////////////////////////////////////////////////

def datosMediador(def codigoMediador){
  
    def db = resp.dbConnect('datasource.CESCEdb');

    def queryMediador = """SELECT par.FIRSTNAME as nombre, par.LASTNAME as apellido, 
						   par.GENERICATTRIBUTE7 as DT, par.GENERICATTRIBUTE2 as NIF
                           FROM TCMP.CS_PARTICIPANT par
                           LEFT JOIN TCMP.CS_POSITION pos
                           ON par.PAYEESEQ = pos.PAYEESEQ
                           AND pos.REMOVEDATE = TO_DATE('22000101', 'yyyymmdd')
                           AND pos.ISLAST = 1
                           WHERE par.REMOVEDATE = TO_DATE('22000101', 'yyyymmdd')
                           AND par.ISLAST = 1 
                           AND pos.NAME = '"""+ codigoMediador +"""'""";
  
    return db.queryForList(queryMediador)?.getAt(0)?:null;
}

//////////////////////////////////////////////////////////////////////

def datosPolizas(def num_poliza, def aval, def codMediador, def subclaveMediador){

    def db = resp.dbConnect('datasource.CESCEdb');

    def dateEfecto = currentCase.getCustomFieldValue('date_efecto').toString();

    def queryPoliza = "SELECT * FROM EXT.CARTERA";
    queryPoliza += " WHERE NUM_POLIZA = '" + num_poliza + "' ";
    queryPoliza +=  (aval && aval != '') ? " AND NUM_AVAL_HOST = '" + aval + "' " : "AND NUM_AVAL_HOST IS NULL";
    queryPoliza += " AND (FECHA_FIN >= '" + dateEfecto + "'";
    queryPoliza += " OR FECHA_FIN IS NULL)";
    queryPoliza += " AND (FECHA_INICIO <= '" + dateEfecto +"'";
    queryPoliza += " OR FECHA_INICIO IS NULL)";
    //Si es expediente no debe filtrar por la fecha vencimiento
    //if(currentCase.getCustomFieldValue('pl_traspasos_caucion') != 'expediente')
        //queryPoliza += " AND FECHA_VENCIMIENTO > '" + dateEfecto +"'";
    queryPoliza += " AND COD_MEDIADOR = '$codMediador'";
  	queryPoliza += " AND COD_SUBCLAVE = '$subclaveMediador'";
    logger.info('queryPoliza: ' + queryPoliza)
    return db.queryForList(queryPoliza)?.getAt(0);

}

if(estado == 'creado'){
  // FIJAR VALOR PARA CASE NAME

  currentCase.setName(tipoMovimiento.toUpperCase() + ' - ' + tipoTraspaso.toUpperCase());
  currentCase.setProject(resp.getProject('gm'));
  resp.cases.update(currentCase);
  logger.info('Fin: sc_insertar_datos_traspasos');
}