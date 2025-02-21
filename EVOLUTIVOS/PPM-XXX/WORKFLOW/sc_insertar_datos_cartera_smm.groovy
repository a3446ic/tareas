// Script test data: 

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import groovy.json.JsonOutput

logger.info('sc_insertar_datos_cartera_smm: Comienza script');

// def db = resp.dbConnect('datasource.CESCEdb');
def tipoMovimiento = currentCase.getCustomFieldValue('pl_tipo_movimiento');
def tipoTraspaso = currentCase.getCustomFieldValue('pl_tipo_traspaso');
def derechosObligaciones = currentCase.getCustomFieldValue('pl_date_traspaso');
def ramo = currentCase.getCustomFieldValue('pl_ramo');
def dateEfecto = currentCase.getCustomFieldValue('date_efecto').toString();
def tipoTraspasoCaucion = currentCase.getCustomFieldValue('pl_traspasos_caucion');


// Mediador cedente
def autoCedente;
autoCedente = currentCase.getCustomFieldValue('auto_mediador');

//def codigoCedenteCompleto = autoCedente ? autoCedente.split(' ')?.getAt(0) : '0000-0000';
def codigoCedenteCompleto = autoCedente != 'Sin Mediador' ? autoCedente : '0000-0000';

def codigoMediadorCedente = codigoCedenteCompleto?.split('-')?.getAt(0);
def subclaveMediadorCedente = codigoCedenteCompleto?.split('-')?.getAt(1);

// Mediador receptor
def codigoMediadorReceptor;
def subClaveMediadorReceptor;

//IDCASO
def idCasoOrigen = currentCase.getId();

def queryTraspaso;

logger.info('tipoMovimiento: ' + tipoMovimiento);
logger.info('tipoTraspaso: ' + tipoTraspaso);
logger.info('derechosObligaciones: ' + derechosObligaciones);
logger.info('ramo: ' + ramo);
logger.info('dateEfecto: ' + dateEfecto);
logger.info('Código Mediador Cedente: ' + codigoCedenteCompleto);
logger.info('codigoMediadorCedente: ' + codigoMediadorCedente);
logger.info('subclaveMediadorCedente: ' + subclaveMediadorCedente);
logger.info('tipoTraspasoCaucion: ' + tipoTraspasoCaucion);

//EJECUTAR QUERY INSERTAR DATOS EN LA TABLA DE TRASPASOS	
def executeQuery(query) {
    def db = resp.dbConnect('datasource.CESCEdb');
    logger.info('queryTraspaso ' + query);
    db.execute(query);
}


 /***********************************************************************************/
/*************************** MEDIADOR > MEDIADOR ************************************/
/*************************** SIN MEDIADOR > MEDIADOR ********************************/
/*************************** MEDIADOR > CANAL DIRECTO *******************************/
/************************************************************************************/

//TRASPASO CREDITO
def traspasoCredito(derechosObligaciones, idCasoOrigen) {
    switch(derechosObligaciones) {
        case 'traspaso_con_derechos_y_obligaciones':
            logger.info('CON DERECHOS Y OBLIGACIONES - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_credito($idCasoOrigen)""");
            break;
        case 'traspaso_sin_derechos_y_obligaciones':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito($idCasoOrigen)""");
            break;
        case 'sin_derechos_y_obligaciones_al_inicio_de_la_anuali':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA ANUALIDAD ACTUAL - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito($idCasoOrigen)""");
            break;
    }
}

//TRASPASO CAUCION
def traspasoCaucion(derechosObligaciones, idCasoOrigen) {
    switch(derechosObligaciones) {
        case 'traspaso_con_derechos_y_obligaciones':
            logger.info('CON DERECHOS Y OBLIGACIONES - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_caucion($idCasoOrigen)""");
            break;
        case 'sin_derechos_y_obligaciones_a_la_renovacin':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_caucion($idCasoOrigen)""");
            break;
    }
}

//TRASPASO AMBOS
def traspasoAmbos(derechosObligaciones, idCasoOrigen) {
    switch(derechosObligaciones) {
        case 'traspaso_con_derechos_y_obligaciones':
            logger.info('CON DERECHOS Y OBLIGACIONES - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_credito($idCasoOrigen)""");
            logger.info('CON DERECHOS Y OBLIGACIONES - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_caucion($idCasoOrigen)""");
            break;
        case 'traspaso_sin_derechos_y_obligaciones':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito($idCasoOrigen)""");
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_caucion($idCasoOrigen)""");
            break;
        case 'sin_derechos_y_obligaciones_al_inicio_de_la_anuali':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA ANUALIDAD ACTUAL - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito($idCasoOrigen)""");
            break;
    }
}

/********************************************************************************/
/*************************** TRASPASO PORCENTAJES  ******************************/
/********************************************************************************/

//TRASPASO PORCENTAJE CREDITO
def traspasoPorcentajeCredito(derechosObligaciones, idCasoOrigen) {
    switch(derechosObligaciones) {
        case 'traspaso_con_derechos_y_obligaciones':
            logger.info('CON DERECHOS Y OBLIGACIONES - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_porcentaje_mediador_con_derechos_credito($idCasoOrigen)""");
            break;
        case 'traspaso_sin_derechos_y_obligaciones':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito($idCasoOrigen)""");
            break;
        case 'sin_derechos_y_obligaciones_al_inicio_de_la_anuali':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA ANUALIDAD ACTUAL - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito($idCasoOrigen)""");
            break;
    }
}

//TRASPASO PORCENTAJE CAUCION
def traspasoPorcentajeCaucion(derechosObligaciones, idCasoOrigen) {
    switch(derechosObligaciones) {
        case 'traspaso_con_derechos_y_obligaciones':
            logger.info('CON DERECHOS Y OBLIGACIONES - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_caucion($idCasoOrigen)""");
            break;
        case 'traspaso_sin_derechos_y_obligaciones':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_caucion($idCasoOrigen)""");
            break;
    }
}

//TRASPASO PORCENTAJE AMBOS
def traspasoPorcentajeAmbos(derechosObligaciones, idCasoOrigen) {
    switch(derechosObligaciones) {
        case 'traspaso_con_derechos_y_obligaciones':
            logger.info('CON DERECHOS Y OBLIGACIONES - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_porcentaje_mediador_con_derechos_credito($idCasoOrigen)""");
            logger.info('CON DERECHOS Y OBLIGACIONES - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_caucion($idCasoOrigen)""");
            break;
        case 'sin_derechos_y_obligaciones_a_la_renovacin':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito($idCasoOrigen)""");
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_caucion($idCasoOrigen)""");
            break;
        case 'sin_derechos_y_obligaciones_al_inicio_de_la_anuali':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA ANUALIDAD ACTUAL - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito($idCasoOrigen)""");
            break;
    }
}
/********************************************************************************/
/*************************** OPERACIONES ESPECIALES *****************************/
/********************************************************************************/

//TRASPASO OPERACIONES ESPECIALES CREDITO
def traspasoOperacionesEspecialesCredito(idCasoOrigen) {
    
    logger.info('OPERACIONES ESPECIALES - CREDITO');
    executeQuery("""CALL EXT.sp_traspaso_operaciones_especiales_credito($idCasoOrigen)""");
            
}

//TRASPASO PORCENTAJE CAUCION
def traspasoOperacionesEspecialesCaucion(idCasoOrigen) {
    
    logger.info('OPERACIONES ESPECIALES - CAUCION');
    executeQuery("""CALL EXT.sp_traspaso_operaciones_especiales_caucion($idCasoOrigen)""");
}

//TRASPASO PORCENTAJE AMBOS
def traspasoOperacionesEspecialesAmbos(idCasoOrigen) {
    
    logger.info('OPERACIONES ESPECIALES - CREDITO');
    executeQuery("""CALL EXT.sp_traspaso_operaciones_especiales_credito($idCasoOrigen)""");

    logger.info('OPERACIONES ESPECIALES - CAUCION');
    executeQuery("""CALL EXT.sp_traspaso_operaciones_especiales_caucion($idCasoOrigen)""");    
    
}


/********************************************************************************/
/*************************** INICIO SCRIPT **************************************/
/********************************************************************************/


switch(tipoMovimiento) {
    case 'sin_mediador_mediador':
        logger.info('SIN-MEDIADOR');
        codigoMediadorCedente = '0000';
        subclaveMediadorCedente = '0000';
        switch(ramo) {
            case 'credito':
                traspasoCredito(derechosObligaciones, idCasoOrigen);
                break;
            case 'caucion':
                traspasoCaucion(derechosObligaciones, idCasoOrigen);
                break;
            case 'ambos':
                traspasoAmbos(derechosObligaciones, idCasoOrigen);
                break;
        }
        break;

    case 'mediador_mediador':
        logger.info('MEDIADOR-MEDIADOR');   
        switch(ramo) {
            case 'credito':
                traspasoCredito(derechosObligaciones, idCasoOrigen);
                break;
            case 'caucion':
                traspasoCaucion(derechosObligaciones, idCasoOrigen);
                break;
            case 'ambos':
                traspasoAmbos(derechosObligaciones, idCasoOrigen);
                break;
        }
        break;

    case 'traspaso_otro_mediador':
        logger.info('TRASPASO % A OTRO MEDIADOR');
        switch(ramo) {
            case 'credito':
                traspasoPorcentajeCredito(derechosObligaciones, json);
                break;
            case 'caucion':
                traspasoPorcentajeCaucion(derechosObligaciones, json);
                break;
            case 'ambos':
                traspasoPorcentajeAmbos(derechosObligaciones, json);
                break;
        }
        break;

    case 'mediador_canal_directo':
        logger.info('MEDIADOR > CANAL-DIRECTO');
        codigoMediadorReceptor = '0000';
        subClaveMediadorReceptor = '0000';
        switch(ramo) {
            case 'credito':
                traspasoCredito(derechosObligaciones, idCasoOrigen);
                break;
            case 'caucion':
                traspasoCaucion(derechosObligaciones, idCasoOrigen);
                break;
            case 'ambos':
                traspasoAmbos(derechosObligaciones, idCasoOrigen);
                break;
        }
        break;

    case 'operaciones_especiales':
        logger.info('OPERACIONES ESPECIALES');   
        
        switch(ramo) {
            case 'credito':
                traspasoOperacionesEspecialesCredito(idCasoOrigen);
                break;
            case 'caucion':
                traspasoOperacionesEspecialesCaucion(idCasoOrigen);
                break;
            case 'ambos':
                traspasoOperacionesEspecialesAmbos(idCasoOrigen);
                break;
        }
        break;

    case 'error_captura':
        logger.info('ERROR-CAPTURA');   
        
        switch(ramo) {
            case 'credito':
                traspasoCredito(derechosObligaciones, idCasoOrigen);
                break;
            case 'caucion':
                traspasoCaucion(derechosObligaciones, idCasoOrigen);
                break;
            case 'ambos':
                traspasoAmbos(derechosObligaciones, idCasoOrigen);
                break;
        }
        break;
}