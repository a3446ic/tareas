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
// Mapa de receptores
// def mapaFormulario = currentCase.getCustomFieldValue('txt_mapa_receptores');
// def mapa = mapaFormulario ? evaluate(mapaFormulario) : [:]  // Evita null
// def mapaReceptores = mapa['receptores'] ?: []  // Si es null, usa lista vacía
// logger.info('mapaReceptores: ' + mapaReceptores);

// Extraer código de mediador y sus pólizas
// def receptores = mapaReceptores.collect { mediador -> 
//     [
//         "codigoMediadorReceptor": mediador.codigo_mediador,
//         "subClaveMediadorReceptor": mediador.subclave_mediador,
//         "porcentajeTraspaso": mediador.porcentaje_traspaso_total,
//         polizasReceptor: mediador.polizas.collect { poliza ->
//             ["num_poliza": poliza.num_poliza, "num_aval": poliza.codigo_aval, "porcentaje_intermediacion": poliza.porcentaje_intermediacion]
//         }
//     ]
// }

// Convertimos a JSON para visualizarlo mejor
// def jsonReceptores = JsonOutput.toJson(receptores)

// Imprimir el JSON formateado
//logger.info('jsonReceptores ' + JsonOutput.prettyPrint(jsonReceptores));
// Mapa de polizas
// def mapaPolizas = mapa['polizas'] ?: []  // Si es null, usa lista vacía
// logger.info("mapaPolizas " + mapaPolizas);
// def polizas = mapaPolizas.collect { poliza -> 
//     [
//         "num_poliza": "$poliza.num_poliza",
//         "num_aval": "$poliza.codigo_aval",
//         "porcentaje_intermediacion": "$poliza.participación"
//     ]
// }
//def numPolizas = polizas.collect { it.num_poliza }
// def jsonPolizas = JsonOutput.toJson(polizas);
// Mapa de avales
//def numAval = polizas.collect { it.codigo_aval }
//def jsonAval = JsonOutput.toJson(numAval);

//JSON
// def json;
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
// logger.info('receptores: ' + receptores);
// logger.info('receptores size ' + receptores.size());
// logger.info('polizas: ' + polizas);
// logger.info('polizas size ' + polizas.size());
// logger.info('jsonPolizas: ' + jsonPolizas);
// //logger.info('jsonAval: ' + jsonAval);
// logger.info('jsonReceptores: ' + jsonReceptores);



//  if (!receptores.isEmpty()) {
//      if(receptores.size() == 1){
//         codigoMediadorReceptor = receptores[0].codigo_mediador;
//         subClaveMediadorReceptor = receptores[0].subclave_mediador;
//         //logger.info('Hay más de un receptor en la lista');
//      }

//  }else{
//      logger.info('No hay receptores en la lista');
//  }

//EJECUTAR QUERY INSERTAR DATOS EN LA TABLA DE TRASPASOS	
def executeQuery(query) {
    def db = resp.dbConnect('datasource.CESCEdb');
    logger.info('queryTraspaso ' + query);
    db.execute(query);
}

//MONTAR JSON
// def createJson(codigoMediadorCedente, subclaveMediadorCedente, jsonPolizas, jsonReceptores, dateEfecto, idCasoOrigen, tipoTraspaso, tipoTraspasoCaucion, tipoMovimiento) {
    
//     return """{
//             "codigoMediadorCedente":"$codigoMediadorCedente",
//             "subClaveMediadorCedente":"$subclaveMediadorCedente",
//             "tipoTraspaso":"$tipoTraspaso",
//             "tipoTraspasoCaucion":"$tipoTraspasoCaucion",
//             "polizas":$jsonPolizas,
//             "receptor":$jsonReceptores,
//             "fechaTraspaso":"$dateEfecto",
//             "tipoMovimiento":"$tipoMovimiento",
//             "caseId":"$idCasoOrigen" 
//     }""";

    
// }

// def createJsonTraspasoPorcentaje(codigoMediadorCedente, subclaveMediadorCedente, dateEfecto, jsonReceptores, idCasoOrigen) {
//     return """{
//         "codigoMediadorCedente":"$codigoMediadorCedente",
//         "subClaveMediadorCedente":"$subclaveMediadorCedente",
//         "fechaTraspaso":"$dateEfecto",
//         "receptores":$jsonReceptores,
//         "caseId":"$idCasoOrigen"
        
//     }""";
// }
//TRASPASO CREDITO
def traspasoCredito(derechosObligaciones, idCasoOrigen) {
    switch(derechosObligaciones) {
        case 'traspaso_con_derechos_y_obligaciones':
            logger.info('CON DERECHOS Y OBLIGACIONES - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_credito($idCasoOrigen)""");
            break;
        case 'traspaso_sin_derechos_y_obligaciones':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito('$idCasoOrigen')""");
            break;
        case 'sin_derechos_y_obligaciones_al_inicio_de_la_anuali':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA ANUALIDAD ACTUAL - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito('$idCasoOrigen')""");
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
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_credito('$idCasoOrigen')""");
            logger.info('CON DERECHOS Y OBLIGACIONES - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_caucion('$idCasoOrigen')""");
            break;
        case 'traspaso_sin_derechos_y_obligaciones':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito('$idCasoOrigen')""");
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_caucion('$idCasoOrigen')""");
            break;
        case 'sin_derechos_y_obligaciones_al_inicio_de_la_anuali':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA ANUALIDAD ACTUAL - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito('$idCasoOrigen')""");
            break;
    }
}

//TRASPASO PORCENTAJE CREDITO
def traspasoPorcentajeCredito(derechosObligaciones, idCasoOrigen) {
    switch(derechosObligaciones) {
        case 'traspaso_con_derechos_y_obligaciones':
            logger.info('CON DERECHOS Y OBLIGACIONES - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_porcentaje_mediador_con_derechos_credito('$idCasoOrigen')""");
            break;
        case 'traspaso_sin_derechos_y_obligaciones':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito('$idCasoOrigen')""");
            break;
        case 'sin_derechos_y_obligaciones_al_inicio_de_la_anuali':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA ANUALIDAD ACTUAL - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito('$idCasoOrigen')""");
            break;
    }
}

//TRASPASO PORCENTAJE CAUCION
def traspasoPorcentajeCaucion(derechosObligaciones, idCasoOrigen) {
    switch(derechosObligaciones) {
        case 'traspaso_con_derechos_y_obligaciones':
            logger.info('CON DERECHOS Y OBLIGACIONES - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_caucion('$idCasoOrigen')""");
            break;
        case 'traspaso_sin_derechos_y_obligaciones':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_caucion('$idCasoOrigen')""");
            break;
    }
}

//TRASPASO PORCENTAJE AMBOS
def traspasoPorcentajeAmbos(derechosObligaciones, idCasoOrigen) {
    switch(derechosObligaciones) {
        case 'traspaso_con_derechos_y_obligaciones':
            logger.info('CON DERECHOS Y OBLIGACIONES - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_porcentaje_mediador_con_derechos_credito('$idCasoOrigen')""");
            logger.info('CON DERECHOS Y OBLIGACIONES - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_con_derechos_caucion('$idCasoOrigen')""");
            break;
        case 'sin_derechos_y_obligaciones_a_la_renovacin':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_credito('$idCasoOrigen')""");
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA RENOVACIÓN - CAUCION');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_renovacion_caucion('$idCasoOrigen')""");
            break;
        case 'sin_derechos_y_obligaciones_al_inicio_de_la_anuali':
            logger.info('SIN DERECHOS Y OBLIGACIONES A LA ANUALIDAD ACTUAL - CREDITO');
            executeQuery("""CALL EXT.sp_traspaso_mediador_mediador_sin_derechos_anualidad_actual_credito('$idCasoOrigen')""");
            break;
    }
}


//INICIO

switch(tipoMovimiento) {
    case 'sin_mediador_mediador':
        logger.info('SIN-MEDIADOR');
        codigoMediadorCedente = '0000';
        subclaveMediadorCedente = '0000';
        // json = createJson(codigoMediadorCedente, subclaveMediadorCedente, jsonPolizas, jsonReceptores, dateEfecto, idCasoOrigen, tipoTraspaso, tipoTraspasoCaucion, tipoMovimiento);
        // logger.info('json: '+ json);
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
        // json = createJson(codigoMediadorCedente, subclaveMediadorCedente, jsonPolizas, jsonReceptores, dateEfecto, idCasoOrigen, tipoTraspaso, tipoTraspasoCaucion, tipoMovimiento);
        // logger.info('json: '+ json);
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
        //json = createJson(codigoMediadorCedente, subclaveMediadorCedente, dateEfecto, codigoMediadorReceptor, subClaveMediadorReceptor, tipoTraspasoCaucion, jsonPolizas, jsonAval, idCasoOrigen);
        //  json = createJsonTraspasoPorcentaje(codigoMediadorCedente, subclaveMediadorCedente, dateEfecto, jsonReceptores, idCasoOrigen);
        //  logger.info('json: '+ json);
        // switch(ramo) {
        //     case 'credito':
        //         traspasoPorcentajeCredito(derechosObligaciones, json);
        //         break;
        //     case 'caucion':
        //         traspasoPorcentajeCaucion(derechosObligaciones, json);
        //         break;
        //     case 'ambos':
        //         traspasoPorcentajeAmbos(derechosObligaciones, json);
        //         break;
        // }
        break;

    case 'mediador_canal_directo':
        logger.info('MEDIADOR > CANAL-DIRECTO');
        codigoMediadorReceptor = '0000';
        subClaveMediadorReceptor = '0000';
        // json = createJson(codigoMediadorCedente, subclaveMediadorCedente, jsonPolizas, jsonReceptores, dateEfecto, idCasoOrigen, tipoTraspaso, tipoTraspasoCaucion, tipoMovimiento);
        // logger.info('json: '+ json);
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