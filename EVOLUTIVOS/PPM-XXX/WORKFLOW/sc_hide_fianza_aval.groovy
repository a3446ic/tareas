def ramoList = form.getField('mult_pl_ramo').getValue() // Obtiene la lista de valores
logger.info('Ramo: ' + ramoList);

if (ramoList) {
    def fianza = form.getField('txt_fianza')
    def aval = form.getField('txt_aval')
    def txtDescripcion = form.getField('txt_detalles_cartera')
    def tabla = form.getTableController('tbl_cartera')
  	
  txtDescripcion.setValue('')
  
  tabla.setSource([])
    

    // Convertir todos los valores de la lista a mayúsculas
    def ramoUpperList = ramoList*.toUpperCase()

    // Verificar si la lista contiene "CAUCION"
    if (ramoUpperList.contains("CAUCION")) {
        fianza.enable()
      	aval.enable()        
        logger.info('Campo habilitado porque ramo contiene CAUCION')        

    } else {
        fianza.disable()
        fianza.setValue('')
      	aval.disable()
        aval.setValue('')
       
        
        logger.info('Campo deshabilitado porque ramo no contiene CAUCION')
    }
  form.refreshElement('tbl_cartera')
}