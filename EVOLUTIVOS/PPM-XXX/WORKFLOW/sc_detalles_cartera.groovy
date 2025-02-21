def tabla = form.getTableController('tbl_cartera')

def selected = tabla.getSelectedObjects().collect { it.getObject() }

if(selected) {
out << selected
  def txtDescripcion = form.getField('txt_detalles_cartera')

  def ramo = selected['RAMO'][0]
  def idproduct = selected['IDPRODUCT'][0]
  def numPoliza = selected['NUM_POLIZA'][0]
  def numFianza = selected['NUM_FIANZA'][0]
  def numExpediente = selected['NUM_EXPEDIENTE'][0]
  def numAvalHost = selected['NUM_AVAL_HOST'][0]
  def numAnualidad = selected['NUM_ANUALIDAD'][0]
  def fechaEmision = selected['FECHA_EMISION'][0]
  def fechaEfecto = selected['FECHA_EFECTO'][0]
  def fechaVencimiento = selected['FECHA_VENCIMIENTO'][0]
  def idPais = selected['IDPAIS'][0]
  def primaMinInt = selected['PRIMA_MIN_INT'][0]
  def primaMinExt = selected['PRIMA_MIN_EXT'][0]
  def nifTomador = selected['NIF_TOMADOR'][0]
  def nombreTomador = selected['NOMBRE_TOMADOR'][0]
  def fechaEfectoTraspaso = selected['FECHA_EFECTO_TRASPASO'][0]
  def porcentajeEspecialEmision = selected['P_ESPECIAL_EMISION'][0];
  def porcentajeEspecialRenovacion = selected['P_ESPECIAL_RENOVACION'][0];
  def fechaInicioOperacionesEspeciales = selected['FECHA_INICIO_OPESP'][0];
  def fechaFinOperacionesEspeciales = selected['FECHA_FIN_OPESP'][0];
  
  
  def descripcion = """Ramo: $ramo
  Id Producto: $idproduct
  Número Poliza: $numPoliza
  Número Fianza: $numFianza
  Número expediente: $numExpediente
  Número Aval Host: $numAvalHost
  Número Anualidad: $numAnualidad
  Fecha Emisión: $fechaEmision
  Fecha Efecto: $fechaEfecto
  Fecha Vencimiento: $fechaVencimiento
  Id Pais: $idPais
  Prima Min Int: $primaMinInt
  Prima Min Ext: $primaMinExt
  Nif Tomador: $nifTomador
  Nombre Tomador: $nombreTomador
  Fecha Efecto Traspaso: $fechaEfectoTraspaso
  Porcentaje Especial de Emisión: $porcentajeEspecialEmision
  Porcentaje Especial de Renovación: $porcentajeEspecialRenovacion
  Fecha Inicio Operaciones Especiales: $fechaInicioOperacionesEspeciales
  Fecha Fin Operaciones Especiales: $fechaFinOperacionesEspeciales
  """

  txtDescripcion.setValue(descripcion)
} else {
  resp.alert.info("Mostrar Detalles: Debe seleccionar un elemento de la lista.")
}