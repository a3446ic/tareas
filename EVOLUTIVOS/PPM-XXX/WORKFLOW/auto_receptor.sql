use datasource.CESCEdb;
SELECT COD_MEDIADOR || '-' || SUBCLAVE || ' ' || NUM_IDENTIFICACION || ' ' || IFNULL(NOMBRE, '') || ' ' || "APELLIDO/RAZON_SOCIAL" as mediador
FROM EXT.MODIFICAR_MEDIADOR 
WHERE COD_MEDIADOR IS NOT NULL 
AND (
    ('$!{form.getValue('pl_tipo_movimiento')}' = 'traspaso_entre_subclaves' AND '$!{form.getValue('auto_mediador')}' IS NOT NULL AND COD_MEDIADOR = SUBSTRING('$!{form.getValue('auto_mediador')}', 0, 4) AND SUBCLAVE <> SUBSTRING('$!{form.getValue('auto_mediador')}', 6, 4))
    OR 
    ('$!{form.getValue('pl_tipo_movimiento')}' <> 'traspaso_entre_subclaves' OR '$!{form.getValue('auto_mediador')}' IS NULL)
)
AND LOWER(COD_MEDIADOR || '-' || SUBCLAVE || ' ' || NUM_IDENTIFICACION || ' ' || IFNULL(NOMBRE, '') || ' ' || "APELLIDO/RAZON_SOCIAL") LIKE LOWER('%$!{searchPhrase}%')
AND DIR_TERRITORIAL LIKE CASE WHEN '$!{currentUser.getDepartment()}' = 'central_cesce' THEN '%' ELSE '$!{currentUser.getDepartment().toString().toUpperCase()}' END



use datasource.CESCEdb;
SELECT * FROM EXT.GET_AUTO_MEDIADOR_RECEPTOR_TRASPASOS('${searchPhrase}','$!{currentUser.getDepartment()}','$!{currentUser.getDepartment().getName().toUpperCase()}','$!{form.getValue('pl_tipo_movimiento')}','$!{form.getValue('auto_mediador')}');