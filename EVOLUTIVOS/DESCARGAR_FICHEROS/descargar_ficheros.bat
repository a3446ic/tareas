@echo off
chcp 65001
setlocal enabledelayedexpansion

::-----------------------------------------------------------------
:: Tiene que estar arrancado el pageant y añadida la clave
:: Pass: CescePr@d
::-----------------------------------------------------------------

:: Configuración de la ruta de WinSCP y detalles de conexión
set WINSCP_PATH="C:\Program Files (x86)\WinSCP\WinSCP.com"
set SFTP_SERVER=sftp://2111:CescePr@d@xfer-prd-fra.calliduscloud.com/
set HOSTKEY="ssh-rsa 1024 MKA9n3CYF8dY+j9P713bUoWelyJtFdv8gNpfn8pkzoc"

:: Obtener la fecha actual en formato yyyymmdd usando WMIC
for /f %%i in ('wmic os get localdatetime ^| find "."') do set datetime=%%i
set Year=%datetime:~0,4%
set Month=%datetime:~4,2%
set Day=%datetime:~6,2%

:: Formatear la fecha como ddmmyyyy para la carpeta
set FolderName=%Month%%Year%
set LogName=%Day%%Month%%Year%

:: Definir la carpeta de destino con la fecha de hoy
set DEST_DIR_IN="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD\%FolderName%\Inbound_Files"
set DEST_DIR_IN_TEST="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD\%FolderName%\Inbound_Files\test"
set DEST_DIR_OUT="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD\%FolderName%\Outbound_Files"
set DEST_DIR_IN_PRD="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\CESCE-ADS20240201PD-SAPCommissions-CESCE SOPORTE AM\02. Documentación Soporte\0203.Inbound_Files"
set DEST_DIR_IN_PRD_TEST="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\CESCE-ADS20240201PD-SAPCommissions-CESCE SOPORTE AM\02. Documentación Soporte\0203.Inbound_Files\test"
set DEST_DIR_OUT_PRD="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\CESCE-ADS20240201PD-SAPCommissions-CESCE SOPORTE AM\02. Documentación Soporte\0203.Outbound Files"
set DEST_DIR_BAD="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD\%FolderName%\Bad_Files"
set LOG_PATH="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD\%FolderName%\log"
set LOG=%LOG_PATH%\log%LogName%.txt
set /a contador=0

echo INICIANDO SCRIPT %date% %time% > %LOG%

:: Crear la carpeta de destino INBOUND si no existe
if not exist %LOG_PATH% (
    mkdir %LOG_PATH%
    echo Carpeta %LOG_PATH% creada. >> %LOG%
) else (
    echo Carpeta %LOG_PATH% ya existe. 
)

:: Crear la carpeta de destino INBOUND si no existe
if not exist %DEST_DIR_IN% (
    mkdir %DEST_DIR_IN%
    echo Carpeta %DEST_DIR_IN% creada. >> %LOG%
) else (
    echo Carpeta %DEST_DIR_IN% ya existe. 
)

:: Crear la carpeta de destino OUTBOUND si no existe
if not exist %DEST_DIR_OUT% (
    mkdir %DEST_DIR_OUT%
    echo Carpeta %DEST_DIR_OUT% creada. >> %LOG%
) else (
    echo Carpeta %DEST_DIR_OUT% ya existe. 
)

:: Crear la carpeta de destino BAD_FILE si no existe
if not exist %DEST_DIR_BAD% (
    mkdir %DEST_DIR_BAD%
    echo Carpeta %DEST_DIR_BAD% creada. >> %LOG%
) else (
    echo Carpeta %DEST_DIR_BAD% ya existe. 
)

:: Crear la carpeta de destino TEST si no existe
if not exist %DEST_DIR_IN_TEST% (
    mkdir %DEST_DIR_IN_TEST%
    echo Carpeta %DEST_DIR_IN_TEST% creada. >> %LOG%
) else (
    echo Carpeta %DEST_DIR_IN_TEST% ya existe. 
)

:: Crear la carpeta de destino TEST si no existe
if not exist %DEST_DIR_IN_PRD_TEST% (
    mkdir %DEST_DIR_IN_PRD_TEST%
    echo Carpeta %DEST_DIR_IN_PRD_TEST% creada. >> %LOG%
) else (
    echo Carpeta %DEST_DIR_IN__PRD_TEST% ya existe. 
)



echo INICIANDO CONEXIÓN %date% %time% >> %LOG%


:: Ejecutar WinSCP con los comandos integrados en el batch
%WINSCP_PATH% -log=%LOG% /nointeractiveinput /command ^
    "open %SFTP_SERVER% " ^
    "cd /archive" ^
    ls ^
    "lcd "%DEST_DIR_IN%"" ^
    "synchronize local -filemask=|*/ -criteria=none" ^
    "lcd "%DEST_DIR_IN_PRD%"" ^
    "synchronize local -filemask=|*/ -criteria=none" ^
    "cd /outbound" ^
    ls ^
    "lcd "%DEST_DIR_OUT%"" ^
    "synchronize local -filemask=|*/ -criteria=none" ^
    "lcd "%DEST_DIR_OUT_PRD%"" ^
    "synchronize local -filemask=|*/ -criteria=none" ^
    "cd /badfiles" ^
    ls ^
    "lcd "%DEST_DIR_BAD%"" ^
    "synchronize local -filemask=|*/ -criteria=none" ^
    "exit" 

:: Configurar las rutas de origen y destino
:: set SOURCE_DIR="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD\%FolderName%\Inbound_Files"



:: Crear la carpeta de destino INBOUND si no existe
if not exist %DEST_DIR_IN_TEST% (
    mkdir %DEST_DIR_IN_TEST%
    echo Carpeta %DEST_DIR_IN_TEST% creada. 
) else (
    echo Carpeta %DEST_DIR_IN_TEST% ya existe. 
)

:: Recorre cada archivo en la carpeta de origen
for %%f in (%DEST_DIR_IN%\2111*) do (
    :: Cambia el prefijo "2111" por "1689" en el nombre del archivo
    set nombre_origen=%%~nxf
    set nombre_nuevo=1689!nombre_origen:~4!

    
    :: Si el archivo no existe en el destino, copiarlo
    if not exist %DEST_DIR_IN_TEST%\!nombre_nuevo! (
        echo Copiando "%%f" como %DEST_DIR_IN_TEST%\"!nombre_nuevo!"
        copy "%%f" %DEST_DIR_IN_TEST%\"!nombre_nuevo!"
        :: Incrementa el contador en 1
        set /a contador+=1
    ) 
)

:: Mostramos registros copiados en el log
echo Copiados %contador% registros desde %DEST_DIR_IN% a %DEST_DIR_IN_TEST% >> %LOG% 

:: PRODUCCION
:: Crear la carpeta de destino INBOUND si no existe
if not exist %DEST_DIR_IN_PRD_TEST% (
    mkdir %DEST_DIR_IN_PRD_TEST%
    echo Carpeta %DEST_DIR_IN_PRD_TEST% creada. 
) else (
    echo Carpeta %DEST_DIR_IN_PRD_TEST% ya existe. 
)

:: Recorre cada archivo en la carpeta de origen
:: Reiniciamos contador
set /a contador = 0

for %%f in (%DEST_DIR_IN_PRD%\2111*) do (
    :: Cambia el prefijo "2111" por "1689" en el nombre del archivo
    set nombre_origen=%%~nxf
    set nombre_nuevo=1689!nombre_origen:~4!

    
    :: Si el archivo no existe en el destino, copiarlo
    if not exist %DEST_DIR_IN_PRD_TEST%\!nombre_nuevo! (
        echo Copiando "%%f" como %DEST_DIR_IN_PRD_TEST%\"!nombre_nuevo!"
        copy "%%f" %DEST_DIR_IN_PRD_TEST%\"!nombre_nuevo!"
        :: Incrementa el contador en 1
        set /a contador+=1
    ) 
)
:: Mostramos registros copiados en el log
echo Copiados %contador% registros desde %DEST_DIR_IN_PRD% a %DEST_DIR_IN_PRD_TEST% >> %LOG% 

:: Verificar el código de salida de WinSCP
if errorlevel 1 (
    echo ------------------¡¡¡ERROR!!!!-------------- >> %LOG%
    echo SE ENCONTRARON ERRRORES DURANTE LA EJECUCIÓN >> %LOG%
    
) else (
    echo PROCESO COMPLETADO CON EXITO >> %LOG%
    echo DESCARGA COMPLETADA >> %LOG%
)

:: pause
endlocal