@echo off
setlocal

REM Configuración de la ruta de WinSCP y detalles de conexión
set WINSCP_PATH="C:\Program Files (x86)\WinSCP\WinSCP.com"
set SFTP_SERVER=sftp://2111:CescePr@d@xfer-prd-fra.calliduscloud.com/
set HOSTKEY="ssh-rsa 1024 MKA9n3CYF8dY+j9P713bUoWelyJtFdv8gNpfn8pkzoc"

REM Obtener la fecha actual en formato yyyymmdd usando WMIC
for /f %%i in ('wmic os get localdatetime ^| find "."') do set datetime=%%i
set Year=%datetime:~0,4%
set Month=%datetime:~4,2%
set Day=%datetime:~6,2%

REM Formatear la fecha como ddmmyyyy para la carpeta
set FolderName=%Month%%Year%
set LogName=%Day%%Month%%Year%

REM Definir la carpeta de destino con la fecha de hoy
set DEST_DIR_IN="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD\%FolderName%\Inbound_Files"
set DEST_DIR_OUT="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD\%FolderName%\Outbound_Files"
set DEST_DIR_BAD="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD\%FolderName%\Bad_Files"
set LOG_PATH="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD\%FolderName%\log"
set LOG=%LOG_PATH%\log%LogName%.txt

echo INICIANDO SCRIPT %date% %time% > %LOG%

REM Crear la carpeta de destino INBOUND si no existe
if not exist %LOG_PATH% (
    mkdir %LOG_PATH%
    echo Carpeta %LOG_PATH% creada. 
) else (
    echo Carpeta %LOG_PATH% ya existe. 
)

REM Crear la carpeta de destino INBOUND si no existe
if not exist %DEST_DIR_IN% (
    mkdir %DEST_DIR_IN%
    echo Carpeta %DEST_DIR_IN% creada. >> %LOG%
) else (
    echo Carpeta %DEST_DIR_IN% ya existe. >> %LOG%
)

REM Crear la carpeta de destino OUTBOUND si no existe
if not exist %DEST_DIR_OUT% (
    mkdir %DEST_DIR_OUT%
    echo Carpeta %DEST_DIR_OUT% creada. >> %LOG%
) else (
    echo Carpeta %DEST_DIR_OUT% ya existe. >> %LOG%
)

REM Crear la carpeta de destino BAD_FILE si no existe
if not exist %DEST_DIR_BAD% (
    mkdir %DEST_DIR_BAD%
    echo Carpeta %DEST_DIR_BAD% creada. >> %LOG%
) else (
    echo Carpeta %DEST_DIR_BAD% ya existe. >> %LOG%
)



echo INICIANDO CONEXIÓN %date% %time% >> %LOG%


REM Ejecutar WinSCP con los comandos integrados en el batch
%WINSCP_PATH% -log=%LOG% /nointeractiveinput /command ^
    "open %SFTP_SERVER% " ^
    "cd /archive" ^
    ls ^
    "lcd "%DEST_DIR_IN%"" ^
    "synchronize local -filemask=|*/ -criteria=none" ^
    "cd /outbound" ^
    ls ^
    "lcd "%DEST_DIR_OUT%"" ^
    "synchronize local -filemask=|*/ -criteria=none" ^
    "cd /badfiles" ^
    ls ^
    "lcd "%DEST_DIR_BAD%"" ^
    "synchronize local -filemask=|*/ -criteria=none" ^
    "exit" 

REM Verificar el código de salida de WinSCP
if errorlevel 1 (
    echo ------------------¡¡¡ERROR!!!!-------------- >> %LOG%
    echo SE ENCONTRARON ERRRORES DURANTE LA EJECUCIÓN >> %LOG%
    
) else (
    echo PROCESO COMPLETADO CON EXITO >> %LOG%
    echo DESCARGA COMPLETADA >> %LOG%
)

REM pause
endlocal