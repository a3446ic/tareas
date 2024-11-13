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
set DEST_DIR="C:\Users\a3446\OneDrive - INSTRUMENTACION Y COMPONENTES SA\ficherosPRD



echo FolderName %FolderName%

for %%f in (%DEST_DIR_IN%\2111*) do (
    set "nombre_origen=%%~nxf"
    
    rem Extraer subcadenas de nombre_origen usando `call` para evaluar después de la asignación
    set "carpeta_mes=!nombre_origen:~15,8!"
    set "dia_mes=!nombre_origen:~21,2!"

    rem Mostrar variables para ver el valor asignado
    echo nombre_origen: !nombre_origen!
    echo carpeta_mes: !carpeta_mes!
    echo dia_mes: !dia_mes!

    rem Comparar FolderName con carpeta_mes
    if "%FolderName%" NEQ "!carpeta_mes!" (
        :: Crear la carpeta de destino INBOUND si no existe
        echo DESTINO %DEST_DIR%\!carpeta_mes!\Inbound_Files"
        if not exist %DEST_DIR%\!carpeta_mes!\Inbound_Files" (
            echo **** %DEST_DIR%\!carpeta_mes!\Inbound_Files"
        )
    ) 
)

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