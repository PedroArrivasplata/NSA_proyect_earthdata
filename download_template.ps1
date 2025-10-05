# PowerShell script to download Earthdata TEMPO files on Windows
# Requires: curl (included in Windows 10+), valid ~/.netrc file

Write-Host 'Iniciando descarga de datos desde Earthdata...'

# Paths
$cookiejar = New-TemporaryFile
$netrcTemp = New-TemporaryFile

# Ensure cleanup on exit
$cleanup = {
    if (Test-Path $cookiejar) { Remove-Item $cookiejar -Force }
    if (Test-Path $netrcTemp) { Remove-Item $netrcTemp -Force }
}
Register-EngineEvent PowerShell.Exiting -Action $cleanup

# Copy credentials from .netrc
$netrcPath = "$env:USERPROFILE\.netrc"
if (!(Test-Path $netrcPath)) {
    Write-Error "ERROR: No se encontro el archivo .netrc en $netrcPath"
    Write-Host "Crea el archivo .netrc con este formato:"
    Write-Host "machine urs.earthdata.nasa.gov login TU_USUARIO password TU_CONTRASENA"
    exit 1
}

Write-Host "Usando credenciales desde $netrcPath"
Copy-Item $netrcPath $netrcTemp -Force

# Función para manejar errores
function Exit-WithError($message) {
    Write-Host ""
    Write-Host "ERROR: $message"
    Write-Host "Verifica si aprobaste la aplicacion en Earthdata:"
    Write-Host "https://urs.earthdata.nasa.gov/profile"
    exit 1
}

# Función principal de descarga
function Fetch-Urls {
    $downloadList = "download_list.txt"

    if (!(Test-Path $downloadList)) {
        Exit-WithError "No se encontró el archivo download_list.txt"
    }

    $urls = Get-Content $downloadList | Where-Object { $_ -ne "" }

    foreach ($url in $urls) {
        $filename = [System.IO.Path]::GetFileName($url.Split("?")[0])
    Write-Host "`nDescargando $filename ..."

        try {
            curl -s -L -b $cookiejar -c $cookiejar --netrc-file $netrcTemp -o $filename $url
            if (Test-Path $filename) {
                Write-Host "Archivo descargado: $filename"
            } else {
                Exit-WithError "No se pudo descargar $filename"
            }
        } catch {
            Exit-WithError "Fallo al descargar $url"
        }
    }
}

# Ejecutar descarga
Fetch-Urls
Write-Host "`nDescarga completada con éxito."
