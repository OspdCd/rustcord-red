$ErrorActionPreference = 'Stop'

$listener = Get-NetTCPConnection -LocalPort 3000 -State Listen -ErrorAction SilentlyContinue |
    Select-Object -First 1
if ($listener) {
    throw 'Port 3000 is already in use. Stop the running backend, then run this script again.'
}

$secureSecret = Read-Host 'Enter your NEW Cloudflare Turnstile secret key' -AsSecureString
if ($secureSecret.Length -eq 0) {
    throw 'The Turnstile secret key cannot be empty.'
}

$secretPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureSecret)
try {
    $env:TURNSTILE_SECRET_KEY = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($secretPointer)
}
finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($secretPointer)
    Remove-Variable secureSecret
}

$env:TURNSTILE_SITE_KEY = '0x4AAAAAAFfRY37-7PGoYfxH'
Push-Location $PSScriptRoot
try {
    cargo run
    $cargoExitCode = $LASTEXITCODE
}
finally {
    Remove-Item Env:TURNSTILE_SECRET_KEY -ErrorAction SilentlyContinue
    Remove-Item Env:TURNSTILE_SITE_KEY -ErrorAction SilentlyContinue
    Pop-Location
}

exit $cargoExitCode
