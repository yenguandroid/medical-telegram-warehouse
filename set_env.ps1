
# set_env.ps1
# Run this script before using dbt in any new PowerShell session:
#   . .\set_env.ps1
 
$env:POSTGRES_HOST     = "localhost"
$env:POSTGRES_PORT     = "5432"
$env:POSTGRES_DB       = "medical_warehouse"
$env:POSTGRES_USER     = "loolteke"
$env:POSTGRES_PASSWORD = "#1234#"