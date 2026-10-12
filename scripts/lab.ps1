param(
    [Parameter(Position=0)]
    [string]$Command,
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$ArgsList
)

$PY = "/home/airflow/lab-venv/bin/python"
$DBT = "/home/airflow/lab-venv/bin/dbt"

switch ($Command) {
    "step"   { docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli step @ArgsList }
    "exp"    { docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli exp @ArgsList }
    "verify" { docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli verify @ArgsList }
    "test"   { docker compose exec -T -w /opt/lab airflow $PY -m unittest discover -s tests }
    "run"    { docker compose exec -T -w /opt/lab airflow scripts/run_pipeline.sh @ArgsList }
    "cli"    { docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli @ArgsList }
    "dbt"    { docker compose exec -T -w /opt/dbt airflow $DBT @ArgsList --no-use-colors }
    "shell"  { docker compose exec airflow bash }
    Default  {
        Write-Host "Cac lenh kha dung:"
        Write-Host "  .\scripts\lab.ps1 step <1..5|all> [--reset]"
        Write-Host "  .\scripts\lab.ps1 exp <idempotency|break-gate|leakage|threshold>"
        Write-Host "  .\scripts\lab.ps1 verify [--module N]"
        Write-Host "  .\scripts\lab.ps1 test"
        Write-Host "  .\scripts\lab.ps1 run [--reset]"
        Write-Host "  .\scripts\lab.ps1 cli <lenh>"
        Write-Host "  .\scripts\lab.ps1 dbt <lenh>"
        Write-Host "  .\scripts\lab.ps1 shell"
    }
}
