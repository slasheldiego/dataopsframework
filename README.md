# DataOps Framework

## Estructura
- `config/` JSON por entorno (`env.dev.json`, `env.prod.json`)
- `data/` CSV de ejemplo para ingesta
- `utils/` funciones de IO, validación y logging
- `notebooks/` pipeline parametrizable por etapas + orquestador
- `ops/` consultas SQL útiles
- `tests/` pruebas unitarias simples (para ejecutar localmente)

## Pasos (Databricks Community)
1) Crea carpetas en Workspace y **sube estos .py** (Import > File).
2) Sube `env.dev.json` a **/FileStore/config/** y el CSV a **/FileStore/data/** (Data > DBFS > Upload).
3) Abre `notebooks/50_orchestrator.py` y define widget `env=dev`.
4) Ejecuta el notebook completo. Verás tablas Delta en DB `demo`:
   - `bronze_customers`, `silver_customers`, `gold_customers`
   - Logging: `ops_event_log`, `ops_dq_results`

## Parametrización
Edita `config/env.dev.json` para cambiar paths, nombres de tablas y reglas DQ.

## Orquestación
El orquestador usa `%run` para encadenar notebooks. Puedes convertirlo en **Job** y programarlo.

## Extensiones sugeridas
- Añadir *alertas* (email/webhook) según `failed > 0` en `ops_dq_results`.
- Integrar *dbt* para transformaciones y *pytest* para tests en CI.
- Medir *costos/tiempos* y publicar métricas en un dashboard de notebook.