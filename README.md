# iFood Data Architecture Case

Pipeline PySpark para ingestão dos dados NYC TLC Yellow Taxi no S3. O armazenamento é exclusivamente cloud.

## Execução no Databricks

Configure no Job ou cluster `S3_BUCKET`, `AWS_REGION`, `DATE_FROM`, `DATE_TO` e, opcionalmente, `S3_BASE_PREFIX` e `RUN_ID`. Conceda ao compute acesso ao bucket por IAM role/instance profile; não salve credenciais AWS no repositório.

```python
from src.config import DATE_FROM, DATE_TO
from src.ingestor import ingest_months
from src.utils import generate_year_month_range

ingest_months(generate_year_month_range(DATE_FROM, DATE_TO))
```

Depois crie a Bronze reutilizando a sessão gerenciada:

```python
from src.bronze import create_bronze

create_bronze(spark)
```

Depois, execute a Silver com `python -m src.silver`. Ela grava Parquet no diretório
`silver` do `RUN_ID` e a tabela Delta `workspace.ifood_case.ny_yellow_silver`,
particionada por ano e mês.
