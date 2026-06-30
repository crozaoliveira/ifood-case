# iFood Data Architecture Case

Pipeline de dados em PySpark para ingestão e processamento dos dados de corridas de táxi da [NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). O projeto é implantado no Databricks como um **Asset Bundle** e executa as camadas Landing, Bronze e Silver.

O período padrão do case é de janeiro a maio de 2023. A implementação processa os datasets Yellow Taxi e Green Taxi; as consultas analíticas usam a tabela Silver resultante.

## Arquitetura

```text
NYC TLC (Parquet)
        |
        v
Landing -> arquivos originais por execução e categoria
        |
        v
Bronze  -> schema padronizado e metadados técnicos
        |
        v
Silver  -> dados tipados, validados e particionados
        |
        v
Análises SQL / notebook
```

Os arquivos são gravados em um Volume do Unity Catalog:

```text
/Volumes/workspace/ifood_case/ny_tlc_tripdata/<RUN_ID>/
|-- landing/
|   |-- yellow/
|   `-- green/
|-- bronze/
`-- silver/
```

O pipeline também cria as tabelas Delta:

- `workspace.ifood_case.ny_tlc_bronze`
- `workspace.ifood_case.ny_tlc_silver`

## Pré-requisitos

- Workspace Databricks com Unity Catalog habilitado.
- Permissão para usar o catálogo `workspace` e criar objetos no schema `ifood_case`.
- Acesso de saída à internet no compute Serverless para baixar os arquivos da NYC TLC.
- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) versão `0.218.0` ou superior.
- Git para clonar o repositório.

O Job utiliza **Serverless Jobs**; não é necessário criar um cluster manualmente.

## 1. Preparar o Unity Catalog

No Databricks SQL Editor ou em um notebook SQL, execute uma vez:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.ifood_case;

CREATE VOLUME IF NOT EXISTS workspace.ifood_case.ny_tlc_tripdata;
```

O usuário ou service principal que executará o Job deve conseguir usar o catálogo e o schema, ler e gravar no volume e criar ou substituir tabelas no schema.

> Os nomes estão definidos em `src/config.py`. Para usar outro catálogo, schema ou volume, altere `catalog_name`, `schema_name` e `table_name` antes do deploy.

## 2. Autenticar a Databricks CLI

Autentique-se no workspace usando OAuth:

```bash
databricks auth login --host https://<seu-workspace>.cloud.databricks.com
```

Quando solicitado, informe um nome para o profile, por exemplo `ifood-case`. Confira a autenticação:

```bash
databricks auth profiles
```

Caso não utilize o profile padrão, acrescente `--profile ifood-case` aos comandos seguintes ou configure `DATABRICKS_CONFIG_PROFILE`.

## 3. Validar e implantar o bundle

Na raiz do repositório, valide a configuração:

```bash
databricks bundle validate -t dev
```

Implante o Job no ambiente de desenvolvimento:

```bash
databricks bundle deploy -t dev
```

O target `dev` publica os arquivos em uma pasta privada do usuário no Workspace. Também existe o target `prod`, que publica em `/Workspace/Shared/.bundle/ifood-case/prod`:

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

Use `prod` somente se o usuário de execução tiver as permissões necessárias no caminho compartilhado e no Unity Catalog.

## 4. Executar o pipeline

Execute com os parâmetros padrão (`2023/01` a `2023/05`):

```bash
databricks bundle run -t dev Teste_Ifood_Case
```

Para definir o período e o identificador da execução:

```bash
databricks bundle run -t dev Teste_Ifood_Case --params date_from=2023/01,date_to=2023/05,run_id=case_2023_01_05
```

Parâmetros disponíveis:

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `date_from` | `2023/01` | Primeiro mês, no formato `YYYY/MM` |
| `date_to` | `2023/05` | Último mês, no formato `YYYY/MM` |
| `run_id` | vazio | Identificador usado no caminho do Volume |

O intervalo é inclusivo e pode atravessar anos. Se `run_id` ficar vazio, o pipeline utiliza automaticamente o ID numérico da execução do Job.

Também é possível executar pela interface do Databricks: acesse **Workflows > Jobs**, abra **Pipeline Ifood-Case**, selecione **Run now with different parameters** e preencha os parâmetros.

## Etapas do Job

O workflow executa três tasks dependentes:

1. `ingestor`: baixa os Parquets originais para a Landing. Arquivos existentes são ignorados.
2. `bronze`: combina Yellow e Green Taxi, padroniza tipos e adiciona metadados técnicos.
3. `silver`: seleciona e tipa os campos analíticos, descarta datas inválidas e adiciona ano, mês e hora de pickup.

A Silver em Parquet é particionada por `TaxiCategory`, `PickupYear` e `PickupMonth`. A tabela Delta é particionada por `PickupYear` e `PickupMonth`.

## 5. Validar a execução

Após o Job terminar, confira as tasks em **Workflows > Jobs > Pipeline Ifood-Case**. Valide a tabela Silver no SQL Editor:

```sql
SELECT
  TaxiCategory,
  PickupYear,
  PickupMonth,
  COUNT(*) AS total_rows
FROM workspace.ifood_case.ny_tlc_silver
GROUP BY TaxiCategory, PickupYear, PickupMonth
ORDER BY PickupYear, PickupMonth, TaxiCategory;
```

Confira os arquivos em **Catalog > workspace > ifood_case > Volumes > ny_tlc_tripdata > `<RUN_ID>`**.

## 6. Executar as análises

Os SQLs estão em `analysis/`:

- `01_avg_total_amount_month.sql`: valor médio mensal das corridas Yellow Taxi.
- `02_avg_passenger_by_hour_may.sql`: média de passageiros por hora em maio de 2023.
- `03_avg_passenger_by_hour_may_by_category.sql`: volume por hora e categoria em fevereiro de 2023.

Copie o conteúdo dos arquivos para o Databricks SQL Editor ou importe e execute `notebooks/04_analysis.ipynb` após a conclusão da Silver.

## Reexecução e escrita

- A Landing é idempotente dentro do mesmo `run_id`: arquivos existentes não são baixados novamente.
- Os diretórios Bronze e Silver do `run_id` são escritos com `overwrite`.
- As tabelas Delta Bronze e Silver também são substituídas e representam a execução mais recente concluída.
- Para manter execuções isoladas, informe um `run_id` único ou deixe o Databricks usar o Job Run ID.

Evite pipelines simultâneos, pois ambos publicariam nas tabelas compartilhadas `ny_tlc_bronze` e `ny_tlc_silver`.

## Estrutura do repositório

```text
.
|-- analysis/              # Consultas SQL do case
|-- notebooks/             # Notebook de análise
|-- resources/pipeline.yml # Definição do Databricks Job
|-- src/
|   |-- config.py          # Parâmetros e objetos do Unity Catalog
|   |-- ingestor.py        # Download para Landing
|   |-- bronze.py          # Construção da Bronze
|   |-- silver.py          # Construção da Silver
|   `-- utils/             # Validação e geração do intervalo mensal
|-- databricks.yml         # Configuração do Asset Bundle
`-- requirements.txt       # Dependências para desenvolvimento local
```

## Solução de problemas

- **`PERMISSION_DENIED` no catálogo ou volume:** confira os grants e a existência de `workspace.ifood_case.ny_tlc_tripdata`.
- **Falha no download:** confirme que o Serverless possui acesso de saída a `d37ci6vzurychx.cloudfront.net`.
- **Nenhum Parquet encontrado:** verifique a task `ingestor` e confirme que todas as tasks receberam o mesmo `run_id`.
- **Bundle não encontra o workspace:** refaça `databricks auth login` e confirme o profile selecionado.
- **Data inválida:** use `YYYY/MM`; o mês deve estar entre `01` e `12`, e `date_from` não pode ser posterior a `date_to`.
