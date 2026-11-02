# olist-lakehouse

![Python](https://img.shields.io/badge/python-3.12+-blue)
![uv](https://img.shields.io/badge/managed%20with-uv-de5fe9)
![Databricks](https://img.shields.io/badge/Declarative-Automation%20Bundles-red)
![Ruff](https://img.shields.io/badge/lint-ruff-orange)
![License](https://img.shields.io/badge/license-MIT-green)

## Overview

`olist-lakehouse` is a personal project built by me (luizhtc) for educational purposes.

This project uses the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
posted at Kaggle to build a complete Databricks Lakehouse featuring bronze, silver and gold tables deployed to Unity Catalog.

It is built to demonstrate end-to-end data engineering on Databricks Data and AI Platform: ingestion,
the medallion architecture, Delta Lake, Unity Catalog, reusable packaged libraries and tested transformation code.

* `src/`: Python source code for this project.
  * `src/olist_lakehouse/`: Shared Python code used by jobs and notebooks.
* `resources/`:  Resource configurations.
* `tests/`: Unit tests for the shared Python code.
* `databricks.yml`: Databricks DABs configuration file.

## Getting started

Choose how you want to work on this project:

(a) Directly in your Databricks workspace, see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code, see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With command line tools, see https://docs.databricks.com/dev-tools/cli/databricks-cli.html

If you're developing with an IDE, dependencies for this project should be installed using uv:

*  Make sure you have the UV package manager installed.
   It's an alternative to tools like pip: https://docs.astral.sh/uv/getting-started/installation/.
*  Run `uv sync --dev` to install the project's dependencies.

---

### Parameter variables configuration

This bundle uses a small set of variables that should be set before deploying or running jobs locally.

#### Required local values

Copy the example file to `databricks.local.yml` and replace the placeholders:

```yaml
# databricks.local.yml
targets:
  dev:
    workspace:
      host: https://<your-workspace>.cloud.databricks.com
  prod:
    workspace:
      host: https://<your-workspace>.cloud.databricks.com

variables:
  workspace_user:
    default: your-email@example.com
```

#### Variables used by the project

- `catalog_name`: the Unity Catalog catalog name used for deployment of tables and UC objects, this catalog must be already created, if it does not exist, the bundle will fail to deploy.
- `bronze_schema_name`: the Bronze layer medallion schema name.
- `silver_schema_name`: the Silver layer medallion schema name.
- `gold_schema_name`: the Gold layer medallion schema name.

**Note: when deploying in target environments with mode: development, the schemas will be prefixed with 'dev_<-username->', you can change this in `databricks.yml`.

---

### Databricks required secret KAGGLE_API_TOKEN

The kaggle ingestion notebook requires a Kaggle API token, you need to use Databricks secrets to deploy a `KAGGLE_API_TOKEN` secret key within a scope named `kaggle`.

Run the following Databricks CLI commands in a safe environment to create the scope and secret:

```bash
databricks secrets create-scope kaggle
databricks secrets put-secret kaggle KAGGLE_API_TOKEN --string-value "<your_token>"
```

You can also create the scope and secret with Python, running this cell in a Databricks Workspace notebook or python file:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.secrets.create_scope("kaggle")
w.secrets.put_secret(
    scope="kaggle",
    key="KAGGLE_API_TOKEN",
    string_value="<your_token>",
)
```

This value is required because the ingestion step uses the `kagglehub` lib to fetch the Olist dataset files from Kaggle.
