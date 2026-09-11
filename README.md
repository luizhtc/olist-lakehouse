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
* `notebooks/`: Notebooks used by jobs and other analysis.
* `resources/`:  Resource configurations.
* `tests/`: Unit tests for the shared Python code.

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

- `workspace_user`: the user email used to build the target workspace path and assign manage permissions in the Databricks bundle.
- `catalog`: the Unity Catalog catalog used by the job parameter, defaulting to `olist` in `databricks.yml`.

You can override the catalog value when deploying or running the job if you want to target a different catalog name.

---

### Databricks required secret KAGGLE_API_TOKEN

This notebook requires a Kaggle API token, defined in the `kaggle` Databricks secret scope.

Run the following Databricks CLI commands in a safe environment to create the scope and secret:

```bash
databricks secrets create-scope kaggle
databricks secrets put-secret kaggle KAGGLE_API_TOKEN --string-value "<your_token>"
```

You can also create the scope and secret with Python:

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
