# azure_costs_to_databricks

## Purpose

Import Azure Cost data into Databricks using Delta Live Tables.

## Prerequisites

- Permissions to export cost data from the Azure Cost Management page (or API)
- An existing Databricks workspace
- A storage account for the cost data export to be delivered to by Azure Cost Management. This must be set up as a volume in Databricks, make sure it's an external volume.
- Set up a daily export of the cost data to the storage account. See https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-export-acm-data?tabs=azure-portal
  - Choose Parquet as the file format
  - Choose to export to the storage account that has been set up as a volume in Databricks
  - Tick the box for "Overwrite data" so that each day the file for the current month is updated with the latest data rather than a new file being created each day.


## Getting started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To check the bundle is set up correctly, type:

   ```
   $ databricks bundle validate --target dev
   ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] azure_costs_to_databricks_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/azure_costs_to_databricks_job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

5. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.
