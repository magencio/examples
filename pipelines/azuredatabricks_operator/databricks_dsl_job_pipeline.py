import kfp.dsl as dsl
import kfp.compiler as compiler
# Git clone custom Kubernetes Pipelines SDK https://github.com/magencio/pipelines.git,
# databricks-wrapper branch to e.g. /mnt/c/_git/magencio-kubeflow-pipelines.
# Then add the SDK to PYTHONPATH:
# export PYTHONPATH=/mnt/c/_git/magencio-kubeflow-pipelines/sdk/python:$PYTHONPATH
import kfp.dsl.databricks as databricks

@dsl.pipeline(
    name="DatabricksRun",
    description="A toy pipeline that computes an approximation to pi with Azure Databricks."
)
def calc_pipeline(job_name="test-job", parameter="10"):

    # Sample based on https://docs.databricks.com/dev-tools/api/latest/examples.html#create-and-run-a-jar-job
    # Additional info:
    #   - Databricks File System: https://docs.microsoft.com/en-us/azure/databricks/data/databricks-file-system
    #   - DBFS CLI: https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-cli#dbfs-cli
    create_job_task = databricks.CreateJobOp(
        name="createjob",
        job_name=job_name,
        new_cluster={
            "spark_version":"5.3.x-scala2.11",
            "node_type_id": "Standard_D3_v2",
            "num_workers": 2
        },
        libraries=[{"jar": "dbfs:/docs/sparkpi.jar"}],
        spark_jar_task={
            "main_class_name": "org.apache.spark.examples.SparkPi",
            "parameters": [parameter]
        },
        schedule={
            "quartz_cron_expression": "0 15 22 ? * *",
            "timezone_id": "America/Los_Angeles"
        }
    )

    delete_job_task = databricks.DeleteJobOp(
        name="deletejob",
        job_name=job_name
    )
    delete_job_task.after(create_job_task)

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")
