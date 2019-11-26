import json
import kfp
import kfp.dsl as dsl
import kfp.compiler as compiler
# Git clone custom Kubernetes Pipelines SDK https://github.com/magencio/pipelines.git, databricks-wrapper branch to 
# e.g. /mnt/c/_git/magencio-kubeflow-pipelines. Then add the SDK to PYTHONPATH: 
# export PYTHONPATH=/mnt/c/_git/magencio-kubeflow-pipelines/sdk/python:$PYTHONPATH
import kfp.dsl.databricks as databricks

@dsl.pipeline(
    name="DatabricksRun",
    description="A toy pipeline that computes an approximation to pi with Azure Databricks.",
)
def calc_pipeline(run_name="test-run", parameter="10"):

    # Sample based on [Create a spark-submit job](https://docs.databricks.com/dev-tools/api/latest/examples.html#create-and-run-a-jar-job)
    # Additional info:
    #   - [Databricks File System](https://docs.microsoft.com/en-us/azure/databricks/data/databricks-file-system)
    #   - [DBFS CLI](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-cli#dbfs-cli)
    databricks.SubmitRunOp(
        name = "submitrun",
        run_name = run_name,
        new_cluster = {
            "spark_version":"5.3.x-scala2.11",
            "node_type_id": "Standard_D3_v2",
            "num_workers": 2
        },
        libraries = [{"jar": "dbfs:/docs/sparkpi.jar"}],
        spark_jar_task = {
            "main_class_name": "org.apache.spark.examples.SparkPi",
            "parameters": [parameter]
        }
    )

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")