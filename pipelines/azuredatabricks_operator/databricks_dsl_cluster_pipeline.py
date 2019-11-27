import kfp.dsl as dsl
import kfp.compiler as compiler
# Git clone custom Kubernetes Pipelines SDK https://github.com/magencio/pipelines.git,
# databricks-wrapper branch to e.g. /mnt/c/_git/magencio-kubeflow-pipelines.
# Then add the SDK to PYTHONPATH:
# export PYTHONPATH=/mnt/c/_git/magencio-kubeflow-pipelines/sdk/python:$PYTHONPATH
import kfp.dsl.databricks as databricks

def create_cluster(cluster_name):
    return databricks.CreateClusterOp(
        name="createcluster",
        cluster_name=cluster_name,
        spark_version="5.3.x-scala2.11",
        node_type_id="Standard_D3_v2",
        spark_conf={
            "spark.speculation": "true"
        },
        num_workers=2
    )

def create_job(cluster_id, job_name):
    # Sample based on [Create a spark-submit job](https://docs.databricks.com/dev-tools/api/latest/examples.html#create-and-run-a-jar-job)
    # Additional info:
    #   - [Databricks File System](https://docs.microsoft.com/en-us/azure/databricks/data/databricks-file-system)
    #   - [DBFS CLI](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-cli#dbfs-cli)
    return databricks.CreateJobOp(
        name="createjob",
        job_name=job_name,
        spec={
            "existing_cluster_id": cluster_id,
            "libraries": [
                {
                    "jar": "dbfs:/docs/sparkpi.jar"
                }
            ],
            "timeout_seconds": 3600,
            "max_retries": 1,
            "schedule": {
                "quartz_cron_expression": "0 15 22 ? * *",
                "timezone_id": "America/Los_Angeles"
            },
            "spark_jar_task": {
                "main_class_name": "org.apache.spark.examples.SparkPi",
                "parameters": ["10"]
            }
        }
    )

def delete_cluster(cluster_id):
    return databricks.DeleteClusterOp(
        name="deletecluster",
        cluster_id=cluster_id
    )

@dsl.pipeline(
    name="DatabricksCluster",
    description="A toy pipeline that performs arithmetic calculations with a bit of Azure with Databricks.",
)
def calc_pipeline(cluster_name="test-cluster", job_name="test-job"):
    create_cluster_task = create_cluster(cluster_name)
    create_job_task = create_job(create_cluster_task.outputs["cluster_id"], job_name)
    delete_cluster_task = delete_cluster(create_cluster_task.outputs["cluster_id"])
    delete_cluster_task.after(create_job_task)

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")
