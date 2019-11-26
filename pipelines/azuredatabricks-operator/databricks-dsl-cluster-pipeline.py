import json
import kfp.dsl as dsl
import kfp.compiler as compiler
# Git clone custom Kubernetes Pipelines SDK https://github.com/magencio/pipelines.git, databricks-wrapper branch to 
# e.g. /mnt/c/_git/magencio-kubeflow-pipelines. Then add the SDK to PYTHONPATH: 
# export PYTHONPATH=/mnt/c/_git/magencio-kubeflow-pipelines/sdk/python:$PYTHONPATH
import kfp.dsl.databricks as databricks

_CLUSTER_SPEC = """
{
    "spark_version":"5.3.x-scala2.11",
    "node_type_id": "Standard_D3_v2",
    "spark_conf": {
        "spark.speculation": "true"
    },
    "num_workers": 2
}
"""

_JOB_SPEC = """
{
    "libraries": [
        {
            "jar": "dbfs:/my-jar.jar"
        },
        {
            "maven": {
                "coordinates": "org.jsoup:jsoup:1.7.2"
            }
        }
    ],
    "timeout_seconds": 3600,
    "max_retries": 1,
    "schedule": {
        "quartz_cron_expression": "0 15 22 ? * *",
        "timezone_id": "America/Los_Angeles"
    },
    "spark_jar_task": {
        "main_class_name": "com.databricks.ComputeModels"
    }
}
"""

def create_cluster(cluster_name, cluster_spec):
    return databricks.CreateClusterOp(
        name = "createcluster",
        cluster_name = cluster_name,
        spec = json.loads(cluster_spec)
    )

def create_job(cluster_id, job_name, job_spec):
    spec = json.loads(job_spec)
    spec["existing_cluster_id"] = cluster_id
    return databricks.CreateJobOp(
        name = "createjob",
        job_name = job_name,
        spec = spec
    )

def delete_cluster(cluster_name):
    return databricks.DeleteClusterOp(
        name = "deletecluster",
        cluster_name = cluster_name
    )

@dsl.pipeline(
    name="DatabricksCluster",
    description="A toy pipeline that performs arithmetic calculations with a bit of Azure with Databricks.",
)
def calc_pipeline(cluster_name="test-cluster", job_name="test-job"):
    create_cluster_result = create_cluster(cluster_name, _CLUSTER_SPEC)
    create_job_result = create_job(create_cluster_result.outputs["cluster_id"], job_name, _JOB_SPEC)
    delete_cluster_result = delete_cluster(create_cluster_result.outputs["cluster_name"])
    delete_cluster_result.after(create_job_result)

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")