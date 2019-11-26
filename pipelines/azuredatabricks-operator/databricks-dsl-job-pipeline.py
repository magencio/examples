import json
import kfp.dsl as dsl
import kfp.compiler as compiler
# Git clone custom Kubernetes Pipelines SDK https://github.com/magencio/pipelines.git, databricks-wrapper branch to 
# e.g. /mnt/c/_git/magencio-kubeflow-pipelines. Then add the SDK to PYTHONPATH: 
# export PYTHONPATH=/mnt/c/_git/magencio-kubeflow-pipelines/sdk/python:$PYTHONPATH
import kfp.dsl.databricks as databricks

_JOB_SPEC = """
{
  "new_cluster": {
    "spark_version":"5.3.x-scala2.11",
    "node_type_id": "Standard_D3_v2",
    "num_workers": 2
  },
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

def create_job(job_name, job_spec):
    return databricks.CreateJobOp(
      name = "createjob",
      job_name = job_name,
      spec = json.loads(job_spec)
    )

def delete_job(job_name):
    return databricks.DeleteJobOp(
      name = "deletejob",
      job_name = job_name
    )

@dsl.pipeline(
    name="DatabricksJob",
    description="A toy pipeline that performs arithmetic calculations with a bit of Azure with Databricks.",
)
def calc_pipeline(job_name="test-job"):
  create_job_result = create_job(job_name, _JOB_SPEC)
  delete_job(create_job_result.outputs["job_name"])

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")
