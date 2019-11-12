"""Main pipeline file"""
import kfp.dsl as dsl
import kfp.compiler as compiler
import kfp.components as components
# Git clone custom Kubernetes Pipelines SDK https://github.com/magencio/pipelines.git, databricks-wrapper branch to 
# e.g. /mnt/c/_git/magencio-kubeflow-pipelines. Then add the SDK to PYTHONPATH: 
# export PYTHONPATH=/mnt/c/_git/magencio-kubeflow-pipelines/sdk/python:$PYTHONPATH
import kfp.dsl.databricks as databricks

import json
import time

_DJOB_SPEC = """
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

def create_job():
  # TODO - also consider json.loads approach as per: https://github.com/kubeflow/pipelines/blob/master/samples/core/resource_ops/resource_ops.py

    return databricks.CreateJobOp(
      name="createjob",
      job_name_prefix="test-job-",
      spec=json.loads(_DJOB_SPEC)
    )

def delete_job(inputVal):
    return databricks.DeleteJobOp(
      name = "deletejob",
      job_name = inputVal
    )

@dsl.pipeline(
    name="Databricks",
    description="A toy pipeline that performs arithmetic calculations with a bit of Azure with Databricks.",
)
def calc_pipeline():
  create_job_result = create_job()
  delete_job(create_job_result.outputs["job_name"])

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")
