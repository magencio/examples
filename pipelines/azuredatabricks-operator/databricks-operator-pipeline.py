# Copyright 2019 microsoft.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from kubernetes import client as k8s
import kfp.dsl as dsl
import kfp.compiler as compiler
import kfp.components as components

import time

def submit_run():
  return dsl.ResourceOp(
    k8s_resource={
      "apiVersion": "databricks.microsoft.com/v1alpha1",
      "kind": "Run",
      "metadata": {
        "name":"test-run",
      },
      "spec":{
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
        "spark_jar_task": {
          "main_class_name": "com.databricks.ComputeModels"
        }
      },
    },
    action = "create", 
    success_condition = "status.metadata.state.life_cycle_state != PENDING, status.metadata.state.life_cycle_state != RUNNING, status.metadata.state.life_cycle_state != TERMINATING",
    attribute_outputs = {
        "name": "{.status.metadata.run_id}",
        "run_id": "{.status.metadata.run_id}",
        "run_name": "{.status.metadata.run_name}",
        "error": "{.status.error}",
        "result": "{.status.notebook_output.result}",
        "result_truncated": "{.status.notebook_output.truncated}",
        "life_cycle_state": "{.status.metadata.state.life_cycle_state}",
        "result_state": "{.status.metadata.state.result_state}"
    },
    name="createjob"
  )

def create_cluster():
  return dsl.ResourceOp(
      k8s_resource={
        "apiVersion": "databricks.microsoft.com/v1alpha1",
        "kind": "Dcluster",
        "metadata": {
          "name":"test-cluster",
        },
        "spec":{
          "spark_version":"5.3.x-scala2.11",
          "node_type_id": "Standard_D3_v2",
          "spark_conf": {
            "spark.speculation": "true"
          },
          "num_workers": 2
        }
      },
      name="foo"
  )

def create_job():
  # TODO - also consider json.loads approach as per: https://github.com/kubeflow/pipelines/blob/master/samples/core/resource_ops/resource_ops.py

  return dsl.ResourceOp(
      k8s_resource={
        "apiVersion": "databricks.microsoft.com/v1alpha1",
        "kind": "Djob",
        "metadata": {
          "name":"test-job-" + str(time.time()),
        },
        "spec":{
          "new_cluster" : {
            "spark_version":"5.3.x-scala2.11",
            "node_type_id": "Standard_D3_v2",
            "num_workers": 2
          },
          "libraries" : [
            {
              "jar": 'dbfs:/my-jar.jar'
            },
            {
              "maven": {
                "coordinates": 'org.jsoup:jsoup:1.7.2'
              }
            }
          ],
          "timeout_seconds" : 3600,
          "max_retries": 1,
          "schedule":{
            "quartz_cron_expression": "0 15 22 ? * *",
            "timezone_id": "America/Los_Angeles",
          },
          "spark_jar_task": {
            "main_class_name": "com.databricks.ComputeModels",
          },
        },
    },
    success_condition="status.job_status.job_id > 0",
    attribute_outputs= {
      "name": "{.status.job_status.job_id}",
      "job_id": "{.status.job_status.job_id}",
      "job_name": "{.metadata.name}"
    },
    action="create",
    name="createjob"
  )

def delete_job(inputVal):
  # name = "deletejob-" + inputVal
  name = "deletejob"
  return dsl.ResourceOp(
      k8s_resource={
        "apiVersion": "databricks.microsoft.com/v1alpha1",
        "kind": "Djob",
        "metadata": {
          # "name":"test-job"
          "name":inputVal,
        },
    },
    action="delete",
    name=name,
  )

@dsl.pipeline(
    name="Databricks",
    description="A toy pipeline that performs arithmetic calculations with a bit of Azure with Databricks.",
)
def calc_pipeline(a="0", b="7", c="17"):
  create_job_result = create_job()
  delete_job(create_job_result.outputs["job_name"])

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")
