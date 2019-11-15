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

import json
import kfp
import kfp.dsl as dsl
import kfp.compiler as compiler
# Git clone custom Kubernetes Pipelines SDK https://github.com/magencio/pipelines.git, databricks-wrapper branch to 
# e.g. /mnt/c/_git/magencio-kubeflow-pipelines. Then add the SDK to PYTHONPATH: 
# export PYTHONPATH=/mnt/c/_git/magencio-kubeflow-pipelines/sdk/python:$PYTHONPATH
import kfp.dsl.databricks as databricks

def submit_run(run_name):
    return databricks.SubmitRunOp(
        name = "submitrun",
        run_name = run_name,
        new_cluster = {
            "spark_version":"5.3.x-scala2.11",
            "node_type_id": "Standard_D3_v2",
            "num_workers": 2
        },
        libraries = [
            {
                "jar": "dbfs:/my-jar.jar"
            },
            {
                "maven": {
                    "coordinates": "org.jsoup:jsoup:1.7.2"
                }
            }
        ],
        spark_jar_task = {
            "main_class_name": "com.databricks.ComputeModels"
        }
    )

def echo(input):
    print(input)

echo_op = kfp.components.func_to_container_op(echo)

@dsl.pipeline(
    name="DatabricksRun",
    description="A toy pipeline that performs arithmetic calculations with a bit of Azure with Databricks.",
)
def calc_pipeline(run_name="test-run"):
    submit_run_task = submit_run(run_name)
    echo_task = echo_op(submit_run_task.outputs["result"])

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")