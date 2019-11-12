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
import kfp.dsl as dsl
import kfp.compiler as compiler
# Git clone custom Kubernetes Pipelines SDK https://github.com/magencio/pipelines.git, databricks-wrapper branch to 
# e.g. /mnt/c/_git/magencio-kubeflow-pipelines. Then add the SDK to PYTHONPATH: 
# export PYTHONPATH=/mnt/c/_git/magencio-kubeflow-pipelines/sdk/python:$PYTHONPATH
import kfp.dsl.databricks as databricks

_CLUSTER_SPEC= """
{
    "spark_version":"5.3.x-scala2.11",
    "node_type_id": "Standard_D3_v2",
    "spark_conf": {
        "spark.speculation": "true"
    },
    "num_workers": 2
}
"""

def create_cluster(clusterName):
    return databricks.CreateClusterOp(
        name = "createcluster",
        cluster_name = clusterName,
        spec = json.loads(_CLUSTER_SPEC)
    )

def delete_cluster(clusterName):
    return databricks.CreateClusterOp(
        name = "deletecluster",
        cluster_name = clusterName
    )

@dsl.pipeline(
    name="DatabricksCluster",
    description="A toy pipeline that performs arithmetic calculations with a bit of Azure with Databricks.",
)
def calc_pipeline():
    clusterName = "test-cluster"
    create_cluster(clusterName)
    delete_cluster(clusterName)

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")