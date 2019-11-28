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

def submit_run(cluster_id, run_name, parameter):
    # Sample based on [Create a spark-submit job](https://docs.databricks.com/dev-tools/api/latest/examples.html#create-and-run-a-jar-job)
    # Additional info:
    #   - [Databricks File System](https://docs.microsoft.com/en-us/azure/databricks/data/databricks-file-system)
    #   - [DBFS CLI](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-cli#dbfs-cli)
    return databricks.SubmitRunOp(
        name="submitrun",
        run_name=run_name,
        existing_cluster_id=cluster_id,
        libraries=[{"jar": "dbfs:/docs/sparkpi.jar"}],
        spark_jar_task={
            "main_class_name": "org.apache.spark.examples.SparkPi",
            "parameters": [parameter]
        }
    )

def delete_run(run_name):
    return databricks.DeleteRunOp(
        name="deleterun",
        run_name=run_name
    )

def delete_cluster(cluster_name):
    return databricks.DeleteClusterOp(
        name="deletecluster",
        cluster_name=cluster_name
    )

@dsl.pipeline(
    name="DatabricksCluster",
    description="A toy pipeline that computes an approximation to pi with Azure Databricks."
)
def calc_pipeline(cluster_name="test-cluster", run_name="test-run", parameter="10"):
    create_cluster_task = create_cluster(cluster_name)
    submit_run_task = submit_run(create_cluster_task.outputs["cluster_id"], run_name, parameter)
    delete_run_task = delete_run(run_name)
    delete_run_task.after(submit_run_task)
    delete_cluster_task = delete_cluster(cluster_name)
    delete_cluster_task.after(delete_run_task)

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")
