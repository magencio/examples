import kfp.dsl as dsl
import kfp.compiler as compiler

def submit_run(run_name):
    return dsl.ResourceOp(
        name="submitrun",
        k8s_resource={
            "apiVersion": "databricks.microsoft.com/v1alpha1",
            "kind": "Run",
            "metadata": {
                "name":run_name,
            },
            "spec":{
                "run_name":run_name,
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
        action="create",
        success_condition="status.metadata.state.life_cycle_state in (TERMINATED, SKIPPED, INTERNAL_ERROR)",
        attribute_outputs={
            "name": "job-{.status.metadata.job_id}-run-{.status.metadata.number_in_job}",
            "run_id": "{.status.metadata.run_id}",
            "run_name": "{.status.metadata.run_name}",
            "life_cycle_state": "{.status.metadata.state.life_cycle_state}",
            "result_state": "{.status.metadata.state.result_state}",
            "notebook_output_result": "{.status.notebook_output.result}",
            "notebook_output_truncated": "{.status.notebook_output.truncated}",
            "error": "{.status.error}"
        }
    )

def delete_run(run_name):
    return dsl.ResourceOp(
        name="deleterun",
        k8s_resource={
            "apiVersion": "databricks.microsoft.com/v1alpha1",
            "kind": "Run",
            "metadata": {
                "name": run_name
            }
        },
        action="delete",
    )

def create_cluster(cluster_name):
    return dsl.ResourceOp(
        name="createcluster",
        k8s_resource={
            "apiVersion": "databricks.microsoft.com/v1alpha1",
            "kind": "Dcluster",
            "metadata": {
                "name":cluster_name,
            },
            "spec":{
                "cluster_name": cluster_name,
                "spark_version":"5.3.x-scala2.11",
                "node_type_id": "Standard_D3_v2",
                "spark_conf": {
                    "spark.speculation": "true"
                },
                "num_workers": 2
            }
        },
        action="create",
        success_condition="status.cluster_info.state in (RUNNING, TERMINATED, UNKNOWN)",
        attribute_outputs={
            "name": "{.metadata.name}",
            "cluster_id": "{.status.cluster_info.cluster_id}",
            "cluster_name": "{.status.cluster_info.cluster_name}",
            "state": "{.status.cluster_info.state}"
        }
    )

def delete_cluster(cluster_name):
    return dsl.ResourceOp(
        name="deletecluster",
        k8s_resource={
            "apiVersion": "databricks.microsoft.com/v1alpha1",
            "kind": "Dcluster",
            "metadata": {
                "name": cluster_name
            }
        },
        action="delete",
    )

def create_job(job_name):
    return dsl.ResourceOp(
        name="createjob",
        k8s_resource={
            "apiVersion": "databricks.microsoft.com/v1alpha1",
            "kind": "Djob",
            "metadata": {
                "name": job_name,
            },
            "spec":{
                "name": job_name,
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
        action="create",
        success_condition="status.job_status.job_id > 0",
        attribute_outputs={
            "name": "{.status.job_status.job_id}",
            "job_id": "{.status.job_status.job_id}",
            "job_name": "{.metadata.name}"
        }
    )

def delete_job(job_name):
    return dsl.ResourceOp(
        name="deletejob",
        k8s_resource={
            "apiVersion": "databricks.microsoft.com/v1alpha1",
            "kind": "Djob",
            "metadata": {
                "name": job_name,
            },
        },
        action="delete",
    )

@dsl.pipeline(
    name="Databricks",
    description="A toy pipeline that manages Databricks resources.",
)
def calc_pipeline(job_name="test-job"):
    create_job_task = create_job(job_name)
    delete_job_task = delete_job(job_name)
    delete_job_task.after(create_job_task)

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")
