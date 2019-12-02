import kfp.dsl as dsl
import kfp.compiler as compiler
import kfp.dsl.databricks as databricks

def create_job(job_name):
    return databricks.CreateJobOp(
        name="createjob",
        job_name=job_name,
        new_cluster={
            "spark_version":"5.3.x-scala2.11",
            "node_type_id": "Standard_D3_v2",
            "num_workers": 2
        },
        libraries=[{"jar": "dbfs:/docs/sparkpi.jar"}],
        spark_jar_task={
            "main_class_name": "org.apache.spark.examples.SparkPi"
        }
    )

def create_run(run_name, job_name, parameter):
    return databricks.SubmitRunOp(
        name="submitrun",
        run_name=run_name,
        job_name=job_name,
        jar_params=[parameter]
    )

def delete_run(run_name):
    return databricks.DeleteRunOp(
        name="deleterun",
        run_name=run_name
    )

def delete_job(job_name):
    return databricks.DeleteJobOp(
        name="deletejob",
        job_name=job_name
    )

@dsl.pipeline(
    name="DatabricksRun",
    description="A toy pipeline that computes an approximation to pi with Azure Databricks."
)
def calc_pipeline(job_name="test-job", run_name="test-job-run", parameter="10"):
    create_job_task = create_job(job_name)
    create_run_task = create_run(run_name, job_name, parameter)
    create_run_task.after(create_job_task)
    delete_run_task = delete_run(run_name)
    delete_run_task.after(create_run_task)
    delete_job_task = delete_job(job_name)
    delete_job_task.after(delete_run_task)

if __name__ == "__main__":
    compiler.Compiler().compile(calc_pipeline, __file__ + ".tar.gz")
