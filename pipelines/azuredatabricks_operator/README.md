### Install Python Dependencies

Set up a [virtual environment](https://docs.python.org/3/tutorial/venv.html) for your Kubeflow 
Pipelines work:

```
python3 -m venv $(pwd)/venv
source ./venv/bin/activate
```

Install the Kubeflow Pipelines sdk, along with other Python dependencies in the [requirements.txt](
    ./requirements.txt) file

```
pip install -r requirements.txt --upgrade
```

### Try Databricks DSL samples

Git clone custom Kubernetes Pipelines SDK https://github.com/magencio/pipelines.git,
databricks-wrapper branch to e.g. /mnt/c/_git/magencio-kubeflow-pipelines.
Then add the SDK to PYTHONPATH:
```
export PYTHONPATH=/mnt/c/_git/magencio-kubeflow-pipelines/sdk/python:$PYTHONPATH
````

Those samples reference 'sparkpi.jar' library. This library can be found here: [Create and run a 
jar job](https://docs.databricks.com/dev-tools/api/latest/examples.html#create-and-run-a-jar-job). 
It can be uploaded to [Databricks File System](
https://docs.microsoft.com/en-us/azure/databricks/data/databricks-file-system) using e.g. [DBFS 
CLI](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-cli#dbfs-cli).