### Install Python Dependencies

Set up a [virtual environment](https://docs.python.org/3/tutorial/venv.html) for your Kubeflow Pipelines work:

```
python3 -m venv $(pwd)/venv
source ./venv/bin/activate
```

Install the Kubeflow Pipelines sdk, along with other Python dependencies in the [requirements.txt](./requirements.txt) file

```
pip install -r requirements.txt --upgrade
```

