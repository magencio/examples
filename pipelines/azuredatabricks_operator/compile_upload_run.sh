#! /bin/bash



export PIPELINE_NAME=databricks-operator_pipeline
export PIPELINE_FILE=databricks-operator_pipeline.py.tar.gz

echo "-> Compile pipeline"
python ./$PIPELINE_NAME.py

echo "-> Upload pipeline"

RESPONSE=`curl -s -X POST -F uploadfile=@$PIPELINE_FILE http://localhost:8080/pipeline/apis/v1beta1/pipelines/upload?name=$PIPELINE_NAME`
export PIPELINE_ID=$(echo $RESPONSE | jq .id -r 2> /dev/null)
if [ "$PIPELINE_ID" == "null" ]; \
then \
  echo -e "\nFailed:\n $RESPONSE\n"; \
else \
  echo "Uploaded with ID $PIPELINE_ID" ; \
fi

echo "-> Run pipeline"

export PIPELINE_RUN_NAME=`date +%s` && \
       PIPELINE_DESCRIPTION='{"description":"","name": "'$PIPELINE_RUN_NAME'","pipeline_spec":{"parameters":[{"name":"a","value":"0"},{"name":"b","value":"7"},{"name":"c","value":"17"}],"pipeline_id":"'$PIPELINE_ID'"},"resource_references":[]}"' && \
        RUN_RESPONSE=`curl 'http://localhost:8080/pipeline/apis/v1beta1/runs' --data "$PIPELINE_DESCRIPTION"` && \
        export PIPELINE_RUN_ID=`echo $RUN_RESPONSE | jq ".run.id" -r`
if [ -z "$PIPELINE_RUN_ID" ]; \
then \
  echo -e "\nFailed to get run id: \n$RUN_RESPONSE"; \
else \
  echo "View run: http://localhost:8080/pipeline/#/runs/details/$PIPELINE_RUN_ID"
fi

read -p "Run started... press any key to continue to delete?:"

echo "-> Delete pipeline"

export PIPELINE_ID=$(curl -s http://localhost:8080/pipeline/apis/v1beta1/pipelines | jq ".pipelines[] | select(.name == \"$PIPELINE_NAME\") | .id" -r) && if [ -z "$PIPELINE_ID" ]; then echo "Pipeline $PIPELINE_NAME not found"; else echo "Deleting pipeline $PIPELINE_NAME" &&  curl -X DELETE http://localhost:8080/pipeline/apis/v1beta1/pipelines/$PIPELINE_ID ; fi # delete