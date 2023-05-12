## Exercise 1:
Send a message containing a weather update for a city to a Pub/Sub topic.

Solution:

1. Create a Pub/Sub topic using the following command: 

```
gcloud pubsub topics create weather_updates
```

2. Send a message to the topic using the following command:

```
gcloud pubsub topics publish weather_updates --message "Current temperature in New York City: 75°F"
```

## Exercise 2:
Create a Cloud Function that logs weather updates received on a Pub/Sub topic.

Solution:

1. Create a Pub/Sub subscription using the following command:

```
gcloud pubsub subscriptions create weather_updates_sub --topic=weather_updates
```

2. Create a new Cloud Function using the following command:

```
gcloud functions deploy weather_updates --runtime python37 --trigger-topic weather_updates_sub --entry-point handler
```

3. In the function code, log incoming messages using the following code:

```python
def handler(data, context):
    print(data)
```

## Exercise 3:
Create a Dataflow pipeline that reads weather updates from a Pub/Sub topic and writes them to a BigQuery table.

Solution:

1. Create a Pub/Sub subscription using the following command:

```
gcloud pubsub subscriptions create weather_updates_sub --topic=weather_updates
```

2. Create a new BigQuery table to store the weather updates.

3. Create a new Dataflow pipeline using the following command:

```
gcloud dataflow jobs run weather_updates_pipeline --gcs-location=gs://dataflow-templates/latest/PubSub_to_BigQuery --region=<region> --staging-location=gs://<bucket>/staging --parameters=inputSubscription=projects/<project-id>/subscriptions/weather_updates_sub,outputTableSpec=<project-id>:<dataset>.<table>
```

4. In the pipeline parameters, replace `<region>`, `<bucket>`, `<project-id>`, `<dataset>`, and `<table>` with the appropriate values for your project and environment.

