# Beginner Exercises
## Exercise 1:
Publish a message to a Pub/Sub topic using the Cloud Console.

Solution:

1. Go to the Cloud Console and navigate to the Pub/Sub section.

2. Click on the topic you want to publish a message to.

3. Click on the "Publish message" button.

4. Enter the message you want to publish in the "Message data" field.

5. Click on the "Publish" button to publish the message to the topic.

## Exercise 2:
Create a subscription to a Pub/Sub topic using the Cloud Console.

Solution:

1. Go to the Cloud Console and navigate to the Pub/Sub section.

2. Click on the topic you want to create a subscription to.

3. Click on the "Create subscription" button.

4. Enter a name for the subscription.

5. Choose a delivery type (pull or push).

6. If you choose push delivery type, enter the endpoint URL where the messages will be delivered.

7. Click on the "Create subscription" button to create the subscription.

## Exercise 3:
View messages in a Pub/Sub subscription using the Cloud Console.

Solution:

1. Go to the Cloud Console and navigate to the Pub/Sub section.

2. Click on the subscription you want to view messages for.

3. Click on the "Pull" button to pull messages from the subscription.

4. If there are messages in the subscription, they will be displayed in the "Received messages" section.

5. If there are no messages in the subscription, the "Received messages" section will be empty.

6. You can also view message details by clicking on the message in the "Received messages" section.

## Exercise 4:
Delete a Pub/Sub topic using the Cloud Console.

Solution:

1. Go to the Cloud Console and navigate to the Pub/Sub section.

2. Click on the topic you want to delete.

3. Click on the "Delete" button.

4. Confirm the deletion by clicking on the "Delete" button again.


## Exercise 5:
Create a Pub/Sub topic using the gcloud command line tool.

Solution:

1. Open the terminal or command prompt.

2. Enter the following command to create a new Pub/Sub topic:

```
gcloud pubsub topics create <topic_name>
```

3. Replace `<topic_name>` with the name you want to give to the new topic.

4. Press Enter to execute the command.

## Exercise 6:
Publish a message to a Pub/Sub topic using the gcloud command line tool.

Solution:

1. Open the terminal or command prompt.

2. Enter the following command to publish a message to a Pub/Sub topic:

```
gcloud pubsub topics publish <topic_name> --message <message_data>
```

3. Replace `<topic_name>` with the name of the topic you want to publish the message to.

4. Replace `<message_data>` with the message you want to publish.

5. Press Enter to execute the command.

## Exercise 6:
Create a subscription to a Pub/Sub topic using the gcloud command line tool.

Solution:

1. Open the terminal or command prompt.

2. Enter the following command to create a new subscription:

```
gcloud pubsub subscriptions create <subscription_name> --topic <topic_name>
```

3. Replace `<subscription_name>` with the name you want to give to the new subscription.

4. Replace `<topic_name>` with the name of the topic you want to create the subscription for.

5. Press Enter to execute the command.

## Exercise 7:
Pull messages from a Pub/Sub subscription using the gcloud command line tool.

Solution:

1. Open the terminal or command prompt.

2. Enter the following command to pull messages from a subscription:

```
gcloud pubsub subscriptions pull <subscription_name> --auto-ack
```

3. Replace `<subscription_name>` with the name of the subscription you want to pull messages from.

4. Press Enter to execute the command.

5. If there are messages in the subscription, they will be displayed in the terminal or command prompt window.

6. If you want to acknowledge the messages after pulling them, remove the `--auto-ack` flag from the command and press Enter.

# Advanced Exercises
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

