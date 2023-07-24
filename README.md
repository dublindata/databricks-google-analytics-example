# Azure Databricks Google Analytics Example

A repository showing different implementation methods for retrieving the Google Analytics through Databricks notebooks, leveraging Workflows and Delta Live Tables.

## What's in this repository?

This code repository contains artifacts that can help you visualize what it will be like to ingest data from your Google Analytics instance(s) to your Lakehouse via Azure Databricks. Using technologies like Databricks Workflows to orchestrate your data ingestion via a pull schedule or using Delta Live Tables to process data coming in as a stream to an Event Hub or to your cloud storage account using Autoloader.

Here's a breakdown of what you'll find in this repository:

* send-events.py - A sample Python application designed to connect to your Google Analytics API and place data both in your Event Hub and Storage Account
* DLT - A folder containing sample Delta Live Table examples that leverage both Event Hub and Azure Storage for ingesting messages from the the sample application
* Workflow - A folder containing a sample notebook and workflow definition to connect to your Google Analytics API to pull data and store it in your Lakehouse

## Requirements

To successfully run this demo you'll need to make sure you have the following:

* An Azure Databricks workspace
* An Azure Storage Account
* An Azure Event Hub
* A Google Analytics Account

In addition, your local development station (or wherever you plan to run the Python application) needs the following Python packages installed:

* azure-eventhub (https://pypi.org/project/azure-eventhub/)
* azure-storage-blob (https://pypi.org/project/azure-storage-blob/2.1.0/)
* google-analytics-data /(https://pypi.org/project/google-analytics-data/)

You can either install these in your global package repository aor ```.venv``` via pip

## Running The Demo

To run these examples, please follow the below steps

### Getting your Google Analytics Credentials

First, you'll need your Google API credentials. There are many ways to authenticate to the service, however this demo assumes you have downloaded your credentials file. For more information, please see the following article (via Google): https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart-client-libraries

Save the credentials.json file in the folder with the code. **DO NOT ADD THIS FILE TO SOURCE CONTROL!**

### Setting Environment Variables

Next, the sample producer (send-events.py) requires you to set the following environment variables. Depending on your operating system, the commands to set them may vary, but on Linux/Mac, you can run the following commands while substituting your own values:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="<the FULL PATH to the credentials.json file from the above step, example: /Home/Drew/credentials.json>"
export STORAGE_ACCOUNT_CONNECTION_STRING="<the storage account connection string from your Azure storage account you want to use to save the indiviual messages for the autoloader example. Starts with 'DefaultEndpointsProtocol='>"
export STORAGE_CONTAINER_NAME="<the name of your storage container in your storage account>"
export EVENT_HUB_NAME="<the short name of your event hub>"
export EVENT_HUB_CONNECTION_STR="<the connection string/access policy to your event hub. Starts with 'Endpoint=sb://'>"
export G4A_PROPERTY_ID="<your G4A property ID, unique to your account>"
```

### Generating Sample Data

For this demonstration to work, you'll need to have your Google Analytics enabled and collecting data. You should be able to click through your site as the sample code is running to get updated real time report numbers.

### Running the Python Code

Once you have verified you have your Azure Storage Account, Event Hubs, and Azure Databricks Workspace all deployed and the relevant environment variables all set, you can run the Python code with the following command:

```bash
python ./send-events.py -c 0
```

This will start the producer and will run until you press control-c to end the execution. The application is designed to take in multiple configurations and can be run in different terminal/command prompt windows to simulate different report types being queried. The argument -c corresponds to a predefined configuration, stored in the ```config.json``` file. You can view the configuration file for some examples of different report types and dimensions and metrics you can poll. More information about the different report types (realtime vs history) and the various metrics and dimensions can be found here:

* API Dimensions and schemas: https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema
* Realtime Dimensions and schemas: https://developers.google.com/analytics/devguides/reporting/data/v1/realtime-api-schema

### Ingesting The Data Via Databricks

Once you have your producer running, the next step is to ingest the data via Databricks. This repository currently has three examples of how to do this:

* Steaming Ingestion (via the Python sample code)
 * ./dlt/autoloader.ipynb - A Delta Live Tables notebook designed to read the events written to storage by the Python example
 * ./dlt/eventhub.ipynb - A Delta Live Tables notebook designed to read the events to event hub  

* Batch Ingestion
 * ./workflows/readfromg4a.ipynb - A sample notebook designed to be run either via Databricks workflows or interactively to connect to the Google Analytics endpoint, gather the data, and place it into a Spark Dataframe for exploration and/or writing to your Lakehouse


 All of the example notebooks also contain some variables that need set and/or cluster environment variables. Please review each notebook for details before implementing and testing.

## Additional Considerations

The example code in this repository uses basic examples and authentication methods to connect to these various data sources and Azure resources; you should explore other methods of connecting, as well as properly securing your secrets and credentials as part of your production deployment

## Disclaimer

The code provided in this repository is meant to serve as an example implementation ONLY, and should not be considered production ready. Please evaluate and use at your own risk.