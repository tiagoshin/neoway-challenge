# Neoway Challenge - by Tiago Shin


### Instructions to run

#### Configuring a google account and project

First of all, we have to use a Google Cloud account. 
You can either reproduce in your own account or use mine. But of course I shouldn't expose my credentials publicly, so I'll send you my serviceaccount.json by email. And then follow the instructions:

- Put the serviceaccount.json into the google_credentials folder.
- In Dockerfile, change the --project parameter from the command: gcloud auth activate-service-account --key-file ./google_credentials/serviceaccount.json --project=<YOUR PROJECT HERE>. I'll send you a project ID as well by email.

#### Configuring the names of the buckets

The names of the buckets in google storage must be unique globally. It means that if you ran this before, you may face issues because some buckets already exists

- In entryfile.sh, change the names of the buckets for unique names, like this:
DATA_BUCKET="<UNIQUE-NAME>"
BIGQUERY_BUCKET="<UNIQUE-NAME>"
SCRIPTS_BUCKET="<UNIQUE-NAME>"
- Then just copy these 3 lines to the second line of the jupyter processes inside the dataproc_scripts folder. Tip: if you have jupyterlab on your machine, please use it. Ideally it should be parameterized into the entryfile.
- Lastly just change the name of the buckets in workflow_dataproc.yaml file for every job, parameter mainPythonFileUri. For example, if the parameter is mainPythonFileUri: gs://<SCRIPT_BUCKET>/fe_customers.py, just change the <SCRIPT_BUCKET> with the same value you defined previously. Ideally it should be parameterized into the entryfile.


#### Running Docker

Ok, now it's everything setup, just make sure you have docker on your machine to build the image and run it. 

- For building the image, from the root project directory:
docker build -t neoway-challenge .

- Just make sure no other process is running with the same name

docker stop neoway-challenge
docker rm neoway-challenge

- Run it and follow the logs. The whole execution should take less than 20 minutes.

docker run --name neoway-challenge neoway-challenge 

#### Making requests to the serving API

As a output of the docker run execution, you'll have a Cloud Run url like this one: https://nc-serving-api-sea7kwf5ma-ue.a.run.app

To check if the endpoint is succefully deployed just curl it:
curl https://nc-serving-api-nx6s6bjuda-ue.a.run.app

If you receive a hello world, proceed.

So you can do a curl put request into this url, with this command:

curl -X 'PUT' \
	<YOUR URL>/evaluate \
	-H 'accept: application/json' \
	-H 'Content-Type: application/json' \
	-d '{"Names":"Cameron Williams", "Age":42.0, "Total_Purchase":11066.8, "Account_Manager":0, "Years":7.22, "Num_Sites":8.0, "Onboard_date":"2013-08-30T07:00:40.000Z", "Location":"10265 Elizabeth Mission Barkerburgh, AK 89518", "Company":"Harvey LLC"}' 

Where <YOUR URL> is just the url that you just created.

Note that you can change the body as long as you respect this data schema:
Names: str
Age: float
Total_Purchase: float
Account_Manager: int
Years: float
Num_Sites: float
Onboard_date: str
Location: str
Company: str

For the Onboard_date, it will accept only dates within the years 2000 to 2050 and it should be exactly like this format: "2013-08-30T07:00:40.000Z"

Note that as Cloud Run is a serverless service, the first request will take longer (abouts 10s). 
Keep doing some requests for warming it up and the response latency will be around 3s


### Concepts

Just explaining a little bit about the solution.

The goals were to:
- Create and train a machine learning model that will classify whether or not a
customer will churn.
-  Deploy your model as a microservice, a REST API that accepts requests containing
a single customer record as a JSON payload and returns a response containing the
model prediction (if the given customer will churn or not).

#### Goal 1: ML model

So for the first goal, I created a batch workflow that:
- Ingest the data from /lnd to /raw in the DATA_BUCKET, in this case just overwriting all the data in parquet format.
- Create features from the /raw data and write it to big query.
- Read the /raw data and the bigquery feature_store data to create a training and test dataframes that are used for finding the best hyperparameters;
- Use the best hyperparameters from the last step to train the data and saved the trained model pipeline. Note that the run executions are into the /run folder.

For achieving that I used:
- Google Storage for keeping the data in a centralized repository.
- Jupyter notebooks for developing with interative code.
- Dataproc for processing with Apache Spark.
- Dataproc Worflows and jobs for schedule and manage dependencies.

#### Goal 2: Serving API

For the second goal, I developed a api that:
- Receives a request with a specified schema;
- Reads bigquery feature_store to get the same features used on the training process.
- Reads the trained pipeline and perform the inference with onnxruntime.
- Returns a dictionary containing the churn information. If {"Churn": 1} the prediction is that the given customer churned, if {"Churn": 0} the customer may not churn.

For achieving that I used:
- Fast API and Python to build the REST api.
- Google Storage for keeping the data in a centralized repository.
- Cloud build for building the docker image and saving it into the google container registry.
- Cloud Run for instancializing a microservice REST API.

### Follow the executions on the Google Cloud Console

One of the coolest things is to follow our executions on the console.
So feel free to navigate through the console, specially checking:
- Google Storage
- Dataproc Workflows
- Cloud Run