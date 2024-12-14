# Pinterest Data Pipeline

## Project overview
The project involved the development of a data pipeline to save and analyse the frequency of image uploads by users worldwide to the Pinterest platform. 

To do this, an EC2 instance was created on AWS to use as a Kafka client machine and an API was built to send data to a MSK cluster. From there, the data was then sent via MSK connect so it could be saved in an AWS S3 bucket. The data was then accessed from a Databricks notebook, where it was cleaned and analysed.
To coordinate a streaming schedule, an Airflow DAG was created that triggered a Databricks notebook once per day. The streaming data was sent via AWS Kinesis to Databricks to be cleaned and saved. 

---

## Table of contents


- [Project overview](#project-overview) 
- [Repository files](#repository-files) 
- [Usage instructions](#usage-instructions)
- [License](#license) 

---

## Repository files


- **user_posting_emulation.py**: Python script to simulate data uploads and send Kafka topics.
- **s3_bucket_to_databricks.ipynb**: Databricks notebook for accessing and cleaning data from an S3 bucket.
- **129076a9eaf9_dag.py**: Airflow DAG to schedule data processing.
- **user_posting_emulation_streaming.py**: Python script to send streaming data to Kinesis.
- **dataframes_to_delta_tables.ipynb**: Databricks notebook for cleaning and saving data to Delta tables.

---

## Installation instructions
### Prerequisites
An AWS account with suitable rights is required for this project.
- An AWS account with suitable rights.
- AWS EC2 instance for Kafka client machine.
- AWS MSK cluster and S3 bucket setup.
- Databricks environment for running notebooks.
- Access to AWS Kinesis and IAM roles.

 Once the AWS setup is complete, run the files as follows:
* Download and save Pinterest data to the AWS S3 bucket using user_posting_emulation.py 
* In Databricks, access, clean and analyse the Pinterest data by running s3_bucket_to_databricks.ipynb
* To create a streaming schedule, upload 129076a9eaf9_dag.py to a suitable AWS bucket
* Save Pinterest data to the relevant Kinesis streams using user_posting_emulation_streaming.py
* In Databricks, access and clean the data using dataframes_to_delta_tables.ipynb

---

## Usage instructions
### 1. Batch data

On the AWS platform, navigate to the EC2 console. An EC2 instant should have already been created. Locate the key pair, copy the name of the key pair and copy the entire Value field. Save the Value field in a key pair file locally, ending in .pem. Name the file the same as the key pair.

Open a CLI to initiate the EC2 instance and follow the connect instructions for SSH client on the EC2 console.
Install required packages using:
* sudo yum install java-1.8.0
* wget https://dlcdn.apache.org/kafka/3.0.0/kafka_2.12-2.8.1.tgz
* tar -xzf kafka_2.12-2.8.1.tgz
* cd kafka_2.12-2.8.1
* cd libs
* wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar
* nano ~/.bashrc
* export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar
* source ~/.bashrc
* echo $CLASSPATH

On AWS, navigate to the IAM console and copy the ARN under Roles. Add this ARN under Trust relationships.
On CLI, navigate to the folder bin and add the following to the file client.properties using nano.
* security.protocol = SASL_SSL
* sasl.mechanism = AWS_MSK_IAM
* sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=arn:aws:iam::584739742957:role/129076a9eaf9-ec2-access-role;
* sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler


Under the MSK management console, copy the bootstrap servers string and the plaintext Apache Zookeeper connection string.
In the CLI, create three Kafka topics for pin, geo and user data under bin/kafka-topics.sh.

Download the Confluent.io Amazon S3 Connector and configure to bucket.
Set up the Kafka REST Proxy on your EC2 client machine.
Install the Confluent package for the Kafka REST Proxy on your EC2 client machine, then start the REST proxy.

In Databricks, run the notebook s3_bucket_to_databricks.ipynb to access, clean and analyse the Pinterest data.


### 2. Streamed data
If yours AWS account has not been provided with access to a MWAA environment Databricks-Airflow-env and its S3 bucket, you must create an API token in Databricks to connect to your AWS account, set up the MWAA-Databricks connection and create a requirements.txt file.

Create a DAG file and upload it to the dags folder in your S3 bucket for MWAA. Then manually trigger the DAG and check it runs successfully.

Using Kinesis Data Streams, create three data streams, one for each Pinterest table (pin, geo, user).
Copy the ARN of your Kinesis access role from the IAM console under Roles. This is the ARN you should use when setting up the Execution role for the integration point of all the methods. Your API should be able to invoke the following actions:
* List streams in Kinesis
* Create, describe and delete streams in Kinesis
* Add records to streams in Kinesis

Run the file user_posting_emulation_streaming.py to save the streamed data.
In Databricks, run the file dataframes_to_delta_tables.ipynb to access and clean the data.
Once the streaming data has been cleaned, you should save each stream in a Delta Table.

---

## License

This project is licensed under the GNU GENERAL PUBLIC LICENSE - see the [LICENSE](LICENSE.txt) file for details.

Copyright (c) [2024] [Daniel White]
