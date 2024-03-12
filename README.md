# Northcoder Data Engineering Group Project

## Contributors - Team Ness

<p align="center">
 <a href="https://github.com/Sandpaul">Paul Sandford</a> | <a href="https://github.com/ldearlove">Liam Dearlove</a> | <a href="https://github.com/innateterina">Inna Teterina</a> | <a href="https://github.com/muhammad7877">Muhammad Raza</a> | <a href="https://github.com/Irfan6672">Muhammed Irfan</a> | <a href="https://github.com/KiraHeichou">Rahul Aneesh</a>
 </p>
 <p align="center"><img style="margin: auto;" src="team-logo.png" alt="Team Ness logo" width="200">
 </p>

 ## Overview

 This project creates a data platform that extracts data from an operational OLTP (Online Transaction Processing) database - `totesys`, archives it in a data lake and makes it available in a remodelled OLAP (Online Analytical Processing) data warehouse in three overlapping star schemas. The platform is hosted on Amazon Web Services (AWS). 

 ([Click here](https://github.com/northcoders/de-project-specification) for full project specification.)

 - AWS Eventbridge triggers the `extract Lambda` function to extract data from the operational database periodically. AWS Systems Manager Paramater Store is used to store a `last_ingested_timestamp` so that only new entries are extracted. Extracted data is stored as JSON in an AWS s3 bucket.

 - When new data enters the ingestion bucket the `transform Lambda` is triggered. This will extract data from the passed JSON file, convert it to a dataframe, transform it as appropriate and save it in parquet format in another AWS s3 bucket.

 - When new data enters the processed data bucket the `load Lambda` is triggered. This will extract data from the passed parquet file and load it into the data warehouse as appropriate.

 - The extract, transform and load processes are all monitored using AWS CloudWatch.
 
 - The data in each bucket is immutable and protected against being terraform destroyed.

 - AWS infrastructure is built using Terraform as IaC (Infrastructure as Code).

 - CI/CD (continuous integration/continuous deployment) is managed by GitHub Actions.

## Set-up

To deploy this project locally:

1. Run the following command to set up your virtual environment and install required dependencies:

```
make requirements
```

2. Activate the virtual environment:

```
source venv/bin/activate
```

3. Set up the `PYTHONPATH`:

```
export PYTHONPATH=$(pwd)
```

4. Run checks for unit-tests, pep8 compliancy, coverage and security vulnerabilities:

```
make run-checks
```

5. Run Terraform to set up the AWS infrastructure:

```
cd terraform
terraform init
terraform plan
terraform apply -auto-approve
```

(Alternatively, if you store your AWS credentials in GitHub secrets as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` then GitHub Actions will run all checks and deploy the code automatically when you push to the repo.)