#!/bin/bash

echo "Setting up db_credentials in AWS Secrets Manager..."

aws secretsmanager create-secret \
    --name db_credentials \
    --secret-string file://./db_credentials.json

echo "dw_credentials created."
