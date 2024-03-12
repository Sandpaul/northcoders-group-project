#!/bin/bash

echo "Setting up dw_credentials in AWS Secrets Manager..."

aws secretsmanager create-secret \
    --name dw_credentials \
    --secret-string file://./dw_credentials.json

echo "dw_credentials created."