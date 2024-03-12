#!/bin/bash

aws secretsmanager create-secret \
    --name db_credentials \
    --secret-string file://./db_credentials.json