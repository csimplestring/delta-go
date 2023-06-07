#!/usr/bin/env bash

set -euo pipefail

aws configure set aws_access_key_id "AKIAI44QH8DHBEXAMPLE" --profile user2 && aws configure set aws_secret_access_key "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY" --profile user2 && aws configure set region "us-east-1" --profile user2 && aws configure set output "text" --profile user2

aws --endpoint-url=http://localhost:4566 s3 mb s3://golden 

aws --endpoint-url="http://localhost:4566" s3 sync /tmp/localstack/tests-data/golden s3://golden