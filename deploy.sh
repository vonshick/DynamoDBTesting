sam deploy --template-file infrastracture.json --stack-name data-lake-updater-stack --s3-bucket data-lake-cloudformation --region eu-central-1 --capabilities CAPABILITY_NAMED_IAM 