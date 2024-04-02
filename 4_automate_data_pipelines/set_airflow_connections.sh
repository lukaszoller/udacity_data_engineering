# Create AWS credentials connection
echo "Creating AWS credentials connection"
airflow connections add aws_credentials \
    --conn-type 'aws' \
    --conn-login 'AKIAQ6K5NW4FHUL3C44H' \
    --conn-password 'tH/YNpfJOwMm0yHGfzIxPsFLYN2hOcQCoyWT6A4W' \
    --conn-uri 'aws://AKIAQ6K5NW4FHUL3C44H:tH/YNpfJOwMm0yHGfzIxPsFLYN2hOcQCoyWT6A4W'

# Create Redshift connection
echo "Creating Redshift connection"
airflow connections add redshift \
    --conn-type 'redshift' \
    --conn-login 'awsuser' \
    --conn-password 'redshiftPassword1' \
    --conn-host 'default-workgroup.065157314314.us-east-1.redshift-serverless.amazonaws.com' \
    --conn-schema 'dev' \
    --conn-port '5439' \
    --conn-uri 'redshift://awsuser:redshiftPassword1@default-workgroup.065157314314.us-east-1.redshift-serverless.amazonaws.com:5439/dev'

# Uncomment and modify the following lines as needed
echo "Setting S3 bucket variable"
airflow variables set s3_bucket borti

read -p "Press Enter to exit..."

# Create AWS credentials connection
echo "Creating AWS credentials connection"
airflow connections add aws_credentials \
    --conn-uri 'aws://AKIAQ6K5NW4FHUL3C44H:tH/YNpfJOwMm0yHGfzIxPsFLYN2hOcQCoyWT6A4W'

# Create Redshift connection
echo "Creating Redshift connection"
airflow connections add redshift \
    --conn-uri 'redshift://awsuser:redshiftPassword1@default-workgroup.065157314314.us-east-1.redshift-serverless.amazonaws.com:5439/dev'

# Uncomment and modify the following lines as needed
echo "Setting S3 bucket variable"
airflow variables set s3_bucket borti

read -p "Press Enter to exit..."
