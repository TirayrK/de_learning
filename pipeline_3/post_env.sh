# Task 4

# 4.1

# Enable required APIs for Cloud SQL and VPC peering
gcloud services enable sqladmin.googleapis.com
gcloud services enable servicenetworking.googleapis.com

# Reserve an IP range for private services access
gcloud compute addresses create google-managed-services-default \
  --global \
  --purpose=VPC_PEERING \
  --prefix-length=16 \
  --network=default

# Create private connection for Cloud SQL
gcloud services vpc-peerings connect \
  --service=servicenetworking.googleapis.com \
  --ranges=google-managed-services-default \
  --network=default

# Create Cloud SQL instance with private IP (adjust MySQL version to match source)
gcloud sql instances create mysql-destination \
  --database-version=MYSQL_8_0 \
  --tier=db-n1-standard-1 \
  --region=us-central1 \
  --network=default \
  --no-assign-ip \
  --storage-size=20GB \
  --storage-type=SSD \
  --backup-start-time=03:00 \
  --enable-bin-log

# Set root password for Cloud SQL instance
gcloud sql users set-password root \
  --host=% \
  --instance=mysql-destination \
  --password=cloudsql_root_password_123

# Create the inventory database on Cloud SQL
gcloud sql databases create inventory --instance=mysql-destination

# Get the private IP address of your Cloud SQL instance
gcloud sql instances describe mysql-destination --format="value(ipAddresses[0].ipAddress)"

# RUN in VM SSH

# Test connection from your VM to Cloud SQL (use the private IP from above)
mysql -h PRIVATE_IP_ADDRESS -u root -p -e "SHOW DATABASES;"

# RUN from local terminal or Cloud Shell

# Connect to Cloud SQL instance to create datastream user
gcloud sql connect mysql-destination --user=root

-- Create datastream user for Cloud Datastream service
CREATE USER 'datastream_user'@'%' IDENTIFIED BY 'datastream_password_123';

-- Grant necessary privileges for Datastream (similar to Debezium but for Datastream)
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'datastream_user'@'%';

-- Grant full access to inventory database
GRANT ALL PRIVILEGES ON inventory.* TO 'datastream_user'@'%';

-- Apply privilege changes
FLUSH PRIVILEGES;

-- Verify user creation
SELECT user, host FROM mysql.user WHERE user = 'datastream_user';

-- Show grants for datastream user
SHOW GRANTS FOR 'datastream_user'@'%';

-- Exit MySQL
EXIT;

# RUN in VM SSH

# Test datastream user connection from your VM (use the private IP)
mysql -h PRIVATE_IP_ADDRESS -u datastream_user -p -e "SHOW DATABASES;"

# Verify replication privileges work
mysql -h PRIVATE_IP_ADDRESS -u datastream_user -p -e "SHOW MASTER STATUS;"


# 4.2

# Enable Database Migration Service API
gcloud services enable datamigration.googleapis.com

# Create source connection profile for GCE MySQL
gcloud database migration connection-profiles create mysql source-profile \
  --region=us-central1 \
  --host=YOUR_VM_INTERNAL_IP \
  --port=3306 \
  --username=debezium_user \
  --password=debezium_password_123 \
  --display-name="Source MySQL on GCE VM"

# Test the source connection profile
gcloud database migration connection-profiles describe source-profile \
  --region=us-central1

# Create continuous migration job from GCE MySQL to Cloud SQL
gcloud database migration migration-jobs create mysql-cloudsql \
  --region=us-central1 \
  --source-connection-profile=source-profile \
  --destination-instance=mysql-destination \
  --type=CONTINUOUS \
  --display-name="MySQL to Cloud SQL Migration"

# Task 5

# 5.1

# Create private connectivity configuration for Datastream
gcloud datastream private-connections create private-config \
  --location=us-central1 \
  --display-name="Private connectivity for Datastream" \
  --vpc-peering-config-vpc=projects/YOUR_PROJECT_ID/global/networks/default \
  --vpc-peering-config-subnet=10.0.0.0/29

# Create Datastream source connection profile for Cloud SQL
gcloud datastream connection-profiles create source-mysql-profile \
  --location=us-central1 \
  --type=mysql \
  --mysql-hostname=YOUR_CLOUD_SQL_PRIVATE_IP \
  --mysql-port=3306 \
  --mysql-username=datastream_user \
  --mysql-password=datastream_password_123 \
  --display-name="Source MySQL Profile" \
  --private-connection=private-config

# Create Datastream destination connection profile for BigQuery
gcloud datastream connection-profiles create destination-profile \
  --location=us-central1 \
  --type=bigquery \
  --display-name="BigQuery Destination Profile"

# Create Datastream (but don't start - notice --no-start flag)
gcloud datastream streams create my-stream \
  --location=us-central1 \
  --display-name="MySQL to BigQuery Stream" \
  --source-connection-profile=source-mysql-profile \
  --destination-connection-profile=destination-profile \
  --mysql-source-config-include-objects-mysql-databases=inventory \
  --mysql-source-config-include-objects-mysql-tables=inventory.products \
  --bigquery-destination-config-source-hierarchy-datasets-dataset-id=inventory_dataset \
  --bigquery-destination-config-data-freshness=900 \
  --no-start

# For making changes in insert_data.py in future

Run in VM SSH
# Create a separate config file for Cloud SQL connection
nano ~/.my_cloudsql.cnf

[client]
host = 10.24.80.7
port = 3306
user = datastream
password = your_actual_password
database = inventory

chmod 600 ~/.my_cloudsql.cnf


# Final steps to run the project

# Start the DMS migration job (local terminal)
gcloud database migration migration-jobs start mysql-cloudsql \
  --region=us-central1

# Monitor migration progress (local terminal)
gcloud database migration migration-jobs describe mysql-cloudsql \
  --region=us-central1

# Stop the cron job by commenting it out (VM SSH)
crontab -e
# Comment out the line like this:
# */5 * * * * /usr/bin/python3 /home/$USER/insert_data.py >> /home/$USER/cron.log 2>&1

# Promote Cloud SQL instance (stops replication) (local terminal)
gcloud database migration migration-jobs promote mysql-cloudsql \
  --region=us-central1

# Copy code from data_insert_cloudsql.py in project files (VM SSH)
nano ~/insert_data.py
# Replace content with Cloud SQL connection details

# Start the Datastream pipeline (local terminal)
gcloud datastream streams start my-stream --location=us-central1

# Monitor Datastream status (local terminal)
gcloud datastream streams describe my-stream --location=us-central1

# Re-enable the cron job (VM SSH)
crontab -e
# Uncomment the line:
# */5 * * * * /usr/bin/python3 /home/$USER/insert_data.py >> /home/$USER/cron.log 2>&1

# Verify data flows to BigQuery through Datastream (local terminal)
bq query --use_legacy_sql=false \
"SELECT COUNT(*) FROM \`YOUR_PROJECT_ID.inventory_dataset.products_from_datastream\`"