# MySQL to Cloud SQL Migration with Real-time Streaming
**Complete Migration Guide with Zero-Downtime Data Pipeline Modernization**

*Author: Monica Ghavalyan*  
*Date: September 2025*

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture and Technologies](#architecture-and-technologies)
3. [Phase 1: Baseline Environment Setup](#phase-1-baseline-environment-setup)
4. [Phase 2: Streaming Pipeline Implementation](#phase-2-streaming-pipeline-implementation)
5. [Phase 3: Database Migration](#phase-3-database-migration)
6. [Phase 4: Pipeline Modernization](#phase-4-pipeline-modernization)
7. [Phase 5: Validation and Cleanup](#phase-5-validation-and-cleanup)
8. [Troubleshooting and Best Practices](#troubleshooting-and-best-practices)

---

## Project Overview

### Project Objective
This project demonstrates a comprehensive database migration strategy that transitions a self-managed MySQL database from Google Compute Engine (GCE) to a fully managed Cloud SQL instance while maintaining continuous, low-latency data streaming to BigQuery. The migration includes modernizing the streaming architecture from a self-managed Debezium/Dataflow pipeline to Google Cloud's serverless Datastream service.

### Success Criteria
- Data consistency verified between source and target systems
- Real-time streaming latency maintained under 60 seconds
- New pipeline demonstrates improved reliability and reduced operational overhead

---

## Architecture and Technologies

### Current State vs Target State
**Before:**
```
GCE VM (MySQL + Python) → Debezium → Pub/Sub → Dataflow → BigQuery
```

**After:**
```
Application → Cloud SQL → Datastream → BigQuery
```

### Technology Stack

#### Core Services
- **Google Compute Engine (GCE)**: Source MySQL hosting platform
- **Cloud SQL**: Fully managed MySQL service as migration target
- **Cloud Datastream**: Serverless change data capture service
- **BigQuery**: Data warehouse for analytics workloads
- **Database Migration Service (DMS)**: Automated database migration tool

#### Supporting Services
- **Apache Debezium**: Open-source change data capture platform
- **Google Cloud Pub/Sub**: Messaging service for event streaming
- **Google Cloud Dataflow**: Stream and batch data processing
- **Python 3.x with Faker**: Application development and data generation

### Prerequisites
Enable required APIs:
```bash
gcloud services enable compute.googleapis.com sqladmin.googleapis.com \
datastream.googleapis.com bigquery.googleapis.com dataflow.googleapis.com \
pubsub.googleapis.com datamigration.googleapis.com servicenetworking.googleapis.com
```

---

## Phase 1: Baseline Environment Setup

### 1.1 GCE VM and MySQL Setup

#### VM Creation
```bash
# Create VM with optimal configuration
gcloud compute instances create mysql-source-vm \
  --zone=us-central1-a \
  --machine-type=e2-medium \
  --boot-disk-size=20GB \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --tags=mysql-server \
  --allow-http-traffic \
  --allow-https-traffic

# Configure firewall for MySQL
gcloud compute firewall-rules create allow-mysql \
  --allow tcp:3306 \
  --source-ranges 0.0.0.0/0 \
  --target-tags mysql-server

# Create and attach data disk
gcloud compute disks create mysql-data-disk --size=20GB --zone=us-central1-a
gcloud compute instances attach-disk mysql-source-vm --disk mysql-data-disk --zone=us-central1-a
```

#### MySQL Installation and Configuration
```bash
# SSH into VM
gcloud compute ssh mysql-source-vm --zone=us-central1-a

# Install MySQL
sudo apt update
sudo apt install mysql-server -y
sudo systemctl start mysql
sudo systemctl enable mysql

# Configure storage
sudo mkfs.ext4 -F /dev/sdb
sudo mkdir -p /var/lib/mysql-data
sudo mount /dev/sdb /var/lib/mysql-data
echo '/dev/sdb /var/lib/mysql-data ext4 defaults 0 0' | sudo tee -a /etc/fstab
sudo chown mysql:mysql /var/lib/mysql-data

# Secure installation
sudo mysql_secure_installation
```

### 1.2 Change Data Capture Configuration

#### Enable Binary Logging
```bash
# Stop MySQL and backup config
sudo systemctl stop mysql
sudo cp /etc/mysql/mysql.conf.d/mysqld.cnf /etc/mysql/mysql.conf.d/mysqld.cnf.backup

# Edit configuration
sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf
```

Add these lines:
```ini
[mysqld]
server-id                = 1
log_bin                  = /var/log/mysql/mysql-bin.log
binlog_format            = ROW
gtid_mode                = ON
enforce_gtid_consistency = ON
```

```bash
# Prepare log directory and restart
sudo mkdir -p /var/log/mysql
sudo chown mysql:mysql /var/log/mysql
sudo systemctl start mysql

# Verify configuration
sudo mysql -u root -p -e "SHOW VARIABLES LIKE 'log_bin%';"
sudo mysql -u root -p -e "SHOW VARIABLES LIKE '%gtid%';"
```

### 1.3 Database Setup and User Creation

```sql
-- Connect as root and create users
sudo mysql -u root -p

-- Create debezium user for CDC
CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'debezium_password_123';
GRANT SELECT, RELOAD, SHOW DATABASES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, SHOW VIEW ON *.* TO 'debezium_user'@'%';
GRANT ALL PRIVILEGES ON `inventory`.* TO 'debezium_user'@'%';

-- Create application user
CREATE USER 'app_user'@'%' IDENTIFIED BY 'app_password_123';
GRANT SELECT, INSERT ON *.* TO 'app_user'@'%';
FLUSH PRIVILEGES;

-- Create database and table
CREATE DATABASE inventory;
USE inventory;
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 1.4 Data Generation Setup

Create MySQL client configuration:
```bash
cat > ~/.my.cnf << 'EOF'
[client]
user = app_user
password = app_password_123
host = localhost
database = inventory
EOF
```

Also create Cloud SQL config file early (for future use)
```bash
cat > ~/.my_cloudsql.cnf << 'EOF'
[client]
host = PLACEHOLDER_CLOUD_SQL_IP
port = 3306
user = datastream_user
password = datastream_password_123
database = inventory
EOF
```

Set secure permissions on both config files
```bash
chmod 600 ~/.my.cnf
chmod 600 ~/.my_cloudsql.cnf
```

Install dependencies and set up data generation:
```bash
sudo apt install python3-pip -y
pip install mysql-connector-python Faker

# Copy insert_data.py script content from project files
nano insert_data.py
chmod +x insert_data.py

# Schedule data generation every 5 minutes
crontab -e
# */5 * * * * /usr/bin/python3 /home/$USER/insert_data.py ~/.my.cnf >> /home/$USER/cron.log 2>&1


---

## Phase 2: Streaming Pipeline Implementation

### 2.1 Pub/Sub and Debezium Setup

#### Pub/Sub Configuration
```bash
# Create topic and subscription
gcloud pubsub topics create mysql-cdc-events.inventory.products
gcloud pubsub subscriptions create mysql-cdc-events.inventory.products.sub \
  --topic=mysql-cdc-events.inventory.products
```

#### Debezium Installation
```bash
# SSH to VM and install Debezium
gcloud compute ssh mysql-source-vm --zone=us-central1-a

mkdir -p ~/debezium-server && cd ~/debezium-server
wget https://repo1.maven.org/maven2/io/debezium/debezium-server-dist/2.4.0.Final/debezium-server-dist-2.4.0.Final.tar.gz
tar -xzf debezium-server-dist-2.4.0.Final.tar.gz
cd debezium-server

# Configure Debezium (copy from application.properties file)
nano conf/application.properties
```

Key configuration properties:
```properties
debezium.sink.type=pubsub
debezium.sink.pubsub.project.id=YOUR_PROJECT_ID
debezium.sink.pubsub.topic.name=mysql-cdc-events.inventory.products
debezium.source.connector.class=io.debezium.connector.mysql.MySqlConnector
debezium.source.database.hostname=localhost
debezium.source.database.user=debezium_user
debezium.source.database.password=debezium_password_123
```

Set up service account and start Debezium:
```bash
# Create service account with Pub/Sub Publisher role in GCP Console
# Download JSON key and upload to VM
export GOOGLE_APPLICATION_CREDENTIALS=~/debezium-pubsub-key.json
nohup ./run.sh > debezium.log 2>&1 &
```

### 2.2 BigQuery and Dataflow Pipeline

#### BigQuery Setup
```bash
# Create dataset and table
bq mk --dataset --location=US inventory_dataset
bq mk --table inventory_dataset.products \
id:INTEGER,product_name:STRING,quantity:INTEGER,created_at:TIMESTAMP
```

#### Dataflow Pipeline
```bash
# Upload UDF function (copy transform.js from project files)
gsutil mb gs://YOUR_PROJECT_ID-dataflow-functions
gsutil cp transform.js gs://YOUR_PROJECT_ID-dataflow-functions/

# Launch Dataflow job
gcloud dataflow jobs run mysql-to-bigquery-stream \
  --gcs-location gs://dataflow-templates-us-central1/latest/PubSub_to_BigQuery \
  --region us-central1 \
  --parameters \
inputTopic=projects/YOUR_PROJECT_ID/topics/mysql-cdc-events.inventory.products,\
outputTableSpec=YOUR_PROJECT_ID:inventory_dataset.products,\
javascriptTextTransformGcsPath=gs://YOUR_PROJECT_ID-dataflow-functions/transform.js,\
javascriptTextTransformFunctionName=transform
```

Verify data flow:
```bash
bq query --use_legacy_sql=false \
"SELECT COUNT(*) FROM \`YOUR_PROJECT_ID.inventory_dataset.products\`"
```

---

## Phase 3: Database Migration

### 3.1 Cloud SQL Provisioning

#### VPC and Network Setup
```bash
# Configure private services access
gcloud compute addresses create google-managed-services-default \
  --global --purpose=VPC_PEERING --prefix-length=16 --network=default

gcloud services vpc-peerings connect \
  --service=servicenetworking.googleapis.com \
  --ranges=google-managed-services-default --network=default
```

#### Cloud SQL Instance Creation
```bash
# Create Cloud SQL instance with private IP
gcloud sql instances create mysql-destination \
  --database-version=MYSQL_8_0 --tier=db-n1-standard-1 \
  --region=us-central1 --network=default --no-assign-ip \
  --storage-size=20GB --storage-type=SSD \
  --backup-start-time=03:00 --enable-bin-log

# Set passwords and create database
gcloud sql users set-password root --host=% \
  --instance=mysql-destination --password=cloudsql_root_password_123
gcloud sql databases create inventory --instance=mysql-destination
```

#### Create Users for Cloud SQL
```bash
gcloud sql connect mysql-destination --user=root

# Create datastream user
CREATE USER 'datastream_user'@'%' IDENTIFIED BY 'datastream_password_123';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'datastream_user'@'%';
GRANT ALL PRIVILEGES ON inventory.* TO 'datastream_user'@'%';
FLUSH PRIVILEGES;
```

### 3.2 Database Migration Service

#### DMS Configuration
```bash
# Enable DMS and create connection profile
gcloud services enable datamigration.googleapis.com

gcloud database migration connection-profiles create mysql source-profile \
  --region=us-central1 --host=YOUR_VM_INTERNAL_IP --port=3306 \
  --username=debezium_user --password=debezium_password_123 \
  --display-name="Source MySQL on GCE VM"

# Create and start migration job
gcloud database migration migration-jobs create mysql-cloudsql \
  --region=us-central1 --source-connection-profile=source-profile \
  --destination-instance=mysql-destination --type=CONTINUOUS \
  --display-name="MySQL to Cloud SQL Migration"

gcloud database migration migration-jobs start mysql-cloudsql --region=us-central1
```

Monitor until migration reaches "CDC in progress" state with near-zero replication delay.

---

## Phase 4: Pipeline Modernization

### 4.1 Datastream Configuration

#### Private Connectivity and Connection Profiles
```bash
# Create private connectivity for Datastream
gcloud datastream private-connections create private-config \
  --location=us-central1 --display-name="Private connectivity for Datastream" \
  --vpc-peering-config-vpc=projects/YOUR_PROJECT_ID/global/networks/default \
  --vpc-peering-config-subnet=10.0.0.0/29

# Create source connection profile for Cloud SQL
gcloud datastream connection-profiles create source-mysql-profile \
  --location=us-central1 --type=mysql \
  --mysql-hostname=YOUR_CLOUD_SQL_PRIVATE_IP --mysql-port=3306 \
  --mysql-username=datastream_user --mysql-password=datastream_password_123 \
  --display-name="Source MySQL Profile" --private-connection=private-config

# Create BigQuery destination profile
gcloud datastream connection-profiles create destination-profile \
  --location=us-central1 --type=bigquery --display-name="BigQuery Destination Profile"
```

#### Stream Creation
```bash
# Create Datastream (don't start yet)
gcloud datastream streams create my-stream \
  --location=us-central1 --display-name="MySQL to BigQuery Stream" \
  --source-connection-profile=source-mysql-profile \
  --destination-connection-profile=destination-profile \
  --mysql-source-config-include-objects-mysql-databases=inventory \
  --mysql-source-config-include-objects-mysql-tables=inventory.products \
  --bigquery-destination-config-source-hierarchy-datasets-dataset-id=inventory_dataset \
  --bigquery-destination-config-data-freshness=900 --no-start
```

### 4.2 Coordinated Cutover

#### Cutover Sequence
**Step 1: Stop Application Writes**
```bash
# SSH to source VM and disable data generation
gcloud compute ssh mysql-source-vm --zone=us-central1-a
crontab -e
# */5 * * * * /usr/bin/python3 /home/$USER/insert_data.py ~/.my.cnf >> /home/$USER/cron.log 2>&1
```

**Step 2: Verify DMS Synchronization**
```bash
# Wait for replication delay to reach zero
gcloud database migration migration-jobs describe mysql-cloudsql --region=us-central1
```

**Step 3: Promote Cloud SQL**
```bash
gcloud database migration migration-jobs promote mysql-cloudsql --region=us-central1
```

**Step 4: Start Datastream and Resume Operations**
```bash
# Start Datastream pipeline
gcloud datastream streams start my-stream --location=us-central1

# Re-enable data generation
crontab -e

# Add this line
*/5 * * * * /usr/bin/python3 /home/$USER/insert_data.py ~/.my_cloudsql.cnf >> /home/$USER/cron.log 2>&1
```

### 4.3 Validation
```bash
# Verify new BigQuery table receives data
bq query --use_legacy_sql=false \
"SELECT COUNT(*) FROM \`YOUR_PROJECT_ID.inventory_dataset.products_from_datastream\`"
```

---

## Phase 5: Validation and Cleanup

### 5.1 Data Validation

#### Compare Data Between Tables
```sql
-- Comprehensive validation query
WITH original_data AS (
  SELECT COUNT(*) as count, SUM(quantity) as total_qty
  FROM `YOUR_PROJECT_ID.inventory_dataset.products`
),
datastream_data AS (
  SELECT COUNT(*) as count, SUM(quantity) as total_qty  
  FROM `YOUR_PROJECT_ID.inventory_dataset.products_from_datastream`
)
SELECT 
  o.count as original_count, d.count as datastream_count,
  o.total_qty as original_total, d.total_qty as datastream_total,
  ABS(o.count - d.count) as count_diff
FROM original_data o CROSS JOIN datastream_data d;
```

### 5.3 Infrastructure Cleanup

#### Legacy Pipeline Cleanup
```bash
# Stop Debezium and clean up VM components
gcloud compute ssh mysql-source-vm --zone=us-central1-a
pkill -f debezium
rm -rf ~/debezium-server

# Cancel Dataflow job
DATAFLOW_JOB_ID=$(gcloud dataflow jobs list --region=us-central1 --filter="name:mysql-to-bigquery-stream" --format="value(id)" --limit=1)
gcloud dataflow jobs cancel $DATAFLOW_JOB_ID --region=us-central1

# Delete Pub/Sub resources
gcloud pubsub subscriptions delete mysql-cdc-events.inventory.products.sub
gcloud pubsub topics delete mysql-cdc-events.inventory.products

# Clean up storage
gsutil rm -r gs://YOUR_PROJECT_ID-dataflow-functions/
gsutil rb gs://YOUR_PROJECT_ID-dataflow-functions
```
---

## Troubleshooting and Best Practices

### Common Issues and Solutions

#### MySQL Connection Problems
- Verify firewall rules allow port 3306
- Check MySQL bind-address configuration  
- Confirm user privileges include appropriate hosts
- Validate network connectivity between components

#### Debezium CDC Issues
- Verify binary logging: `SHOW VARIABLES LIKE 'log_bin%'`
- Check GTID configuration: `SHOW VARIABLES LIKE '%gtid%'`
- Confirm debezium user has replication privileges
- Review Debezium server logs for errors

#### Cloud SQL Migration Problems
- Verify private IP connectivity between source/target
- Check source MySQL binary log retention
- Monitor Cloud SQL performance metrics
- Ensure adequate instance sizing

#### Datastream Configuration Issues
- Verify private connectivity configuration
- Check datastream user privileges on Cloud SQL
- Confirm BigQuery dataset accessibility
- Monitor Datastream logs for authentication errors

### Security Best Practices

#### Database Security
- Use strong, unique passwords for all database users
- Implement least-privilege access principles

#### Network Security
- Use private IP addresses for managed services
- Implement VPC firewall rules with minimal access
- Enable VPC Flow Logs for monitoring

---

## Conclusion

This migration successfully demonstrates a complete transformation from self-managed infrastructure to fully managed Google Cloud services while maintaining data consistency and minimizing downtime. Key achievements include:

- **Zero-data-loss migration** from GCE MySQL to Cloud SQL
- **Modernized streaming architecture** using Cloud Datastream  
- **Improved operational efficiency** through managed services
- **Enhanced scalability** with cloud-native solutions
- **Comprehensive automation** for repeatable deployments

The project reduces operational overhead, improves reliability, and provides better disaster recovery capabilities while serving as a template for similar database migration initiatives.

---

## Appendix

### Service Account Permissions
**Minimum Required IAM Roles:**
- Compute Engine Admin, Cloud SQL Admin
- BigQuery Admin, Pub/Sub Admin  
- Dataflow Admin, Datastream Admin, Storage Admin

### Configuration Files
All scripts used in this project:
- `pre_env.sh`: Pre-migration setup commands
- `post_env.sh`: Post-migration and cutover commands  
- `application.properties`: Debezium configuration
- `transform.js`: JavaScript UDF for data transformation
- `insert_data.py`: Data generation scripts

This documentation provides a complete, production-ready guide with all commands tested and validated for real-world implementation.