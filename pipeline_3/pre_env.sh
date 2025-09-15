# Task 1
# 1.1

# Create VM
gcloud compute instances create mysql-source-vm \
  --zone=us-central1-a \
  --machine-type=e2-medium \
  --boot-disk-size=20GB \
  --boot-disk-type=pd-standard \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --tags=mysql-server \
  --allow-http-traffic \
  --allow-https-traffic

# Firewall rule for MySQL port 3306
gcloud compute firewall-rules create allow-mysql \
  --allow tcp:3306 \
  --source-ranges 0.0.0.0/0 \
  --target-tags mysql-server

# Create and attach data disk
gcloud compute disks create mysql-data-disk \
  --size=20GB \
  --zone=us-central1-a

gcloud compute instances attach-disk mysql-source-vm \
  --disk mysql-data-disk \
  --zone=us-central1-a


# 1.2

# SSH into the VM
gcloud compute ssh mysql-source-vm --zone=us-central1-a

# Update package repositories
sudo apt update

# Install MySQL server package
sudo apt install mysql-server -y

# Start MySQL service
sudo systemctl start mysql

# Check MySQL service status
sudo systemctl status mysql

# Mount and prepare the data disk
sudo mkfs.ext4 -F /dev/sdb
sudo mkdir -p /var/lib/mysql-data
sudo mount /dev/sdb /var/lib/mysql-data
echo '/dev/sdb /var/lib/mysql-data ext4 defaults 0 0' | sudo tee -a /etc/fstab

# Set ownership for MySQL data directory
sudo chown mysql:mysql /var/lib/mysql-data

# Run MySQL secure installation (interactive)
sudo mysql_secure_installation

'''
During mysql_secure_installation, answer:
Set root password: Yes (choose a strong password)
Remove anonymous users: Yes
Disallow root login remotely: Yes
Remove test database: Yes
Reload privilege tables: Yes
'''

# Test MySQL connection
sudo mysql -u root -p

#Inside MySQL, run these SQL commands:
-- Show databases to verify installation
SHOW DATABASES;

-- Exit MySQL
EXIT;


# 1.3

# Stop MySQL service before modifying configuration
sudo systemctl stop mysql

# Create backup of original MySQL configuration
sudo cp /etc/mysql/mysql.conf.d/mysqld.cnf /etc/mysql/mysql.conf.d/mysqld.cnf.backup

# Edit MySQL configuration file to enable binlog and CDC
sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf

#Add these lines under the [mysqld] section:
[mysqld]
server-id                = 1
log_bin                  = /var/log/mysql/mysql-bin.log
binlog_format            = ROW
gtid_mode                = ON
enforce_gtid_consistency = ON
# Save and exit the editor (if using nano: Ctrl+X, Y, Enter)

# Create directory for MySQL logs with proper permissions
sudo mkdir -p /var/log/mysql
sudo chown mysql:mysql /var/log/mysql

# Start MySQL service with new configuration
sudo systemctl start mysql

# Check MySQL service status
sudo systemctl status mysql

# Verify configurations are working
sudo mysql -u root -p -e "SHOW VARIABLES LIKE 'log_bin%';"
sudo mysql -u root -p -e "SHOW VARIABLES LIKE '%gtid%';"


# 1.4

# Connect to MySQL as root to create users
sudo mysql -u root -p

#Inside MySQL, run these SQL commands:
-- Create debezium_user with replication privileges
CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'debezium_password_123';

-- Grant replication and database access privileges to debezium_user
GRANT SELECT, RELOAD, SHOW DATABASES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, SHOW VIEW ON *.* TO 'debezium_user'@'%';

-- Grant full privileges on inventory database to debezium_user
GRANT ALL PRIVILEGES ON `inventory`.* TO 'debezium_user'@'%';

-- Create application user
CREATE USER 'app_user'@'%' IDENTIFIED BY 'app_password_123';

-- Grant SELECT and INSERT privileges to app_user
GRANT SELECT, INSERT ON *.* TO 'app_user'@'%';

-- Apply all privilege changes
FLUSH PRIVILEGES;

-- Verify users were created
SELECT user, host FROM mysql.user WHERE user IN ('debezium_user', 'app_user');

-- Show grants for debezium_user
SHOW GRANTS FOR 'debezium_user'@'%';

-- Show grants for app_user
SHOW GRANTS FOR 'app_user'@'%';

-- Exit MySQL
EXIT;


# Task 2
# 2.1

# Connect to MySQL as root
sudo mysql -u root -p

#Inside MySQL, run these SQL commands:
-- Create the inventory database
CREATE DATABASE inventory;

-- Switch to the inventory database
USE inventory;

-- Create the products table with specified schema
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Verify the table was created correctly
DESCRIBE products;

-- Show the database structure
SHOW TABLES;

-- Exit MySQL
EXIT;


# 2.2
sudo apt install python3-pip -y
pip install mysql-connector-python Faker

# Create the insert_data.py script
nano insert_data.py

# COPY CODE FROM insert_data.py FROM THE PROJECT AND PASTE
# Save and exit the editor (if using nano: Ctrl+X, Y, Enter)

# Create MySQL client configuration file with credentials
cat > ~/.my.cnf << 'EOF'
[client]
user = app_user
password = app_password_123
host = localhost
database = inventory
EOF

# Also create Cloud SQL config file early (for future use)
cat > ~/.my_cloudsql.cnf << 'EOF'
[client]
host = PLACEHOLDER_CLOUD_SQL_IP
port = 3306
user = datastream_user
password = datastream_password_123
database = inventory
EOF

# Set secure permissions on both config files
chmod 600 ~/.my.cnf
chmod 600 ~/.my_cloudsql.cnf

# Test the script with VM config
python3 insert_data.py ~/.my.cnf

# Test the script with default config (should be same result)
python3 insert_data.py

# Verify data was inserted
mysql --defaults-file=~/.my.cnf -e "USE inventory; SELECT COUNT(*) FROM products;"

# 2.3

# Open crontab for editing
crontab -e

# When the editor opens, add this line at the bottom:

# Run insert_data.py every 5 minutes and log output
*/5 * * * * /usr/bin/python3 /home/$USER/insert_data.py ~/.my.cnf >> /home/$USER/cron.log 2>&1

# Save and exit the editor (if using nano: Ctrl+X, Y, Enter)

# Verify the crontab was set correctly
crontab -l

# Check if cron service is running
sudo systemctl status cron

# Wait a few minutes, then check if the cron job is working
tail -f ~/cron.log

# Verify new records are being added every 5 minutes
mysql --defaults-file=~/.my.cnf -e "USE inventory; SELECT COUNT(*) FROM products; SELECT * FROM products ORDER BY created_at DESC LIMIT 10;"


# Task 3
# 3.1

# RUN from local terminal or Cloud Shell
# Create Pub/Sub topic with the specified name
gcloud pubsub topics create mysql-cdc-events.inventory.products

# Create subscription for the topic
gcloud pubsub subscriptions create mysql-cdc-events.inventory.products.sub \
  --topic=mysql-cdc-events.inventory.products

# RUN in VM SSH
# Create directory for Debezium
mkdir -p ~/debezium-server
cd ~/debezium-server

# Download Debezium Server
wget https://repo1.maven.org/maven2/io/debezium/debezium-server-dist/2.4.0.Final/debezium-server-dist-2.4.0.Final.tar.gz

# Extract Debezium
tar -xzf debezium-server-dist-2.4.0.Final.tar.gz
cd debezium-server

# Create configuration file
nano conf/application.properties

# COPY CODE FROM application.properties FROM THE PROJECT AND PASTE
# Save and exit the editor (if using nano: Ctrl+X, Y, Enter)

'''
Go to GCP Console → IAM & Admin → Service Accounts
Create new service account with "Pub/Sub Publisher" role
Generate and download JSON key
Upload it to the VM
'''

# Set the Google Application Credentials environment variable
export GOOGLE_APPLICATION_CREDENTIALS=~/debezium-pubsub-key.json

# Start Debezium server in background with logging
nohup ./run.sh > debezium.log 2>&1 &

# Check if Debezium is running
ps aux | grep debezium


# 3.2

# RUN from local terminal or Cloud Shell

# Create a BigQuery dataset
gcloud config set project YOUR_PROJECT_ID
bq mk --dataset --location=US inventory

# Create the target table with schema matching MySQL products table
bq mk --table \
inventory.products \
id:INTEGER,product_name:STRING,quantity:INTEGER,created_at:TIMESTAMP


# 3.3

# Create a Cloud Storage bucket for the UDF function
gsutil mb gs://YOUR_PROJECT_ID-dataflow-functions

# Upload the transform.js file to the bucket (copy from your project files)
gsutil cp transform.js gs://YOUR_PROJECT_ID-dataflow-functions/

# Launch Dataflow job with Pub/Sub to BigQuery template
gcloud dataflow jobs run mysql-to-bigquery-stream \
  --gcs-location gs://dataflow-templates-us-central1/latest/PubSub_to_BigQuery \
  --region us-central1 \
  --parameters \
inputTopic=projects/YOUR_PROJECT_ID/topics/mysql-cdc-events.inventory.products,\
outputTableSpec=YOUR_PROJECT_ID:inventory.products,\
javascriptTextTransformGcsPath=gs://YOUR_PROJECT_ID-dataflow-functions/transform.js,\
javascriptTextTransformFunctionName=transform

# Monitor the Dataflow job status
gcloud dataflow jobs list --region=us-central1 --status=active

# Check if data is flowing into BigQuery
bq query --use_legacy_sql=false \
"SELECT COUNT(*) as total_records FROM \`YOUR_PROJECT_ID.inventory.products\`"

# For having synchronized data, clear both tables data and let the script and dataflow job run.
