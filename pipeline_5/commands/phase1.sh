# Update package lists and upgrade all installed packages automatically
sudo apt update && sudo apt upgrade -y

# Install additional packages for Docker repository setup
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

# Create directory for APT keyrings
sudo mkdir -p /etc/apt/keyrings

# Download and add Docker's official GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Add Docker repository to APT sources
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update package lists with Docker repository
sudo apt update

# Install Docker Engine and related components
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Add current user to docker group for running Docker without sudo
sudo usermod -aG docker $USER

# Start and enable Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Verify Docker installation by checking version
docker --version

# Install Terraform using snap package manager
sudo snap install terraform --classic

# Change to home directory
cd ~

# Create a test directory for Terraform
mkdir terraform-test

# Change to terraform-test directory
cd terraform-test

# Create Terraform configuration file for GCP
cat > main.tf << 'EOF'
provider "google" {
  project = "sada-tirayr"
  region  = "us-central1"
}

data "google_project" "current" {}

output "project_id" {
  value = data.google_project.current.project_id
}
EOF

# Initialize Terraform working directory and download provider plugins
terraform init

# Preview changes that Terraform will make
terraform plan

# Return to home directory
cd ~

# Remove terraform-test directory and its contents
rm -rf ~/terraform-test

# Create directory structure for project (data, scripts, output, docker)
mkdir -p data scripts output docker

# List contents with detailed information
ls -la

# Check size of Reviews.csv file in human-readable format
du -h ~/data/Reviews.csv

# Count number of lines in Reviews.csv file
wc -l ~/data/Reviews.csv

# Display first 3 lines of Reviews.csv file
head -3 ~/data/Reviews.csv

# List directories in home directory
ls

# Change to scripts directory
cd scripts/

# Create/edit sentiment_analysis.py file using nano text editor
nano sentiment_analysis.py
# Copy paste the spark code

# Return to home directory
cd ..

# Change to docker directory
cd ~/docker

# Create new docker-compose.yml with Apache Spark configuration
nano docker-compose.yml
# Copy paste the content of.yml file

# Start Docker containers defined in docker-compose.yml in detached mode
docker compose up -d

# List running Docker containers
docker ps

# View logs from spark-master container
docker logs spark-master

# View logs from spark-worker container
docker logs spark-worker

# Access spark-master container shell
docker exec -it spark-master bash

# Install pip3 inside spark-master container (run inside container)
apt-get update && apt-get install -y python3-pip

# Install required Python packages inside spark-master container (run inside container)
pip3 install nltk textblob

# Exit from spark-master container
exit

# Access spark-worker container shell
docker exec -it spark-worker bash

# Install pip3 inside spark-worker container (run inside container)
apt-get update && apt-get install -y python3-pip

# Install required Python packages inside spark-worker container (run inside container)
pip3 install nltk textblob

# Exit from spark-worker container
exit

# Submit Spark job to process sentiment analysis on the cluster
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark-scripts/sentiment_analysis.py

# List contents of reviews_with_sentiment.csv directory
ls ~/output/reviews_with_sentiment.csv/

# Display first 10 lines from partition CSV files
head -10 ~/output/reviews_with_sentiment.csv/part-*.csv

# Concatenate all partition files and display first 20 lines
cat ~/output/reviews_with_sentiment.csv/part-*.csv | head -20

# Count total number of lines in reviews_with_sentiment CSV files
wc -l ~/output/reviews_with_sentiment.csv/part-*.csv

# Count total number of lines in product_sentiment_summary CSV files
wc -l ~/output/product_sentiment_summary.csv/part-*.csv