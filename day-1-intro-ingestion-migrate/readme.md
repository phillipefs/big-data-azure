<p align="center">
  <a href="" rel="noopener">
    <img src="https://github.com/owshq-plumbers/trn-cc-bg-azure/blob/main/images/day1.png" alt="Project logo">
 </a>
</p>




The first part of our jorney is to understand a few things to build a fundamental understanding.

* History of Microsoft Azure
* How the Azure Portal works
* Subscription understanding
* What is a resource group
* Identity and Authorization Manager [IAM]
* Azure Command-line
* How pricing works, everything have a cost

After understand these fundamental principals, let's build our cloud data infrastructure using terraform.

### Terraform 

```sh
# enter in terraform directory
cd day-1-Intro-Ingestao-migrate/terraform

# terraform initialize command line
terraform init

# login in Azure using CLI
az login

# terraform planning of which resources will be build
terraform plan -var-file="settings.tfvars"

# create resources
terraform apply -var-file="settings.tfvars" -auto-approve


# house keeping
# verify itens that will be destroy
terraform plan -var-file="settings.tfvars" -destroy

# apply the destruction plan
terraform apply -var-file="settings.tfvars" -destroy -auto-approve


```
### Blob Storage

Now let's understando more about the storage part:

* Object Storage
* Data Lake
* Blob Storage
* Data Lake Gen 2
* SDKs

Let's understand how to build a data lake using Microsoft Azure

### Event Hubs

For ingestion, first we need to delve into some concepts.

* Pub & Sub
* Delivery Guarantee
* Event Hubs
* Integrations


Bulding a Python application to ingest data into Azure Event Hubs.

```sh
# move to event-hubs diretory
cd day-1-intro-ingestion-migrate/event-hubs/producer

# create a python venv first
virtualenv producer-venv
source ./producer-venv/bin/activate

# install requirements libs
pip install -r requirements.txt

# run application
python3.9 app.py
```

### SQL

Let's understand the SQL options we have in Microsoft Azure.

* RDMS
* IaaS vs SaaS & PaaS
* Azure SQL Database
* Azure Database for MySQL
* Azure Database for MariaDB

Let's observe how to make migration using Azure Database Migration Service in Azure Portal.
### Data Transfer

The most common task when we are on-premises environment is to transfer you data inside the cloud, let's understand how it works in Microsoft Azure

* Data migration
* Azure Data Box
* DMS
* Azure Portal
* Azure Storage Explore
* Azure CLI

Let's see how we migrate data using one of utility tools, AzCopy.

```sh
# azcopy application CLI
# download site 
# https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json

/Users/mateusoliveira/azcopy 

# login for AzCopy authetication
/Users/mateusoliveira/azcopy login

# for run the azcopy you need to generate the SAS Token [blob SAS URL]
/Users/mateusoliveira/azcopy  copy '/Users/mateusoliveira/mateus/DataEngineeringWork/OwsHQ/github/trn-cc-bg-azure/day-1-intro-ingestion-migrate/transfer/file/*.json' 'https://trnccbgazurestg.blob.core.windows.net/landing/subscriptions?sp=racwlmo&st=2022-11-02T13:15:19Z&se=2022-11-02T21:15:19Z&spr=https&sv=2021-06-08&sr=d&sig=SPrsWDX261m79hX2ITg1im7MxQH9hwreJdEq%2BmQlyYI%3D&sdd=1'
```

