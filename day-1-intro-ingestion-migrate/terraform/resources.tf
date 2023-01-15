
# create our resource group for training objects
resource "azurerm_resource_group" "rg-trn-cc-bg-azure" {
  name     = "rg-trn-cc-bg-azure"
  location = "East US 2"
}


# create storage account [blob storage] Data Lake Gen2
# account_kind = StorageV2 [Data lake gen2]
resource "azurerm_storage_account" "trn-cc-bg-azure-stg" {
  name                     = "trnccbgazurestg"
  resource_group_name      = azurerm_resource_group.rg-trn-cc-bg-azure.name
  location                 = azurerm_resource_group.rg-trn-cc-bg-azure.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}
/*
# creating landing container - data coming from application [first contact]
resource "azurerm_storage_container" "landing" {
  name                  = "landing"
  storage_account_name  = azurerm_storage_account.trn-cc-bg-azure-stg.name
  container_access_type = "private"
}

# creating processing container - data processed based on business needs [second contact]
/*
resource "azurerm_storage_container" "processing" {
  name                  = "processing"
  storage_account_name  = azurerm_storage_account.trn-cc-bg-azure-stg.name
  container_access_type = "private"
}

# creating curated container - data deliver to the business [final contact]
resource "azurerm_storage_container" "curated" {
  name                  = "curated"
  storage_account_name  = azurerm_storage_account.trn-cc-bg-azure-stg.name
  container_access_type = "private"
}
/*
# event hubs
resource "azurerm_eventhub_namespace" "trn-cc-bg-azure-eh" {
  name                = "bg-azure-eh"
  location            = azurerm_resource_group.rg-trn-cc-bg-azure.location
  resource_group_name = azurerm_resource_group.rg-trn-cc-bg-azure.name
  sku                 = "Standard"
  capacity            = 1

  tags = {
    environment = "Dev"
  }
}

resource "azurerm_eventhub" "src-app-music-events-json" {
  name                = "src-app-music-events-json"
  namespace_name      = azurerm_eventhub_namespace.trn-cc-bg-azure-eh.name
  resource_group_name = azurerm_resource_group.rg-trn-cc-bg-azure.name
  partition_count     = 2
  message_retention   = 1
}
*/