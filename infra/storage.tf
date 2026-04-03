# Criar a Storage Account (Data Lake Gen2)
resource "azurerm_storage_account" "datalake" {
  name                     = "loyaltydatadl2026" # Removi o hífen e a variável aqui
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Ativa o ADLS Gen2

  tags = {
    environment = "dev"
  }
}

# Criar o Container Principal (File System)
resource "azurerm_storage_data_lake_gen2_filesystem" "lake_container" {
  name               = "lake"
  storage_account_id = azurerm_storage_account.datalake.id
}
