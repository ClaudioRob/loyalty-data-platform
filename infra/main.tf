# 1. Definir o Provider (Azure)
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

# No seu infra/main.tf
provider "azurerm" {
  features {}
  subscription_id = "8c6f6f57-f620-483a-bbf5-3fc7dad0488f"
  tenant_id       = "3048dc87-43f0-4100-9acb-ae1971c79395" # O ID que aparece para 'Crs Consultoria'
}

# 2. Criar o Resource Group (O "container" de tudo)
resource "azurerm_resource_group" "rg" {
  name     = "${var.project_name}-rg"
  location = var.location
}

