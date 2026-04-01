# Gerar uma senha aleatória para o banco
resource "random_password" "pg_password" {
  length           = 16
  special          = true
  override_special = "_%@"
}

# Criar o Servidor PostgreSQL Flexível
resource "azurerm_postgresql_flexible_server" "postgres" {
  name                   = "${var.project_name}-psql-2026"
  resource_group_name    = azurerm_resource_group.rg.name
  location               = azurerm_resource_group.rg.location
  version                = "13"
  administrator_login    = "psqladmin"
  administrator_password = random_password.pg_password.result

  storage_mb = 32768
  sku_name   = "B_Standard_B1ms"
  zone       = "1"
}

# Firewall para acesso
resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_my_ip" {
  name             = "allow-local-ip"
  server_id        = azurerm_postgresql_flexible_server.postgres.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "255.255.255.255"
}