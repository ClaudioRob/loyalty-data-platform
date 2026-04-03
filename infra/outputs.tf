output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "postgresql_server_name" {
  value = azurerm_postgresql_flexible_server.postgres.name
}

output "postgresql_admin_login" {
  value = azurerm_postgresql_flexible_server.postgres.administrator_login
}

# CUIDADO: Em produção, não exportamos senhas no output. 
# Aqui usaremos para facilitar o seu teste inicial.
output "postgresql_admin_password" {
  value     = random_password.pg_password.result
  sensitive = true
}