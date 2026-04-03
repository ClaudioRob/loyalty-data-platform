import os
from azure.storage.file_datalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

def upload_to_bronze(file_path, target_name):
    # O nome que definimos no Terraform
    account_name = "loyaltydatadl2026" 
    account_url = f"https://{account_name}.dfs.core.windows.net"
    
    # O DefaultAzureCredential vai usar o seu 'az login' automaticamente
    token_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)
    
    # O container 'lake' que o Terraform está criando
    file_system_client = service_client.get_file_system_client(file_system="lake")
    
    # Criando/Acessando o diretório 'bronze'
    directory_client = file_system_client.get_directory_client("bronze")
    
    print(f"⬆️ Enviando {target_name}...")
    file_client = directory_client.get_file_client(target_name)
    
    with open(file_path, "rb") as data:
        file_client.upload_data(data, overwrite=True)
    
    print(f"✅ {target_name} está na Camada Bronze!")

if __name__ == "__main__":
    # Caminhos relativos a partir da raiz do projeto
    upload_to_bronze("data/transactions_20260331.csv", "transactions_20260331.csv")
    upload_to_bronze("data/customer_profiles_20260331.json", "customer_profiles_20260331.json")