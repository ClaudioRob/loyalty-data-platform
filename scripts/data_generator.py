import pandas as pd
import json
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('pt_PT') # Usando localidade Portugal para cidades e nomes
Faker.seed(42)

def generate_mock_data(records=100):
    transactions = []
    customers = []
    
    # Criar uma lista de 20 clientes fixos para gerar recorrência
    cust_ids = [i for i in range(1001, 1021)]
    
    for c_id in cust_ids:
        customers.append({
            "cust_id": c_id,
            "name": fake.name(),
            "loyalty_tier": random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            "home_city": fake.city(),
            "active_products": random.sample(['Seguro Auto', 'Previdência', 'Cartão Prime', 'Seguro Vida'], k=random.randint(1,3)),
            "known_devices": [fake.uuid4() for _ in range(2)] # Dispositivos confiáveis
        })

    for _ in range(records):
        c_id = random.choice(cust_ids)
        channel = random.choice(['Físico', 'Online', 'App'])
        
        tx = {
            "tx_id": fake.uuid4(),
            "cust_id": c_id,
            "tx_datetime": (datetime.now() - timedelta(days=random.randint(0, 1))).strftime('%Y-%m-%d %H:%M:%S'),
            "tx_amount": round(random.uniform(5.0, 2500.0), 2),
            "tx_category": random.choice(['Alimentação', 'Seguro', 'Previdência', 'Saúde', 'Lazer']),
            "channel": channel,
            "location_city": fake.city() if channel == 'Físico' else 'N/A',
            "ip_address": fake.ipv4() if channel != 'Físico' else 'N/A',
            "device_id": fake.uuid4() # Simula um ID de dispositivo para a transação
        }
        transactions.append(tx)

    # --- INSERÇÃO DE FRAUDE PROPOSITAL ---
    # Fraude Online: Mesmo Device ID para clientes diferentes em curto tempo
    fraud_device = fake.uuid4()
    for i in range(2):
        transactions.append({
            "tx_id": fake.uuid4(),
            "cust_id": cust_ids[i],
            "tx_datetime": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "tx_amount": 4999.99,
            "tx_category": 'Eletrônicos',
            "channel": 'Online',
            "location_city": 'N/A',
            "ip_address": '1.2.3.4',
            "device_id": fraud_device # ID compartilhado (Suspeito!)
        })

    # Salvar Arquivos
    pd.DataFrame(transactions).to_csv('data/transactions_20260331.csv', index=False)
    with open('data/customer_profiles_20260331.json', 'w', encoding='utf-8') as f:
        json.dump(customers, f, ensure_ascii=False, indent=4)

    print("✅ Arquivos gerados em /data: transactions (CSV) e customer_profiles (JSON)")

if __name__ == "__main__":
    generate_mock_data()