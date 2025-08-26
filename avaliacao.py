import requests
import pandas as pd
import time

API_URL = 'https://carchost.fieldcontrol.com.br/ratings'
API_KEY = 'ODU1OWZkODItYjU3MC00NjllLTlmYjEtYTA3ZGJjNzBmN2E2OjU3MDE2'
headers = {
    'Content-Type': 'application/json;charset=UTF-8',
    'X-Api-Key': API_KEY,
}

def get_all_ratings():
    results = []
    offset = 0
    limit = 100

    while True:
        params = {'offset': offset, 'limit': limit}
        response = requests.get(API_URL, headers=headers, params=params)
        if response.status_code != 200:
            print(f'Erro ao buscar página: {response.status_code}')
            break
        data = response.json()
        items = data.get('items', [])
        total_count = data.get('totalCount', 0)
        if not items:
            break
        results.extend(items)
        if len(results) >= total_count:
            break
        offset += limit
        time.sleep(0.5)

    if results:
        df = pd.json_normalize(results)
        # --- Ajuste: reduz 3 horas da coluna 'createdAt' e remove qualquer timezone ---
        if 'createdAt' in df.columns:
            df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce')
            df['createdAt'] = df['createdAt'].dt.tz_localize(None)
            df['createdAt'] = df['createdAt'] - pd.Timedelta(hours=3)

        df.to_excel('avaliacoes_garantia.xlsx', index=False)
        print(f"Arquivo Excel 'avaliacoes_garantia.xlsx' gerado com sucesso! Total de avaliações: {len(results)}")
    else:
        print("Nenhuma avaliação encontrada.")

if __name__ == '__main__':
    print("Buscando avaliações...")
    get_all_ratings()



