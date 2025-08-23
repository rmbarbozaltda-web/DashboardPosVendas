import requests
import pandas as pd
import os
import pytz
from dateutil import parser
from datetime import timedelta

# --- Configurações ---
API_URL_TASKS = "https://carchost.fieldcontrol.com.br/tasks"
API_URL_EMPLOYEES = "https://carchost.fieldcontrol.com.br/employees/"
API_KEY = "ODU1OWZkODItYjU3MC00NjllLTlmYjEtYTA3ZGJjNzBmN2E2OjU3MDE2"
LIMIT = 100
ARQUIVO = "atividades.xlsx"

headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "X-Api-Key": API_KEY,
}

status_traducao = {
    "done": "Concluída",
    "scheduled": "Agendada",
    "canceled": "Cancelada",
    "reported": "Reportada",
    "pending": "Pendente",
    "in-progress": "Em andamento",
    "on-route": "A caminho"
}

# --- Funções de Processamento e Normalização ---

def traduz_status(status, status_nao_traduzidos):
    if pd.notnull(status):
        if status not in status_traducao:
            status_nao_traduzidos.add(status)
        return status_traducao.get(status, status)
    return status

def processa_traducao_status(df):
    status_nao_traduzidos = set()
    if 'status' in df.columns:
        df['status_pt'] = df['status'].apply(lambda x: traduz_status(x, status_nao_traduzidos))
        if status_nao_traduzidos:
            print("ATENÇÃO: Encontrados status sem tradução:")
            for st in status_nao_traduzidos:
                print(f" - '{st}'")
            print("Adicione ao dicionário 'status_traducao' acima para traduzir corretamente.")
    else:
        print("Aviso: coluna 'status' não encontrada na base.")

def normaliza_colunas_id(df):
    for col in df.columns:
        amostra = df[col].head(10).dropna()
        if not amostra.empty and amostra.apply(lambda x: isinstance(x, dict) and 'id' in x).any():
            print(f"Normalizando coluna '{col}' (pegando só o valor de 'id').")
            df[col] = df[col].apply(lambda x: x['id'] if isinstance(x, dict) and 'id' in x else x)

def normaliza_colunas_data_hora(df):
    colunas_candidatas = ['scheduling', 'startedAt', 'finishedAt', 'onRouteAt', 'checkinAt']
    for col in colunas_candidatas:
        if col in df.columns:
            amostra = df[col].head(10).dropna()
            if not amostra.empty and amostra.apply(lambda x: isinstance(x, dict) and ('date' in x or 'time' in x)).any():
                print(f"Normalizando coluna '{col}' (montando string a partir de 'date' e 'time').")
                def dict_to_str(x):
                    if not isinstance(x, dict): return x
                    dt = x.get('date', '')
                    tm = x.get('time', '')
                    return f"{dt} {tm}".strip()
                df[col] = df[col].apply(dict_to_str)

def normaliza_colunas_latlong(df):
    # CORREÇÃO: Adicionadas as novas colunas de coordenadas
    colunas_candidatas = [
        'checkinLocation', 'checkoutLocation', 'onRouteLocation', 'startLocation',
        'coords', 'startCoords', 'completeCoords'
    ]
    for col in colunas_candidatas:
        if col in df.columns:
            amostra = df[col].head(10).dropna()
            if not amostra.empty and amostra.apply(lambda x: isinstance(x, dict) and ('latitude' in x or 'coords' in x)).any():
                print(f"Normalizando coluna '{col}' (latitude/longitude).")
                def extrai_latlong(x):
                    if isinstance(x, dict) and 'coords' in x and isinstance(x['coords'], dict):
                        lat, lon = x['coords'].get('latitude'), x['coords'].get('longitude')
                    elif isinstance(x, dict):
                        lat, lon = x.get('latitude'), x.get('longitude')
                    else:
                        return ''
                    return f"{lat}, {lon}" if lat is not None and lon is not None else ''
                df[col] = df[col].apply(extrai_latlong)

def normaliza_colunas_complexas_para_string(df):
    # NOVA FUNÇÃO: Converte colunas com dicionários/listas genéricos para texto
    colunas_candidatas = ['metadata', 'customFields']
    for col in colunas_candidatas:
        if col in df.columns:
            # Verifica se a coluna contém dicionários ou listas
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                print(f"Convertendo coluna complexa '{col}' para texto.")
                df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) and isinstance(x, (dict, list)) else x)

def ajustar_horario_scheduling(df):
    if 'scheduling' not in df.columns:
        return df

    print("Ajustando +3 horas na coluna 'scheduling'...")
    datas_dt = pd.to_datetime(df['scheduling'], errors='coerce')
    df['scheduling'] = datas_dt.apply(lambda x: x + timedelta(hours=3) if pd.notna(x) else x)
    return df

def normaliza_datas_para_brasilia(df):
    tz_brasilia = pytz.timezone('America/Sao_Paulo')
    colunas_de_data = ['scheduling', 'startedAt', 'finishedAt', 'onRouteAt', 'checkinAt', 'createdAt', 'updatedAt']
    
    for col in colunas_de_data:
        if col in df.columns:
            datetime_series = pd.to_datetime(df[col], errors='coerce')
            if not datetime_series.isna().all():
                print(f"Formatando datas para Brasília na coluna: '{col}'")
                def formata_data(dt_obj):
                    if pd.isna(dt_obj): return ''
                    if dt_obj.tzinfo is None: dt_obj = pytz.utc.localize(dt_obj)
                    return dt_obj.astimezone(tz_brasilia).strftime('%d/%m/%Y %H:%M:%S')
                df[col] = datetime_series.apply(formata_data)

# --- Funções de Coleta de Dados (API) ---

def get_all_tasks():
    all_data = []
    offset = 0
    while True:
        params = {"limit": LIMIT, "offset": offset}
        try:
            resp = requests.get(API_URL_TASKS, headers=headers, params=params)
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if not items: break
            all_data.extend(items)
            print(f"Obtidas {len(items)} linhas (offset {offset})")
            if len(items) < LIMIT: break
            offset += LIMIT
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição da API de tarefas: {e}")
            break
    return all_data

def get_employees():
    id_to_nome = {}
    offset = 0
    while True:
        params = {"limit": LIMIT, "offset": offset}
        try:
            resp = requests.get(API_URL_EMPLOYEES, headers=headers, params=params)
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if not items: break
            for emp in items:
                nome = emp.get("name") or emp.get("fullName") or "Desconhecido"
                _id = emp.get("id") or emp.get("_id")
                if _id:
                    id_to_nome[str(_id)] = nome
            if len(items) < LIMIT: break
            offset += LIMIT
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição da API de funcionários: {e}")
            break
    return id_to_nome

def adiciona_colaborador_nome(df, id_to_nome):
    if "employee" not in df.columns:
        print("Aviso: Campo 'employee' não encontrado para adicionar nome do colaborador.")
        df["colaborador_nome"] = ""
        return
    
    print("Adicionando nome do colaborador...")
    df["colaborador_nome"] = df["employee"].astype(str).map(id_to_nome).fillna('Não encontrado')

# --- Função Principal ---

def gerar_arquivo_atividades():
    id_to_nome = get_employees()
    
    def processar_dataframe(df):
        """Pipeline completo de processamento para um DataFrame."""
        normaliza_colunas_id(df)
        normaliza_colunas_data_hora(df)
        df = ajustar_horario_scheduling(df)
        normaliza_colunas_latlong(df) # Agora processa as colunas corretas
        normaliza_colunas_complexas_para_string(df) # Nova chamada de função
        normaliza_datas_para_brasilia(df)
        processa_traducao_status(df)
        adiciona_colaborador_nome(df, id_to_nome)
        return df

    if not os.path.exists(ARQUIVO):
        print("Arquivo não existe. Gerando base nova de atividades...")
        atividades = get_all_tasks()
        if not atividades:
            print("Nenhuma atividade encontrada na API.")
            return
        df = pd.DataFrame(atividades)
        df = processar_dataframe(df)
        df.to_excel(ARQUIVO, index=False)
        print(f"Base de atividades salva em {ARQUIVO}")
        return

    print("Arquivo já existe. Verificando atualizações...")
    df_antigo = pd.read_excel(ARQUIVO, engine='openpyxl')
    atividades_novas = get_all_tasks()
    if not atividades_novas:
        print("Nenhuma atividade nova carregada da API. O arquivo não foi modificado.")
        return
    
    df_novo = pd.DataFrame(atividades_novas)

    if 'id' not in df_antigo.columns or 'id' not in df_novo.columns:
        print("ERRO: Coluna 'id' não encontrada. Verifique a API ou o arquivo Excel.")
        return

    print("\nProcessando os novos dados recebidos da API...")
    df_novo = processar_dataframe(df_novo)
    print("Processamento dos novos dados concluído.\n")

    df_antigo['id'] = df_antigo['id'].astype(str)
    df_novo['id'] = df_novo['id'].astype(str)

    ids_antigos = set(df_antigo['id'])
    ids_novos = set(df_novo['id'])
    inseridos = len(ids_novos - ids_antigos)
    atualizados = len(ids_novos.intersection(ids_antigos))

    df_final = pd.concat([df_antigo, df_novo]).drop_duplicates(subset=['id'], keep='last', ignore_index=True)
    
    df_final.to_excel(ARQUIVO, index=False)
    print(f"Incremental concluído: {atualizados} registros atualizados, {inseridos} novos registros inseridos.")
    print(f"Base final salva em {ARQUIVO}")

if __name__ == "__main__":
    gerar_arquivo_atividades()







