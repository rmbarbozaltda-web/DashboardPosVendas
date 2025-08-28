import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

# --- CARREGANDO O NOVO ARQUIVO DE CONFIGURAÇÃO ---
try:
    with open('config_teste.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)
except Exception as e:
    st.error(f"Erro ao ler o arquivo config_teste.yaml: {e}")
    st.stop()

# --- DIAGNÓSTICO ---
# Vamos verificar o que foi carregado do novo arquivo
st.subheader("Diagnóstico do config_teste.yaml")
st.json(config)

# --- INICIALIZAÇÃO DO AUTENTICADOR ---
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

# --- TELA DE LOGIN ---
st.subheader("Tela de Teste de Login")
authenticator.login()

# --- LÓGICA PÓS-LOGIN ---
if st.session_state["authentication_status"]:
    authenticator.logout()
    st.write(f'Bem-vindo *{st.session_state["name"]}*')
    st.success('Login realizado com sucesso no ambiente de teste!')
elif st.session_state["authentication_status"] is False:
    st.error('Usuário/senha está incorreto')
elif st.session_state["authentication_status"] is None:
    st.warning('Por favor, insira seu usuário e senha')

