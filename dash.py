import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import warnings
import io
import pytz
# --- INÍCIO DA PARTE DE AUTENTICAÇÃO ---
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
# --- FIM DA PARTE DE AUTENTICAÇÃO ---
warnings.filterwarnings('ignore')
# Configuração da página
st.set_page_config(
    page_title="Dashboard Pós-Vendas Topema",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)
# --- INÍCIO DA PARTE DE AUTENTICAÇÃO ---
# Carregando as credenciais do arquivo config.yaml
with open('config_teste.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)
# Criando o objeto authenticator
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)
# Renderizando o widget de login
authenticator.login()
if st.session_state["authentication_status"]:
    # --- FIM DA PARTE DE AUTENTICAÇÃO ---
    # Título principal
    st.title("🏭 Dashboard Pós-Vendas Topema")
    st.markdown("---")
    @st.cache_data
    def carregar_dados():
        """Carrega e processa todos os dados necessários"""
        try:
            # Carregando as tabelas
            ordens_servico = pd.read_excel('ordens_de_servico.xlsx')
            atividades = pd.read_excel('atividades.xlsx')
            equipamentos = pd.read_excel('tabela_equipamentos.xlsx')
            respostas = pd.read_excel('tabela_respostas.xlsx')
            depara_etiquetas = pd.read_excel('DePara Etiquetas.xlsx')
            depara_estados = pd.read_excel('DePara Estados.xlsx')
            avaliacoes_garantia = pd.read_excel('avaliacoes_garantia.xlsx')
            
            # --- FILTRAR OS TOTALMENTE ARQUIVADAS ---
            if 'archived' in atividades.columns:
                atividades['archived'] = atividades['archived'].astype(bool)
                status_arquivamento_por_os = atividades.groupby('order')['archived'].all()
                os_ids_para_remover = status_arquivamento_por_os[status_arquivamento_por_os].index.tolist()
                ordens_servico = ordens_servico[~ordens_servico['id'].isin(os_ids_para_remover)]
            # Filtrando apenas ordens de garantia
            ordens_servico = ordens_servico[ordens_servico['Tipo de Serviço'] == 'Garantia']
            # Convertendo datas e tratando timezones
            colunas_data_os = ['Criado em (UTC)', 'Atualizado em (UTC)', 'Atualizado em (Brasília)']
            for col in colunas_data_os:
                if col in ordens_servico.columns:
                    ordens_servico[col] = pd.to_datetime(ordens_servico[col], errors='coerce')
                    if 'UTC' in col and ordens_servico[col].dt.tz is None:
                        ordens_servico[col] = ordens_servico[col].dt.tz_localize('UTC')
            colunas_data_ativ = ['startedAt', 'completedAt', 'createdAt', 'updatedAt', 'scheduling']
            for col in colunas_data_ativ:
                if col in atividades.columns:
                    atividades[col] = pd.to_datetime(atividades[col], errors='coerce')
                    if atividades[col].dt.tz is None:
                        atividades[col] = atividades[col].dt.tz_localize('UTC')
            # Aplicando DE/PARA nos estados
            if not depara_estados.empty:
                estado_map = dict(zip(depara_estados['DE'], depara_estados['PARA']))
                ordens_servico['Cliente - Estado'] = ordens_servico['Cliente - Estado'].map(estado_map).fillna(ordens_servico['Cliente - Estado'])
            # Processando etiquetas (equipamentos)
            def processar_etiquetas(row):
                if pd.isna(row['Etiquetas']):
                    return []
                etiquetas = [e.strip() for e in str(row['Etiquetas']).split(',')]
                if not depara_etiquetas.empty:
                    etiqueta_map = dict(zip(depara_etiquetas['DE'], depara_etiquetas['PARA']))
                    etiquetas_processadas = [etiqueta_map.get(e, e) for e in etiquetas]
                else:
                    etiquetas_processadas = etiquetas
                return etiquetas_processadas
            ordens_servico['Etiquetas_Processadas'] = ordens_servico.apply(processar_etiquetas, axis=1)
            # LÓGICA DE CONCLUSÃO DAS OS - AJUSTADA
            def calcular_status_os(os_id):
                atividades_os = atividades[atividades['order'] == os_id].copy()
                # NOVO: filtrando apenas atividades NÃO arquivadas
                atividades_validas = atividades_os[atividades_os['archived'] == False]
                if atividades_validas.empty:
                    # Essa OS já foi removida pela filtragem das totalmente arquivadas
                    return 'Sem Atividade', None, False
                # A OS só estará concluída se todas as atividades válidas tiverem completedAt preenchido
                todas_concluidas = atividades_validas['completedAt'].notna().all()
                if todas_concluidas:
                    status = 'Concluída'
                    data_conclusao = atividades_validas['completedAt'].max()
                    os_concluida = True
                else:
                    # Status será o da última atividade criada (entre as não arquivadas)
                    ultima_atividade = atividades_validas.sort_values('createdAt', ascending=False).iloc[0]
                    status = ultima_atividade['status_pt']
                    data_conclusao = None
                    os_concluida = False
                return status, data_conclusao, os_concluida
            status_info = []
            for os_id in ordens_servico['id']:
                status, data_conclusao, concluida = calcular_status_os(os_id)
                status_info.append({'id': os_id, 'status_final': status, 'data_conclusao': data_conclusao, 'os_concluida': concluida})
            status_df = pd.DataFrame(status_info)
            ordens_servico = ordens_servico.merge(status_df, on='id', how='left')
            # LÓGICA DE CORREÇÃO DE DATAS DE CRIAÇÃO
            mask_data_invalida = (ordens_servico['data_conclusao'].notna()) & (ordens_servico['Criado em (UTC)'].notna()) & (ordens_servico['data_conclusao'] < ordens_servico['Criado em (UTC)'])
            os_ids_para_corrigir = ordens_servico.loc[mask_data_invalida, 'id']
            if not os_ids_para_corrigir.empty:
                atividades_para_correcao = atividades[atividades['order'].isin(os_ids_para_corrigir) & atividades['scheduling'].notna()].copy()
                if not atividades_para_correcao.empty:
                    atividades_para_correcao = atividades_para_correcao.sort_values('createdAt')
                    ultimas_atividades_agendadas = atividades_para_correcao.drop_duplicates(subset='order', keep='last')
                    mapa_datas_corrigidas = ultimas_atividades_agendadas.set_index('order')['scheduling']
                    ordens_servico['data_criacao_corrigida'] = ordens_servico['id'].map(mapa_datas_corrigidas)
                    ordens_servico['Criado em (UTC)'] = np.where(
                        (mask_data_invalida) & (ordens_servico['data_criacao_corrigida'].notna()),
                        ordens_servico['data_criacao_corrigida'],
                        ordens_servico['Criado em (UTC)']
                    )
                    ordens_servico = ordens_servico.drop(columns=['data_criacao_corrigida'])
            # AJUSTE DE FUSO HORÁRIO PARA BRASÍLIA
            fuso_horario_br = 'America/Sao_Paulo'
            ordens_servico['Criado em'] = ordens_servico['Criado em (UTC)'].dt.tz_convert(fuso_horario_br)
            ordens_servico['data_conclusao'] = ordens_servico['data_conclusao'].dt.tz_convert(fuso_horario_br)
            return ordens_servico, atividades, equipamentos, respostas, depara_etiquetas, depara_estados,avaliacoes_garantia
        except Exception as e:
            st.error(f"Erro ao carregar dados: {str(e)}")
            return None, None, None, None, None, None, None

    # Carregando os dados
    ordens_servico, atividades, equipamentos, respostas, depara_etiquetas, depara_estados, avaliacoes_garantia = carregar_dados()

    if ordens_servico is not None:

        st.sidebar.write(f'Bem-vindo, *{st.session_state["name"]}*')
        authenticator.logout('Logout', 'sidebar')
        st.sidebar.markdown("---") # Adiciona uma linha divisória
        # Adicionar o logo na barra lateral
        try:
            st.sidebar.image("logo.png", use_container_width=True)
        except Exception as e:
            st.sidebar.warning(f"Não foi possível carregar o logo. Verifique o arquivo de imagem.")

        # Sidebar com filtros
        st.sidebar.header("🔍 Filtros")
        numeros_os = ['Todos'] + sorted(ordens_servico['Numero OS'].dropna().unique().tolist())
        numero_os_selecionado = st.sidebar.selectbox("Número da OS", numeros_os)

        clientes = ['Todos'] + sorted(ordens_servico['Cliente'].dropna().unique().tolist())
        cliente_selecionado = st.sidebar.selectbox("Cliente", clientes)

        estados = ['Todos'] + sorted(ordens_servico['Cliente - Estado'].dropna().unique().tolist())
        estado_selecionado = st.sidebar.selectbox("Estado", estados)

        colaboradores = ['Todos'] + sorted(atividades['colaborador_nome'].dropna().unique().tolist())
        colaborador_selecionado = st.sidebar.selectbox("Colaborador", colaboradores)

        todos_equipamentos = []
        for etiquetas_list in ordens_servico['Etiquetas_Processadas']:
            todos_equipamentos.extend(etiquetas_list)
        equipamentos_unicos = ['Todos'] + sorted(list(set(todos_equipamentos))) if todos_equipamentos else ['Todos']
        equipamento_selecionado = st.sidebar.selectbox("Equipamento", equipamentos_unicos)

        status_os_opcoes = ['Todos', 'Abertos', 'Fechados']
        status_os_selecionado = st.sidebar.selectbox("Status da OS", status_os_opcoes)

        data_min = ordens_servico['Criado em'].min().date()
        data_max = ordens_servico['Criado em'].max().date()
        data_inicio, data_fim = st.sidebar.date_input(
            "Período de Criação",
            value=[data_min, data_max],
            min_value=data_min,
            max_value=data_max,
            format="DD/MM/YYYY"
        )

        st.sidebar.markdown("---")
        sla_dias = st.sidebar.number_input("Meta de SLA (dias)", min_value=1, value=2, step=1)

        # Aplicando filtros
        df_filtrado = ordens_servico.copy()

        if status_os_selecionado == 'Abertos':
            df_filtrado = df_filtrado[df_filtrado['os_concluida'] == False]
        elif status_os_selecionado == 'Fechados':
            df_filtrado = df_filtrado[df_filtrado['os_concluida'] == True]

        if numero_os_selecionado != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['Numero OS'] == numero_os_selecionado]
        if cliente_selecionado != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['Cliente'] == cliente_selecionado]
        if estado_selecionado != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['Cliente - Estado'] == estado_selecionado]
        if equipamento_selecionado != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['Etiquetas_Processadas'].apply(lambda x: equipamento_selecionado in x)]

        atividades_filtro_os = atividades[atividades['order'].isin(df_filtrado['id'])]

        if colaborador_selecionado != 'Todos':
            os_ids_colaborador = atividades[atividades['colaborador_nome'] == colaborador_selecionado]['order'].unique()
            df_filtrado = df_filtrado[df_filtrado['id'].isin(os_ids_colaborador)]
            atividades_filtro_os = atividades_filtro_os[atividades_filtro_os['colaborador_nome'] == colaborador_selecionado]

        if data_inicio and data_fim:
            data_inicio_tz = pd.Timestamp(data_inicio, tz='America/Sao_Paulo')
            data_fim_tz = pd.Timestamp(data_fim, tz='America/Sao_Paulo') + pd.Timedelta(days=1)
            df_filtrado = df_filtrado[(df_filtrado['Criado em'] >= data_inicio_tz) & (df_filtrado['Criado em'] < data_fim_tz)]

        # --- SEÇÃO DE CARDS DE KPI ---
        st.header("📊 Indicadores Chave")

        # CSS para os cards customizados
        card_style = """
        <style>
        .card {
            background-color: #1e293b; /* Fundo escuro */
            color: #e2e8f0; /* Texto claro */
            width: 100%;
            height: 140px;
            padding: 20px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            border-radius: 12px;
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
        }
        .card-total { border-left: 6px solid #0ea5e9; } /* Azul vibrante */
        .card-concluidas { border-left: 6px solid #22c55e; } /* Verde vibrante */
        .card-abertas { border-left: 6px solid #f59e0b; } /* Amarelo vibrante */
        .card-sla { border-left: 6px solid #ef4444; } /* Vermelho vibrante */
        .card-title {
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 10px;
            text-align: center;
        }
        .card-value {
            font-size: 2.5rem;
            font-weight: 700;
            text-align: center;
        }
        </style>
        """
        st.markdown(card_style, unsafe_allow_html=True)

        # Cálculos dos KPIs
        total_os = len(df_filtrado)
        os_concluidas = df_filtrado['os_concluida'].sum()
        os_abertas = total_os - os_concluidas
        df_concluidas = df_filtrado[df_filtrado['os_concluida']].copy()
        
        sla_medio_dias = 0
        percentual_no_sla = 0
        if not df_concluidas.empty:
            df_concluidas['tempo_resolucao'] = (df_concluidas['data_conclusao'] - df_concluidas['Criado em']).dt.days
            sla_medio_dias = df_concluidas['tempo_resolucao'].mean()
            os_no_sla = (df_concluidas['tempo_resolucao'] <= sla_dias).sum()
            percentual_no_sla = (os_no_sla / len(df_concluidas)) * 100 if len(df_concluidas) > 0 else 0

        # Exibição dos cards
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f'<div class="card card-total"><div class="card-title">Total de OS</div><div class="card-value">{total_os}</div></div>', unsafe_allow_html=True)
        with col2:
            st.markdown(f'<div class="card card-concluidas"><div class="card-title">OS Concluídas</div><div class="card-value">{os_concluidas}</div></div>', unsafe_allow_html=True)
        with col3:
            st.markdown(f'<div class="card card-abertas"><div class="card-title">OS Abertas</div><div class="card-value">{os_abertas}</div></div>', unsafe_allow_html=True)
        with col4:
            # <-- CARD CORRIGIDO PARA MOSTRAR O SLA MÉDIO EM DIAS -->
            st.markdown(f'<div class="card card-sla"><div class="card-title">SLA Médio (dias)</div><div class="card-value">{sla_medio_dias:.1f}</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # --- SEÇÃO DE GRÁFICOS PRINCIPAIS ---
        st.header("📈 Evolução e Desempenho")
        
        # --- GRÁFICO DE BARRAS "EVOLUÇÃO MENSAL" ---
        st.subheader("Evolução Mensal de OS (Abertas vs. Fechadas)")
        df_filtrado['mes_ano_criacao'] = df_filtrado['Criado em'].dt.to_period('M')
        df_filtrado_concluidas = df_filtrado[df_filtrado['data_conclusao'].notna()].copy()
        df_filtrado_concluidas['mes_ano_conclusao'] = df_filtrado_concluidas['data_conclusao'].dt.to_period('M')
        
        abertas_por_mes = df_filtrado.groupby('mes_ano_criacao').size()
        fechadas_por_mes = df_filtrado_concluidas.groupby('mes_ano_conclusao').size()
        
        meses = sorted(list(set(abertas_por_mes.index.to_timestamp()) | set(fechadas_por_mes.index.to_timestamp())))
        meses = [pd.Period(m, 'M') for m in meses]
        meses_str = [m.strftime('%Y-%m') for m in meses]
        
        abertas_vals = [abertas_por_mes.get(mes, 0) for mes in meses]
        fechadas_vals = [fechadas_por_mes.get(mes, 0) for mes in meses]
        
        fig_evolucao = go.Figure()
        fig_evolucao.add_trace(go.Bar(
            x=meses_str,
            y=abertas_vals,
            name='Abertas',
            marker_color='#0ea5e9', # <-- COR ALTERADA PARA AZUL
            text=abertas_vals,
            textposition='outside' # <-- POSIÇÃO DO RÓTULO ALTERADA
        ))
        fig_evolucao.add_trace(go.Bar(
            x=meses_str,
            y=fechadas_vals,
            name='Fechadas',
            marker_color='#22c55e',
            text=fechadas_vals,
            textposition='outside' # <-- POSIÇÃO DO RÓTULO ALTERADA
        ))

        # Ajuste para garantir que os rótulos não sejam cortados
        max_y_val = 0
        if abertas_vals or fechadas_vals:
            max_y_val = max(max(abertas_vals) if abertas_vals else [0], max(fechadas_vals) if fechadas_vals else [0])

        fig_evolucao.update_layout(
            barmode='group',
            xaxis_title="Mês",
            yaxis_title="Quantidade de OS",
            legend_title="Status",
            height=500,
            margin=dict(l=20, r=20, t=40, b=20),
            yaxis=dict(range=[0, max_y_val * 1.15 + 1]) # Adiciona espaço no topo
        )
        st.plotly_chart(fig_evolucao, use_container_width=True)

                        # ---------------- Gráfico de linha: Evolução acumulada do backlog mês a mês -----------------
        st.markdown("---")
        st.subheader("📈 Evolução Acumulada do Backlog (mês a mês)")

        # <-- Aqui o segredo: use o dataframe filtrado!
        df_backlog = df_filtrado.copy()
        df_backlog = df_backlog[df_backlog['Criado em'].notna()]

        if df_backlog.empty:
            st.info("Nenhuma OS nos filtros selecionados para compor o gráfico.")
        else:
            # Pegue o timezone da coluna "Criado em"
            tz = df_backlog['Criado em'].dt.tz

            primeiro_mes = df_backlog['Criado em'].min().to_period('M').to_timestamp()
            ultimo_mes = max(
                df_backlog['Criado em'].max() if df_backlog['Criado em'].notna().any() else pd.Timestamp.now(tz),
                df_backlog['data_conclusao'].max() if df_backlog['data_conclusao'].notna().any() else pd.Timestamp.now(tz)
            ).to_period('M').to_timestamp()

            # Lista de meses, agora timezone-aware
            meses = pd.date_range(primeiro_mes, ultimo_mes, freq='MS', tz=tz)

            backlog_acumulado = []

            for mes in meses:
                fim_mes = (mes + pd.offsets.MonthEnd(0)).tz_convert(tz)
                criadas = df_backlog[df_backlog['Criado em'] <= fim_mes]
                concluidas = criadas[criadas['data_conclusao'].notna() & (criadas['data_conclusao'] <= fim_mes)]
                backlog = len(criadas) - len(concluidas)
                backlog_acumulado.append({
                    "mes": mes,
                    "backlog_acumulado": backlog
                })

            df_evolucao_backlog = pd.DataFrame(backlog_acumulado)

            fig_backlog = px.line(
                df_evolucao_backlog,
                x="mes",
                y="backlog_acumulado",
                markers=True,
                title="Evolução Acumulada do Backlog de OS (Mês a Mês)",
                labels={"mes": "Mês", "backlog_acumulado": "Backlog acumulado"}
            )
            fig_backlog.update_layout(xaxis=dict(tickformat="%b/%Y"))

            st.plotly_chart(fig_backlog, use_container_width=True)

        # --- GRÁFICO DE VELOCÍMETRO ---
        st.subheader("Percentual de OS Concluídas no Prazo (SLA)")
        fig_sla = go.Figure(go.Indicator(
            mode="gauge+number",
            value=percentual_no_sla,
            title={'text': f"Meta: {sla_dias} dias"},
            domain={'x': [0, 1], 'y': [0, 1]},
            gauge={
                'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                'bar': {'color': "#063581"},
                'borderwidth': 2,
                'bordercolor': "gray",
                'steps': [
                    {'range': [0, 70], 'color': '#ef4444'},
                    {'range': [70, 90], 'color': '#f59e0b'},
                    {'range': [90, 100], 'color': '#22c55e'}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 95
                }
            }
        ))
        fig_sla.update_layout(
            height=300,
            margin=dict(l=30, r=30, t=50, b=20)
        )
        st.plotly_chart(fig_sla, use_container_width=True)

        # --- SEÇÃO DE GRÁFICOS DE BARRAS ---
        st.markdown("---")
        st.header("📋 Análises por Categoria")
        if not df_filtrado.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Top 10 Colaboradores - Atividades")
                colaborador_counts = atividades_filtro_os['colaborador_nome'].value_counts().head(10)
                fig_colab = px.bar(x=colaborador_counts.index, y=colaborador_counts.values, text=colaborador_counts.values)
                fig_colab.update_traces(textposition='outside', texttemplate='%{text}', marker_color='#1f77b4')
                fig_colab.update_layout(height=400, xaxis_title="Colaboradores", yaxis_title="Número de Atividades", xaxis_tickangle=-45, yaxis=dict(range=[0, colaborador_counts.max() * 1.15 if not colaborador_counts.empty else 10]))
                st.plotly_chart(fig_colab, use_container_width=True)

            with col2:
                st.subheader("Top 10 Estados - Quantidade de OS")
                estado_counts = df_filtrado['Cliente - Estado'].value_counts().head(10)
                fig_estados = px.bar(x=estado_counts.index, y=estado_counts.values, text=estado_counts.values)
                fig_estados.update_traces(textposition='outside', texttemplate='%{text}', marker_color='#1f77b4')
                fig_estados.update_layout(height=400, xaxis_title="Estados", yaxis_title="Quantidade de OS", xaxis_tickangle=-45, yaxis=dict(range=[0, estado_counts.max() * 1.15 if not estado_counts.empty else 10]))
                st.plotly_chart(fig_estados, use_container_width=True)

            # Gráfico de Equipamentos
            st.subheader("Top 10 Equipamentos com mais OS")
            df_equipamentos = df_filtrado.explode('Etiquetas_Processadas')
            equipamentos_counts = df_equipamentos['Etiquetas_Processadas'].value_counts().head(10)
            if not equipamentos_counts.empty:
                fig_equip = px.bar(x=equipamentos_counts.index, y=equipamentos_counts.values, text=equipamentos_counts.values)
                fig_equip.update_traces(textposition='outside', texttemplate='%{text}', marker_color='#ff7f0e')
                fig_equip.update_layout(height=400, xaxis_title="Equipamentos", yaxis_title="Quantidade de OS", xaxis_tickangle=-45, yaxis=dict(range=[0, equipamentos_counts.max() * 1.15]))
                st.plotly_chart(fig_equip, use_container_width=True)
            else:
                st.info("Nenhum equipamento encontrado para os filtros selecionados.")

                # --- GRÁFICO FALHA, CAUSA E AÇÃO ---
        st.subheader("📊 Análise de Falhas, Causas e Ações Corretivas")

        # >>>>>>>> AJUSTE AQUI: DESCONSIDERAR "archived" == "VERDADEIRO" <<<<<<<<
        if 'archived' in respostas.columns:
            respostas = respostas[respostas['archived'] != "VERDADEIRO"]

        if 'name' in respostas.columns and 'title' in respostas.columns and 'answer' in respostas.columns:
            # Identificar coluna de vínculo
            link_column_name = None
            for col_name in ['id_OS', 'order', 'order.id']:
                if col_name in respostas.columns:
                    link_column_name = col_name
                    break
            if link_column_name:
                # 1. Filtrar formulários que contém "FALHA" na coluna "name"
                formularios_falha = respostas[respostas['name'].str.contains('FALHA', case=False, na=False)]
                # Filtrar apenas as OS que estão no período selecionado
                os_ids_filtradas = set(df_filtrado['id'].tolist())
                formularios_falha = formularios_falha[formularios_falha[link_column_name].isin(os_ids_filtradas)]
                if not formularios_falha.empty:
                    # 2. Localizar falhas
                    df_falhas = formularios_falha[
                        formularios_falha['title'].str.strip() == 'QUAL A FALHA DO EQUIPAMENTO?'
                    ].copy()
                    # 3. Localizar causas
                    df_causas = formularios_falha[
                        formularios_falha['title'].str.contains('QUAL A CAUSA DA FALHA', case=False, na=False)
                    ].copy()
                    # 4. Localizar ações
                    acoes_titles = [
                        'QUAL A AÇÃO TOMADA PARA RESOLVER O PROBLEMA?',
                        'QUAL AÇÃO FOI TOMADA?',
                        'QUAL A AÇÃO TOMADA?'
                    ]
                    df_acoes = formularios_falha[
                        formularios_falha['title'].isin(acoes_titles)
                    ].copy()
                    # Processar ações separadas por "&" - criar registros separados
                    acoes_expandidas = []
                    for _, row in df_acoes.iterrows():
                        acoes = str(row['answer']).split('&')
                        for acao in acoes:
                            acao_limpa = acao.strip()
                            if acao_limpa and acao_limpa.lower() != 'nan':
                                nova_row = row.copy()
                                nova_row['answer'] = acao_limpa
                                acoes_expandidas.append(nova_row)
                    if acoes_expandidas:
                        df_acoes_processadas = pd.DataFrame(acoes_expandidas)
                    else:
                        df_acoes_processadas = pd.DataFrame()
                    # Inicializar session_state para filtros se não existir
                    if 'filtro_falha' not in st.session_state:
                        st.session_state.filtro_falha = 'Todas'
                    if 'filtro_causa' not in st.session_state:
                        st.session_state.filtro_causa = 'Todas'
                    if 'filtro_acao' not in st.session_state:
                        st.session_state.filtro_acao = 'Todas'
                    # Botão para limpar filtros (deve vir antes dos selectbox)
                    if st.button("🔄 Limpar Filtros"):
                        st.session_state.filtro_falha = 'Todas'
                        st.session_state.filtro_causa = 'Todas'
                        st.session_state.filtro_acao = 'Todas'
                        st.rerun()
                    # Filtros interativos interdependentes
                    col_filtro1, col_filtro2, col_filtro3 = st.columns([1, 1, 1])
                    with col_filtro1:
                        # Filtro de Falhas (sempre mostra todas as opções disponíveis)
                        falhas_unicas = df_falhas['answer'].dropna().unique() if not df_falhas.empty else []
                        falhas_disponiveis = ['Todas'] + sorted([str(f) for f in falhas_unicas])
                        falha_selecionada = st.selectbox(
                            "🚨 Selecionar Falha:",
                            falhas_disponiveis,
                            index=falhas_disponiveis.index(st.session_state.filtro_falha) if st.session_state.filtro_falha in falhas_disponiveis else 0,
                            key="select_falha"
                        )
                        st.session_state.filtro_falha = falha_selecionada
                    with col_filtro2:
                        # Filtro de Causas (limitado pelas falhas selecionadas)
                        if falha_selecionada != 'Todas':
                            # Buscar OS que têm a falha selecionada
                            os_com_falha = set(df_falhas[df_falhas['answer'] == falha_selecionada][link_column_name].tolist())
                            # Filtrar causas apenas para essas OS
                            causas_filtradas = df_causas[df_causas[link_column_name].isin(os_com_falha)]
                            causas_unicas = causas_filtradas['answer'].dropna().unique()
                        else:
                            causas_unicas = df_causas['answer'].dropna().unique() if not df_causas.empty else []
                        causas_disponiveis = ['Todas'] + sorted([str(c) for c in causas_unicas])
                        # Se a causa atual não está mais disponível, resetar para 'Todas'
                        if st.session_state.filtro_causa not in causas_disponiveis:
                            st.session_state.filtro_causa = 'Todas'
                        causa_selecionada = st.selectbox(
                            "🔍 Selecionar Causa:",
                            causas_disponiveis,
                            index=causas_disponiveis.index(st.session_state.filtro_causa) if st.session_state.filtro_causa in causas_disponiveis else 0,
                            key="select_causa"
                        )
                        st.session_state.filtro_causa = causa_selecionada
                    with col_filtro3:
                        # Filtro de Ações (limitado pelas causas selecionadas)
                        os_para_acoes = set(os_ids_filtradas)
                        # Se há falha selecionada, limitar às OS com essa falha
                        if falha_selecionada != 'Todas':
                            os_com_falha = set(df_falhas[df_falhas['answer'] == falha_selecionada][link_column_name].tolist())
                            os_para_acoes &= os_com_falha
                        # Se há causa selecionada, limitar às OS com essa causa
                        if causa_selecionada != 'Todas':
                            os_com_causa = set(df_causas[df_causas['answer'] == causa_selecionada][link_column_name].tolist())
                            os_para_acoes &= os_com_causa
                        # Filtrar ações para as OS resultantes
                        if not df_acoes_processadas.empty:
                            acoes_filtradas = df_acoes_processadas[df_acoes_processadas[link_column_name].isin(os_para_acoes)]
                            acoes_unicas = acoes_filtradas['answer'].dropna().unique()
                        else:
                            acoes_unicas = []
                        acoes_disponiveis = ['Todas'] + sorted([str(a) for a in acoes_unicas])
                        # Se a ação atual não está mais disponível, resetar para 'Todas'
                        if st.session_state.filtro_acao not in acoes_disponiveis:
                            st.session_state.filtro_acao = 'Todas'
                        acao_selecionada = st.selectbox(
                            "🔧 Selecionar Ação:",
                            acoes_disponiveis,
                            index=acoes_disponiveis.index(st.session_state.filtro_acao) if st.session_state.filtro_acao in acoes_disponiveis else 0,
                            key="select_acao"
                        )
                        st.session_state.filtro_acao = acao_selecionada
                    st.markdown("---")
                    # Aplicar filtros aos datasets originais
                    os_filtradas_por_criterio = set(os_ids_filtradas)
                    if falha_selecionada != 'Todas':
                        os_com_falha = set(df_falhas[df_falhas['answer'] == falha_selecionada][link_column_name].tolist())
                        os_filtradas_por_criterio &= os_com_falha
                    if causa_selecionada != 'Todas':
                        os_com_causa = set(df_causas[df_causas['answer'] == causa_selecionada][link_column_name].tolist())
                        os_filtradas_por_criterio &= os_com_causa
                    if acao_selecionada != 'Todas':
                        os_com_acao = set(df_acoes_processadas[df_acoes_processadas['answer'] == acao_selecionada][link_column_name].tolist())
                        os_filtradas_por_criterio &= os_com_acao
                    # Filtrar datasets pelas OS que atendem a todos os critérios
                    df_falhas_filtrado = df_falhas[df_falhas[link_column_name].isin(os_filtradas_por_criterio)]
                    df_causas_filtrado = df_causas[df_causas[link_column_name].isin(os_filtradas_por_criterio)]
                    df_acoes_filtrado = df_acoes_processadas[df_acoes_processadas[link_column_name].isin(os_filtradas_por_criterio)]
                    # Gráficos independentes
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.subheader("🚨 Top 10 Falhas")
                        if not df_falhas_filtrado.empty:
                            falhas_counts = df_falhas_filtrado['answer'].value_counts().head(10)
                            if not falhas_counts.empty:
                                fig_falhas = px.bar(
                                    y=falhas_counts.index,
                                    x=falhas_counts.values,
                                    orientation='h',
                                    text=falhas_counts.values,
                                    title=f"Total de Respostas: {falhas_counts.sum()}"
                                )
                                fig_falhas.update_layout(
                                    yaxis={'categoryorder':'total ascending'},
                                    height=500,
                                    xaxis_title="Quantidade",
                                    yaxis_title="",
                                    showlegend=False,
                                    margin=dict(l=20, r=100, t=60, b=40)
                                )
                                # Calcular range adequado para o eixo X
                                max_val = falhas_counts.max()
                                fig_falhas.update_xaxes(range=[0, max_val * 1.25])
                                # Configurar texto das barras
                                fig_falhas.update_traces(
                                    textposition='outside',
                                    textfont_size=10
                                )
                                st.plotly_chart(fig_falhas, use_container_width=True)
                            else:
                                st.info("Nenhuma falha encontrada.")
                        else:
                            st.info("Nenhuma falha encontrada com os filtros aplicados.")
                    with col2:
                        st.subheader("🔍 Top 10 Causas")
                        if not df_causas_filtrado.empty:
                            causas_counts = df_causas_filtrado['answer'].value_counts().head(10)
                            if not causas_counts.empty:
                                fig_causas = px.bar(
                                    y=causas_counts.index,
                                    x=causas_counts.values,
                                    orientation='h',
                                    text=causas_counts.values,
                                    title=f"Total de Respostas: {causas_counts.sum()}"
                                )
                                fig_causas.update_layout(
                                    yaxis={'categoryorder':'total ascending'},
                                    height=500,
                                    xaxis_title="Quantidade",
                                    yaxis_title="",
                                    showlegend=False,
                                    margin=dict(l=20, r=100, t=60, b=40)
                                )
                                # Calcular range adequado para o eixo X
                                max_val = causas_counts.max()
                                fig_causas.update_xaxes(range=[0, max_val * 1.25])
                                # Configurar texto das barras
                                fig_causas.update_traces(
                                    textposition='outside',
                                    textfont_size=10
                                )
                                st.plotly_chart(fig_causas, use_container_width=True)
                            else:
                                st.info("Nenhuma causa encontrada.")
                        else:
                            st.info("Nenhuma causa encontrada com os filtros aplicados.")
                    with col3:
                        st.subheader("🔧 Top 10 Ações Corretivas")
                        if not df_acoes_filtrado.empty:
                            acoes_counts = df_acoes_filtrado['answer'].value_counts().head(10)
                            if not acoes_counts.empty:
                                fig_acoes = px.bar(
                                    y=acoes_counts.index,
                                    x=acoes_counts.values,
                                    orientation='h',
                                    text=acoes_counts.values,
                                    title=f"Total de Respostas: {acoes_counts.sum()}"
                                )
                                fig_acoes.update_layout(
                                    yaxis={'categoryorder':'total ascending'},
                                    height=500,
                                    xaxis_title="Quantidade",
                                    yaxis_title="",
                                    showlegend=False,
                                    margin=dict(l=20, r=100, t=60, b=40)
                                )
                                # Calcular range adequado para o eixo X
                                max_val = acoes_counts.max()
                                fig_acoes.update_xaxes(range=[0, max_val * 1.25])
                                # Configurar texto das barras
                                fig_acoes.update_traces(
                                    textposition='outside',
                                    textfont_size=10
                                )
                                st.plotly_chart(fig_acoes, use_container_width=True)
                            else:
                                st.info("Nenhuma ação encontrada.")
                        else:
                            st.info("Nenhuma ação encontrada com os filtros aplicados.")
                    # Resumo dos filtros aplicados
                    filtros_ativos = []
                    if falha_selecionada != 'Todas':
                        filtros_ativos.append(f"Falha: {falha_selecionada}")
                    if causa_selecionada != 'Todas':
                        filtros_ativos.append(f"Causa: {causa_selecionada}")
                    if acao_selecionada != 'Todas':
                        filtros_ativos.append(f"Ação: {acao_selecionada}")
                    if filtros_ativos:
                        st.info(f"🔍 **Filtros Aplicados:** {' | '.join(filtros_ativos)}")
                    # Debug info (remova depois de testar)
                    if st.checkbox("🔍 Mostrar informações de debug"):
                        st.write("**Total de respostas de falha encontradas:**", len(df_falhas))
                        st.write("**Total de respostas de causa encontradas:**", len(df_causas))
                        st.write("**Total de respostas de ação encontradas:**", len(df_acoes_processadas))
                        st.write("**OS filtradas pelo período:**", len(os_ids_filtradas))
                        st.write("**OS filtradas pelos critérios:**", len(os_filtradas_por_criterio))
                else:
                    st.info("Nenhum formulário de falha encontrado para as OS filtradas.")
            else:
                st.warning("Não foi possível encontrar uma coluna de vínculo ('id_OS', 'order' ou 'order.id') na tabela de respostas.")
        else:
            st.warning("As colunas 'name', 'title' e/ou 'answer' não foram encontradas na tabela de respostas.")


            # --- ANÁLISE DE RECORRÊNCIA DE PROBLEMAS ---
        st.subheader("🔄 Análise de Recorrência de Problemas")

        # Verificar se temos as colunas necessárias
        required_columns = ['colaborador_nome', 'order']
        date_columns = ['createdAt', 'startedAt', 'completedAt', 'updatedAt']

        # Encontrar coluna de data disponível
        date_column = None
        for col in date_columns:
            if col in atividades.columns:
                date_column = col
                break

        if all(col in atividades.columns for col in required_columns) and date_column:
            
            # Fazer merge com a tabela de ordens de serviço para obter o tipo de serviço
            if 'ordens_servico' in locals() and ordens_servico is not None:
                # Verificar se as colunas necessárias existem
                if 'id' in ordens_servico.columns and 'Tipo de Serviço' in ordens_servico.columns:
                    
                    # Merge das tabelas
                    atividades_com_tipo = atividades.merge(
                        ordens_servico[['id', 'Tipo de Serviço']], 
                        left_on='order', 
                        right_on='id', 
                        how='left'
                    )
                    
                else:
                    st.error("Colunas 'id' ou 'Tipo de Serviço' não encontradas na tabela ordens_servico")
                    st.info(f"Colunas disponíveis em ordens_servico: {list(ordens_servico.columns)}")
                    atividades_com_tipo = atividades.copy()
            else:
                st.error("Tabela 'ordens_servico' não encontrada. Certifique-se de carregar o arquivo ordens_de_servico.xlsx")
                atividades_com_tipo = atividades.copy()
            
            # Converter coluna de data para datetime e remover timezone para comparação
            atividades_com_tipo[date_column] = pd.to_datetime(atividades_com_tipo[date_column]).dt.tz_localize(None)
            
            # Converter datas de filtro para datetime
            data_inicio_dt = pd.to_datetime(data_inicio)
            data_fim_dt = pd.to_datetime(data_fim)
            
            # Filtrar atividades do período selecionado
            atividades_periodo = atividades_com_tipo[
                (atividades_com_tipo[date_column] >= data_inicio_dt) & 
                (atividades_com_tipo[date_column] <= data_fim_dt)
            ].copy()
            
            # Filtrar apenas tipo de serviço "Garantia" se a coluna existir
            if 'Tipo de Serviço' in atividades_periodo.columns:
                # Filtrar por Garantia (case-insensitive)
                atividades_periodo = atividades_periodo[
                    atividades_periodo['Tipo de Serviço'].str.upper().str.contains('GARANTIA', na=False)
                ].copy()
            else:
                st.warning(f"⚠️ Coluna 'Tipo de Serviço' não encontrada após merge.")
                st.info(f"Análise será feita com todos os tipos de serviço")
            
            if not atividades_periodo.empty:
                
                # Agrupar por OS e identificar primeira atividade e total de atividades
                recorrencia_data = []
                
                for os_id in atividades_periodo['order'].unique():
                    # Filtrar apenas valores não nulos para order
                    if pd.isna(os_id):
                        continue
                        
                    atividades_os = atividades_periodo[
                        atividades_periodo['order'] == os_id
                    ].sort_values(date_column)
                    
                    if len(atividades_os) > 0:
                        # Primeira atividade (responsável)
                        primeira_atividade = atividades_os.iloc[0]
                        colaborador_responsavel = primeira_atividade['colaborador_nome']
                        
                        # Verificar se colaborador não é nulo
                        if pd.isna(colaborador_responsavel):
                            colaborador_responsavel = "Não informado"
                        
                        # Total de atividades na OS
                        total_atividades = len(atividades_os)
                        
                        # Recorrências = total - 1 (primeira não conta como recorrência)
                        recorrencias = total_atividades - 1
                        
                        recorrencia_data.append({
                            'order_id': os_id,
                            'colaborador_responsavel': colaborador_responsavel,
                            'total_atividades': total_atividades,
                            'recorrencias': recorrencias,
                            'data_primeira_atividade': primeira_atividade[date_column],
                            'tipo_servico': primeira_atividade.get('Tipo de Serviço', 'N/A')
                        })
                
                if recorrencia_data:
                    df_recorrencia = pd.DataFrame(recorrencia_data)
                    
                    # Calcular métricas por colaborador
                    metricas_colaborador = df_recorrencia.groupby('colaborador_responsavel').agg({
                        'order_id': 'count',  # Total de OS iniciadas
                        'recorrencias': ['sum', 'mean'],  # Total e média de recorrências
                        'total_atividades': 'sum'  # Total de atividades geradas
                    }).round(2)
                    
                    # Flatten column names
                    metricas_colaborador.columns = [
                        'os_iniciadas', 'total_recorrencias', 'media_recorrencias', 'total_atividades'
                    ]
                    
                    # Calcular percentual de OS com recorrência
                    os_com_recorrencia = df_recorrencia.groupby('colaborador_responsavel').apply(
                        lambda x: (x['recorrencias'] > 0).sum()
                    )
                    
                    metricas_colaborador['os_com_recorrencia'] = os_com_recorrencia
                    metricas_colaborador['percentual_recorrencia'] = (
                        (metricas_colaborador['os_com_recorrencia'] / metricas_colaborador['os_iniciadas']) * 100
                    ).round(2)
                    
                    # Reset index para usar como dataframe normal
                    metricas_colaborador = metricas_colaborador.reset_index()
                    
                    # VISUALIZAÇÕES
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("📊 Média de Recorrências por Colaborador")
                        if len(metricas_colaborador) > 0:
                            # Ordenar por média de recorrências (decrescente) para o gráfico
                            dados_grafico_media = metricas_colaborador.sort_values('media_recorrencias', ascending=False)
                            
                            fig_media = px.bar(
                                dados_grafico_media.head(15),
                                y='colaborador_responsavel',
                                x='media_recorrencias',
                                orientation='h',
                                text='media_recorrencias',
                                title="Top 15 - Média de Recorrências",
                                color='media_recorrencias',
                                color_continuous_scale='Reds'
                            )
                            
                            fig_media.update_layout(
                                yaxis={'categoryorder':'total ascending'},
                                height=600,
                                xaxis_title="Média de Recorrências",
                                yaxis_title="",
                                showlegend=False
                            )
                            
                            fig_media.update_traces(
                                textposition='auto',
                                textfont_size=10
                            )
                            
                            st.plotly_chart(fig_media, use_container_width=True)
                    
                    with col2:
                        st.subheader("📈 Percentual de OS com Recorrência")
                        if len(metricas_colaborador) > 0:
                            # Ordenar por percentual de recorrência (decrescente) para o gráfico
                            dados_grafico_percent = metricas_colaborador.sort_values('percentual_recorrencia', ascending=False)
                            
                            fig_percent = px.bar(
                                dados_grafico_percent.head(15),
                                y='colaborador_responsavel',
                                x='percentual_recorrencia',
                                orientation='h',
                                text='percentual_recorrencia',
                                title="Top 15 - % OS com Recorrência",
                                color='percentual_recorrencia',
                                color_continuous_scale='Oranges'
                            )
                            
                            fig_percent.update_layout(
                                yaxis={'categoryorder':'total ascending'},
                                height=600,
                                xaxis_title="Percentual (%)",
                                yaxis_title="",
                                showlegend=False
                            )
                            
                            fig_percent.update_traces(
                                textposition='auto',
                                textfont_size=10,
                                texttemplate='%{text}%'
                            )
                            
                            st.plotly_chart(fig_percent, use_container_width=True)
                    
                    # RESUMO GERAL
                    st.subheader("📋 Resumo Geral")
                    col_resumo1, col_resumo2, col_resumo3, col_resumo4 = st.columns(4)
                    
                    total_os = len(df_recorrencia)
                    total_os_com_recorrencia = len(df_recorrencia[df_recorrencia['recorrencias'] > 0])
                    media_geral_recorrencias = df_recorrencia['recorrencias'].mean()
                    percentual_geral_recorrencia = (total_os_com_recorrencia / total_os * 100) if total_os > 0 else 0
                    
                    with col_resumo1:
                        st.metric("Total de OS", total_os)
                    
                    with col_resumo2:
                        st.metric("OS com Recorrência", total_os_com_recorrencia)
                    
                    with col_resumo3:
                        st.metric("Média Geral de Recorrências", f"{media_geral_recorrencias:.2f}")
                    
                    with col_resumo4:
                        st.metric("% Geral de Recorrência", f"{percentual_geral_recorrencia:.1f}%")
                    
                    # TABELA DETALHADA COM OPÇÃO DE ORDENAÇÃO
                    st.subheader("📊 Ranking Detalhado por Colaborador")
                    
                    # Opção para escolher ordenação
                    col_ordem1, col_ordem2 = st.columns(2)
                    with col_ordem1:
                        criterio_ordenacao = st.selectbox(
                            "Ordenar por:",
                            ['% Recorrência', 'Média Recorrências', 'Total Recorrências', 'OS Iniciadas'],
                            key="ordem_recorrencia"
                        )
                    
                    with col_ordem2:
                        ordem_crescente = st.selectbox(
                            "Ordem:",
                            ['Decrescente', 'Crescente'],
                            key="tipo_ordem_recorrencia"
                        )
                    
                    # Aplicar ordenação baseada na seleção
                    if criterio_ordenacao == '% Recorrência':
                        metricas_ordenadas = metricas_colaborador.sort_values(
                            'percentual_recorrencia', 
                            ascending=(ordem_crescente == 'Crescente')
                        )
                    elif criterio_ordenacao == 'Média Recorrências':
                        metricas_ordenadas = metricas_colaborador.sort_values(
                            'media_recorrencias', 
                            ascending=(ordem_crescente == 'Crescente')
                        )
                    elif criterio_ordenacao == 'Total Recorrências':
                        metricas_ordenadas = metricas_colaborador.sort_values(
                            'total_recorrencias', 
                            ascending=(ordem_crescente == 'Crescente')
                        )
                    else:  # OS Iniciadas
                        metricas_ordenadas = metricas_colaborador.sort_values(
                            'os_iniciadas', 
                            ascending=(ordem_crescente == 'Crescente')
                        )
                    
                    # Preparar tabela para exibição
                    tabela_display = metricas_ordenadas.copy()
                    tabela_display.columns = [
                        'Colaborador', 'OS Iniciadas', 'Total Recorrências', 
                        'Média Recorrências', 'Total Atividades', 'OS c/ Recorrência', '% Recorrência'
                    ]
                    
                    # Formatar colunas
                    tabela_display['Média Recorrências'] = tabela_display['Média Recorrências'].apply(lambda x: f"{x:.2f}")
                    tabela_display['% Recorrência'] = tabela_display['% Recorrência'].apply(lambda x: f"{x:.1f}%")
                    
                    st.dataframe(
                        tabela_display,
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # ANÁLISE POR PERÍODO (OPCIONAL)
                    if st.checkbox("📅 Mostrar Análise Temporal de Recorrências"):
                        st.subheader("📅 Evolução das Recorrências por Período")
                        
                        # Adicionar coluna de mês/ano para análise temporal
                        df_recorrencia['mes_ano'] = df_recorrencia['data_primeira_atividade'].dt.to_period('M')
                        
                        evolucao_temporal = df_recorrencia.groupby('mes_ano').agg({
                            'order_id': 'count',
                            'recorrencias': ['sum', 'mean']
                        }).round(2)
                        
                        evolucao_temporal.columns = ['total_os', 'total_recorrencias', 'media_recorrencias']
                        evolucao_temporal = evolucao_temporal.reset_index()
                        evolucao_temporal['mes_ano_str'] = evolucao_temporal['mes_ano'].astype(str)
                        
                        fig_temporal = px.line(
                            evolucao_temporal,
                            x='mes_ano_str',
                            y='media_recorrencias',
                            title="Evolução da Média de Recorrências por Mês",
                            markers=True
                        )
                        
                        fig_temporal.update_layout(
                            xaxis_title="Período",
                            yaxis_title="Média de Recorrências",
                            height=400
                        )
                        
                        st.plotly_chart(fig_temporal, use_container_width=True)
                    
                else:
                    st.info("Nenhum dado de recorrência encontrado para o período selecionado.")
            
            else:
                st.info("Nenhuma atividade de Garantia encontrada para o período selecionado.")

        else:
            missing_cols = [col for col in required_columns if col not in atividades.columns]
            st.error(f"Colunas necessárias não encontradas: {missing_cols}. Coluna de data também é necessária.")
            if not date_column:
                st.error(f"Nenhuma coluna de data encontrada. Colunas procuradas: {date_columns}")
                st.info(f"Colunas disponíveis no dataset: {list(atividades.columns)}")         

                                # --- SEÇÃO AGENDA DOS TÉCNICOS ---
        st.markdown("---")
        st.header("🗓️ Agenda dos Técnicos")

        data_agenda = st.date_input(
            "Selecione uma data para ver a agenda",
            datetime.now(),
            format="DD/MM/YYYY"
        )

        os_garantia_ids = ordens_servico['id'].unique()
        atividades_agendadas = atividades[
            (atividades['order'].isin(os_garantia_ids)) &
            (atividades['archived'] == False) &
            (atividades['scheduling'].notna())
        ].copy()

        if not atividades_agendadas.empty and data_agenda:
            fuso_horario_br = 'America/Sao_Paulo'
            data_selecionada_tz = pd.Timestamp(data_agenda, tz=fuso_horario_br)
            atividades_do_dia = atividades_agendadas[atividades_agendadas['scheduling'].dt.date == data_selecionada_tz.date()].copy()

            # Certifique-se de preservar o id da atividade
            if 'id' in atividades_do_dia.columns:
                atividades_do_dia = atividades_do_dia.rename(columns={'id': 'atividade_id'})

            # Primeiro merge: liga atividade com OS
            agenda_df = pd.merge(
                atividades_do_dia,
                df_filtrado[['id', 'Numero OS', 'Cliente']],
                left_on='order',
                right_on='id',
                how='left',
                suffixes=('', '_os')
            )
            agenda_df.dropna(subset=['Numero OS'], inplace=True)

            # Segundo merge: liga avaliação pela chave correta
            avaliacoes_cols = ['task.id', 'stars', 'comment', 'createdAt']
            avaliacoes_garantia['createdAt'] = pd.to_datetime(avaliacoes_garantia['createdAt'])
            idx = avaliacoes_garantia.groupby('task.id')['createdAt'].idxmax()
            avaliacoes_latest = avaliacoes_garantia.loc[idx, avaliacoes_cols]
            agenda_df = pd.merge(
                agenda_df,
                avaliacoes_latest,
                left_on="atividade_id",
                right_on="task.id",
                how="left"
            )

            # Cria a coluna com o status da avaliação usando emoji
            agenda_df['Status Avaliação'] = agenda_df['stars'].apply(lambda x: '✅' if pd.notna(x) else '⚠️')

            # Funções auxiliares
            def criar_url_mapa(coords):
                if pd.notna(coords) and isinstance(coords, str) and ',' in coords:
                    coords_limpas = coords.replace(" ", "")
                    return f"https://www.google.com/maps/search/?api=1&query={coords_limpas}"
                return None

            def link_estrela(link):
                if pd.notna(link) and isinstance(link, str) and link.strip():
                    return link
                return None

            agenda_df['map_url'] = agenda_df['coords'].apply(criar_url_mapa)
            agenda_df['rating_link'] = agenda_df['ratingLink'].apply(link_estrela)

            # Seleciona só as colunas que existem
            cols_necessarios = [
                'scheduling',
                'Cliente',
                'colaborador_nome',
                'Numero OS',
                'map_url',
                'rating_link',
                'Status Avaliação'
            ]
            cols_existentes = [c for c in cols_necessarios if c in agenda_df.columns]
            agenda_display = agenda_df[cols_existentes].copy()

            # Renomeia colunas para exibição, só até o número existente!
            nomes_exibicao = [
                'Horário',
                'Cliente',
                'Técnico',
                'Número OS',
                'Localização',
                'Link Avaliação',
                'Status Avaliação'
            ]
            agenda_display.columns = nomes_exibicao[:len(agenda_display.columns)]

            if 'Horário' in agenda_display.columns:
                agenda_display = agenda_display.sort_values(by='Horário')
                agenda_display['Horário'] = pd.to_datetime(agenda_display['Horário'], errors='coerce').dt.strftime('%H:%M')
            if 'Data Avaliação' in agenda_display.columns:
                agenda_display['Data Avaliação'] = pd.to_datetime(agenda_display['Data Avaliação'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')

            st.dataframe(
                agenda_display,
                column_config={
                    "Horário": st.column_config.TimeColumn(
                        "Horário",
                        format="HH:mm",
                    ),
                    "Localização": st.column_config.LinkColumn(
                        "Localização",
                        help="Clique para abrir o local no Google Maps",
                        display_text="🗺️"
                    ),
                    "Link Avaliação": st.column_config.LinkColumn(
                        "Avaliação",
                        help="Clique para avaliar o atendimento",
                        display_text="⭐"
                    ),
                    "Status Avaliação": st.column_config.TextColumn(
                        "Status Avaliação",
                        help="Situação da avaliação: ✅ feita, ⚠️ pendente"
                    ),
                    "Estrelas": st.column_config.TextColumn(
                        "Estrelas",
                        help="Nota do cliente de 1 a 5 estrelas"
                    ),
                    "Comentário": st.column_config.TextColumn(
                        "Comentário",
                        help="Comentário do cliente, se houver"
                    ),
                    "Data Avaliação": st.column_config.DatetimeColumn(
                        "Data Avaliação",
                        format="DD/MM/YYYY HH:mm"
                    )
                },
                hide_index=True,
                use_container_width=False
            )

        else:
            st.info(f"Nenhuma atividade agendada para o dia {data_agenda.strftime('%d/%m/%Y')}.")
        st.markdown("---")


                        # --- INDICADORES DE AVALIAÇÃO DE ATENDIMENTO ---

        # 1. CARREGUE as ordens de serviço, GARANTA o nome certo da coluna do número OS!
        # ordens_servico = pd.read_excel('ordens_de_servico.xlsx')
        # print(ordens_servico.columns) # Descomente para conferir o nome real da coluna B!
        # Exemplo: coluna B chama-se 'Numero OS' (mude abaixo se o nome for outro!)
        # Exemplo de coluna do nome do cliente: 'Cliente' (mude se necessário!)

        # 2. Merge atividades para trazer 'order', colaborador, NÚMERO da OS e Cliente
        atividades_aux = atividades[['id', 'order', 'colaborador_nome']].merge(
            ordens_servico[['id', 'Numero OS', 'Cliente']],  # <-- AJUSTE 'Cliente' se for outro nome!
            left_on='order',
            right_on='id',
            how='left',
            suffixes=('', '_os'),
        )

        # 3. Merge avaliações com atividades (agora já temos colaborador_nome, Numero OS e Cliente)
        avaliacoes_com_order = pd.merge(
            avaliacoes_garantia,
            atividades_aux[['id', 'order', 'colaborador_nome', 'Numero OS', 'Cliente']],
            left_on='task.id',
            right_on='id',
            how='left'
        )

        # 4. Filtra avaliações apenas das OS filtradas
        avaliacoes_filtradas = avaliacoes_com_order[avaliacoes_com_order['order'].isin(df_filtrado['id'])].copy()
        avaliacoes_filtradas['createdAt'] = pd.to_datetime(avaliacoes_filtradas['createdAt'], errors='coerce')

        # 5. Indicadores gerais
        nota_media_geral = avaliacoes_filtradas['stars'].mean()
        total_avaliacoes = avaliacoes_filtradas.shape[0]
        distribuicao_notas = avaliacoes_filtradas['stars'].value_counts(normalize=True).sort_index() * 100

        # --- RANKING de técnicos visual: posição + emoji na MESMA coluna + quantidade de avaliações, com empate por média/avalições ---
        ranking_colaboradores = (
            avaliacoes_filtradas
            .groupby('colaborador_nome')
            .agg({'stars': ['mean', 'count']})
            .reset_index()
        )
        ranking_colaboradores.columns = ['Técnico', 'Nota Média', 'Avaliações']
        # NOVA ORDENAÇÃO: primeiro por Nota Média (desc), depois Avaliações (desc), depois nome (asc, só para ordenação estável)
        ranking_colaboradores = ranking_colaboradores.sort_values(
            by=['Nota Média', 'Avaliações', 'Técnico'],
            ascending=[False, False, True]
        ).reset_index(drop=True)
        # Ranking numérico: empate considerando os dois critérios
        ranking_colaboradores['PosiçãoNum'] = ranking_colaboradores[['Nota Média', 'Avaliações']].apply(
            lambda x: tuple(x), axis=1
        ).rank(method='min', ascending=False).astype(int)
        def get_icone(posnum):
            if posnum == 1: return '🏆'
            if posnum == 2: return '🥈'
            if posnum == 3: return '🥉'
            return ''
        ranking_colaboradores['Posição'] = ranking_colaboradores['PosiçãoNum'].astype(str) + 'º ' + ranking_colaboradores['PosiçãoNum'].apply(get_icone)
        ranking_colaboradores = ranking_colaboradores[['Posição', 'Técnico', 'Nota Média', 'Avaliações']]

        # --- Evolução mensal, garantindo sempre x como string ---
        if not avaliacoes_filtradas.empty and avaliacoes_filtradas['createdAt'].notnull().any():
            avaliacoes_filtradas['mes_ano'] = avaliacoes_filtradas['createdAt'].dt.strftime('%Y-%m')
            evolucao_nota = (
                avaliacoes_filtradas
                .groupby('mes_ano')['stars']
                .mean()
                .sort_index()
            )
        else:
            evolucao_nota = pd.Series([], dtype=float)

        # --- Comentários recentes (DATA FORMATADA + ESTRELAS + NÚMERO REAL OS + CLIENTE) ---
        comentarios_recentes = avaliacoes_filtradas[['createdAt', 'Numero OS', 'Cliente', 'colaborador_nome', 'stars', 'comment']]
        comentarios_recentes = comentarios_recentes.sort_values('createdAt', ascending=False).head(10)
        comentarios_recentes['Data'] = comentarios_recentes['createdAt'].dt.strftime('%d/%m/%Y %H:%M:%S')
        # Função nota em estrelas cheias e vazias
        def estrelas_icone(star):
            try:
                n = int(star)
                return '⭐' * n + '☆' * (5 - n)
            except:
                return ''
        comentarios_recentes['Estrelas'] = comentarios_recentes['stars'].apply(estrelas_icone)
        comentarios_recentes = comentarios_recentes.rename(columns={
            'Numero OS': 'Nº OS',  # <- Mostra o número REAL da OS
            'Cliente': 'Cliente',
            'colaborador_nome': 'Técnico',
            'comment': 'Comentário'
        })[['Data', 'Nº OS', 'Cliente', 'Técnico', 'Estrelas', 'Comentário']]

        # --- EXIBIÇÃO STREAMLIT ---
        st.markdown("## ⭐ Indicadores de Avaliação de Atendimento")
        col1, col2, col3 = st.columns(3)
        col1.metric("Nota Média", f"{nota_media_geral:.2f}" if not np.isnan(nota_media_geral) else "-")
        col2.metric("Avaliações Recebidas", f"{total_avaliacoes}")
        col3.metric("Avaliações 5⭐", f"{distribuicao_notas.get(5, 0):.1f}%")

        st.markdown("### Distribuição das Notas (%)")
        st.bar_chart(distribuicao_notas)
        st.markdown("### Ranking dos Técnicos por Nota Média")
        st.dataframe(ranking_colaboradores, hide_index=True, use_container_width=True)
        st.markdown("### Evolução Mensal da Nota Média")
        if not evolucao_nota.empty:
            st.line_chart(evolucao_nota)
        else:
            st.info("Ainda não há avaliações mensais para exibir este gráfico.")
        st.markdown("### Comentários Recentes dos Clientes")
        st.dataframe(comentarios_recentes, hide_index=True, use_container_width=True)
        st.markdown("---")

        # Tabela resumo
        st.header("📋 Tabela Resumo das OS")
        if not df_filtrado.empty:
            df_display = df_filtrado[[
                'Numero OS', 'Cliente', 'Descrição','Cliente - Estado', 'Criado em',
                'status_final', 'data_conclusao', 'os_concluida', 'link'
            ]].copy()
            df_display['os_concluida'] = df_display['os_concluida'].map({True: '✅ Sim', False: '❌ Não'})
            df_display.columns = ['Número OS', 'Cliente', 'Descrição', 'Estado', 'Criado em', 'Status Final', 'Data Conclusão', 'Concluída', 'link']

            st.dataframe(
                df_display,
                use_container_width=True,
                column_config={
                    "link": st.column_config.LinkColumn(
                        "Relatório",
                        help="Clique para abrir o relatório.",
                        display_text="📄"
                    ),
                    "Criado em": st.column_config.DatetimeColumn(
                        "Criado em",
                        format="DD/MM/YYYY HH:mm",
                    ),
                    "Data Conclusão": st.column_config.DatetimeColumn(
                        "Data Conclusão",
                        format="DD/MM/YYYY HH:mm",
                    )
                },
                hide_index=True
            )

            @st.cache_data
            def to_excel(df):
                df_export = df.copy()
                df_export['Criado em'] = df_export['Criado em'].apply(lambda x: x.strftime('%d/%m/%Y %H:%M') if pd.notna(x) else 'N/A')
                df_export['Data Conclusão'] = df_export['Data Conclusão'].apply(lambda x: x.strftime('%d/%m/%Y %H:%M') if pd.notna(x) else 'N/A')
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_export.to_excel(writer, index=False, sheet_name='OS_Filtradas')
                processed_data = output.getvalue()
                return processed_data

            excel_data = to_excel(df_display)
            st.download_button(
                label="📥 Baixar Dados Filtrados (XLSX)",
                data=excel_data,
                file_name=f"os_filtradas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("Nenhuma OS encontrada com os filtros aplicados.")
    else:
        st.error("Não foi possível carregar os dados. Verifique se todos os arquivos estão na pasta correta.")
        st.info("Arquivos necessários: ordens_de_servico.xlsx, atividades.xlsx, tabela_equipamentos.xlsx, tabela_respostas.xlsx, DePara Etiquetas.xlsx, DePara Estados.xlsx")
        
        
# --- INÍCIO DA PARTE DE AUTENTICAÇÃO ---
elif st.session_state["authentication_status"] is False:
    st.error('Usuário/senha incorreto')
elif st.session_state["authentication_status"] is None:
    st.warning('Por favor, insira seu usuário e senha')
# --- FIM DA PARTE DE AUTENTICAÇÃO ---