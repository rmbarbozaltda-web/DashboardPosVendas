import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

def analisar_respostas_formulario(arquivo_excel):
    """
    Análise completa e precisa dos dados do formulário
    """
    # Carregar os dados
    try:
        df = pd.read_excel(arquivo_excel)
        print(f"✅ Arquivo carregado com sucesso!")
        print(f"📊 Total de registros: {len(df)}")
        print(f"📋 Colunas disponíveis: {list(df.columns)}")
        print("-" * 80)
    except Exception as e:
        print(f"❌ Erro ao carregar arquivo: {e}")
        return
    
    # Exibir informações básicas dos dados
    print("🔍 VISÃO GERAL DOS DADOS")
    print(f"Tipos de formulários únicos: {df['name'].nunique()}")
    print(f"OSs únicas: {df['Numero OS'].nunique()}")
    print(f"Perguntas únicas: {df['title'].nunique()}")
    print("-" * 80)
    
    # Análise por tipo de formulário
    print("📝 ANÁLISE POR TIPO DE FORMULÁRIO")
    form_counts = df['name'].value_counts()
    for form_type, count in form_counts.items():
        print(f"  • {form_type}: {count} respostas")
    print("-" * 80)
    
    # Análise das perguntas mais comuns
    print("❓ TOP 10 PERGUNTAS MAIS FREQUENTES")
    question_counts = df['title'].value_counts().head(10)
    for i, (question, count) in enumerate(question_counts.items(), 1):
        print(f"  {i:2d}. {question} ({count} respostas)")
    print("-" * 80)
    
    # Análise de "Identificação do Problema" - categoria específica
    problemas_df = df[df['title'] == 'Identificação do Problema'].copy()
    
    if not problemas_df.empty:
        print("🔧 ANÁLISE DE 'IDENTIFICAÇÃO DO PROBLEMA'")
        print(f"Total de respostas para esta pergunta: {len(problemas_df)}")
        
        # Contar cada tipo de problema
        problema_counts = problemas_df['answer'].value_counts()
        
        print("\nDetalhamento por tipo de problema:")
        for i, (problema, count) in enumerate(problema_counts.items(), 1):
            print(f"  {i:2d}. {problema}: {count} ocorrências")
        
        # Verificar especificamente "Outras Peças de Refrigeração não Relacionadas (Especificar)"
        outros_problemas = problemas_df[problemas_df['answer'].str.contains(
            'Outras Peças de Refrigeração não Relacionadas', na=False
        )]
        
        print(f"\n🔍 Análise específica de 'Outros problemas':")
        print(f"Registros encontrados: {len(outros_problemas)}")
        
        if not outros_problemas.empty:
            print("\nOSs com este tipo de problema:")
            for _, row in outros_problemas.iterrows():
                print(f"  • OS: {row['Numero OS']} - Data: {row['createdAt'][:10]}")
        
        print("-" * 80)
    
    # Análise de OSs e suas respostas
    print("📋 ANÁLISE POR ORDEM DE SERVIÇO")
    os_counts = df['Numero OS'].value_counts()
    print(f"OSs com mais respostas registradas:")
    for i, (os_num, count) in enumerate(os_counts.head(5).items(), 1):
        print(f"  {i}. OS {os_num}: {count} respostas")
    print("-" * 80)
    
    # Análise temporal
    print("📅 ANÁLISE TEMPORAL")
    df['data'] = pd.to_datetime(df['createdAt']).dt.date
    data_counts = df['data'].value_counts().sort_index()
    
    print("Distribuição por data (últimas 10):")
    for data, count in data_counts.tail(10).items():
        print(f"  • {data}: {count} respostas")
    print("-" * 80)
    
    # Análise de tipos de campos
    print("📊 ANÁLISE POR TIPO DE CAMPO")
    type_counts = df['type'].value_counts()
    for field_type, count in type_counts.items():
        if pd.notna(field_type):
            print(f"  • {field_type}: {count} campos")
    print("-" * 80)
    
    # Verificação de dados específicos mencionados
    print("🔍 VERIFICAÇÃO DETALHADA DOS DADOS")
    
    # Contar registros por formulário e pergunta específica
    form_problem_analysis = df[df['title'] == 'Identificação do Problema'].groupby(['name', 'answer']).size().reset_index(name='count')
    
    if not form_problem_analysis.empty:
        print("\nProblemas por tipo de formulário:")
        for _, row in form_problem_analysis.iterrows():
            print(f"  • {row['name']} | {row['answer']}: {row['count']} casos")
    
    # Exportar resumo para Excel (opcional)
    try:
        # Criar resumo executivo
        resumo_data = {
            'Metrica': [
                'Total de Registros',
                'Total de OSs',
                'Total de Formulários',
                'Perguntas Únicas',
                'Problemas "Outros"'
            ],
            'Valor': [
                len(df),
                df['Numero OS'].nunique(),
                df['name'].nunique(),
                df['title'].nunique(),
                len(outros_problemas) if 'outros_problemas' in locals() else 0
            ]
        }
        
        resumo_df = pd.DataFrame(resumo_data)
        
        # Salvar análise detalhada
        with pd.ExcelWriter('analise_formularios.xlsx') as writer:
            resumo_df.to_excel(writer, sheet_name='Resumo_Executivo', index=False)
            form_counts.to_excel(writer, sheet_name='Formularios', header=['Quantidade'])
            if not problemas_df.empty:
                problema_counts.to_excel(writer, sheet_name='Problemas', header=['Quantidade'])
            os_counts.head(20).to_excel(writer, sheet_name='Top_OSs', header=['Respostas'])
        
        print("📄 Arquivo 'analise_formularios.xlsx' gerado com sucesso!")
        
    except Exception as e:
        print(f"⚠️  Erro ao gerar arquivo Excel: {e}")
    
    print("✅ Análise concluída!")

# Executar a análise
if __name__ == "__main__":
    # Nome do arquivo Excel
    arquivo = "tabela_respostas.xlsx"
    
    # Executar análise
    analisar_respostas_formulario(arquivo)