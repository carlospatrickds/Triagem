import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

# Configuração da página
st.set_page_config(
    page_title="Tratamento de Dados PJE - Versão Simplificada",
    page_icon="📊",
    layout="wide"
)

# Título principal
st.title("Tratamento de Dados PJE - Versão Simplificada")
st.markdown("Aplicação para processamento e análise de dados de processos judiciais.")

# Função para converter timestamp para data legível
def convert_timestamp(timestamp):
    if pd.isna(timestamp):
        return None
    try:
        if isinstance(timestamp, str):
            timestamp = float(timestamp)
        return datetime.fromtimestamp(timestamp/1000).strftime('%d/%m/%Y %H:%M')
    except:
        return timestamp

# Sidebar para upload e opções
with st.sidebar:
    st.header("Opções de Processamento")
    
    # Upload de arquivo
    uploaded_files = st.file_uploader("Carregar arquivos CSV", type=["csv"], accept_multiple_files=True)
    
    # Opções de processamento
    st.subheader("Configurações")
    
    delimiter = st.selectbox(
        "Delimitador do CSV",
        options=[";", ",", "\\t", "|"],
        index=0
    )
    
    encoding = st.selectbox(
        "Encoding",
        options=["utf-8", "latin1", "iso-8859-1", "cp1252"],
        index=0
    )
    
    # Opções de filtro
    st.subheader("Filtros")
    filter_options = st.multiselect(
        "Selecione as colunas para filtrar",
        options=["Número do Processo", "Classe", "Órgão Julgador", "Tarefa", "Prioridade"]
    )

# Função principal para processar os dados
def process_data(files, delimiter, encoding):
    if not files:
        return None
    
    # Lista para armazenar todos os dataframes
    all_dfs = []
    
    for uploaded_file in files:
        try:
            # Ler o arquivo CSV
            df = pd.read_csv(
                uploaded_file,
                delimiter=delimiter,
                encoding=encoding,
                low_memory=False,
                quotechar='"',
                on_bad_lines='skip'  # Versão mais recente do pandas usa on_bad_lines em vez de error_bad_lines
            )
            
            # Adicionar nome do arquivo como coluna
            df['Fonte'] = uploaded_file.name
            
            # Adicionar à lista de dataframes
            all_dfs.append(df)
            
        except Exception as e:
            st.error(f"Erro ao processar o arquivo {uploaded_file.name}: {e}")
    
    if not all_dfs:
        return None
    
    # Concatenar todos os dataframes
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Processar datas em formato timestamp
    if "Data Último Movimento" in combined_df.columns:
        combined_df["Data Último Movimento"] = combined_df["Data Último Movimento"].apply(convert_timestamp)
    
    # Converter colunas booleanas
    bool_columns = ["Sigiloso", "Prioridade"]
    for col in bool_columns:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].map({"true": "Sim", "false": "Não"})
    
    # Adicionar coluna de dias corridos
    if "Dias" in combined_df.columns:
        combined_df["Dias"] = pd.to_numeric(combined_df["Dias"], errors="coerce")
    
    return combined_df

# Função para aplicar filtros
def apply_filters(df, filters):
    filtered_df = df.copy()
    
    for column, values in filters.items():
        if values and column in filtered_df.columns:
            filtered_df = filtered_df[filtered_df[column].isin(values)]
    
    return filtered_df

# Função para gerar visualizações
def generate_visualizations(df):
    if df is None or df.empty:
        return
    
    st.header("Visualizações")
    
    # Dividir em duas colunas
    col1, col2 = st.columns(2)
    
    with col1:
        # Gráfico de contagem por Órgão Julgador
        if "Órgão Julgador" in df.columns:
            fig1 = px.bar(
                df["Órgão Julgador"].value_counts().reset_index(),
                x="Órgão Julgador",
                y="count",
                title="Processos por Órgão Julgador",
                labels={"count": "Quantidade", "Órgão Julgador": "Órgão Julgador"}
            )
            st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Gráfico de contagem por Tarefa
        if "Tarefa" in df.columns:
            fig2 = px.pie(
                df["Tarefa"].value_counts().reset_index(),
                values="count",
                names="Tarefa",
                title="Distribuição por Tarefa"
            )
            st.plotly_chart(fig2, use_container_width=True)
    
    # Gráfico de dias por órgão julgador (boxplot)
    if "Dias" in df.columns and "Órgão Julgador" in df.columns:
        fig3 = px.box(
            df,
            x="Órgão Julgador",
            y="Dias",
            title="Distribuição de Dias por Órgão Julgador",
            color="Órgão Julgador"
        )
        st.plotly_chart(fig3, use_container_width=True)

# Processamento principal
if uploaded_files:
    # Processar dados
    df = process_data(uploaded_files, delimiter, encoding)
    
    if df is not None:
        st.header("Dados Carregados")
        st.write(f"Total de registros: {len(df)}")
        
        # Criar filtros dinâmicos
        filters = {}
        for column in filter_options:
            if column in df.columns:
                unique_values = df[column].dropna().unique()
                selected_values = st.multiselect(
                    f"Filtrar por {column}",
                    options=sorted(unique_values),
                    default=[]
                )
                filters[column] = selected_values
        
        # Aplicar filtros
        filtered_df = apply_filters(df, filters)
        
        # Mostrar dados filtrados
        st.header("Dados Filtrados")
        st.write(f"Registros após filtros: {len(filtered_df)}")
        st.dataframe(filtered_df)
        
        # Gerar visualizações
        generate_visualizations(filtered_df)
        
        # Análises adicionais
        st.header("Análises Adicionais")
        
        # Estatísticas de dias
        if "Dias" in filtered_df.columns:
            stats_col1, stats_col2 = st.columns(2)
            with stats_col1:
                st.metric("Média de Dias", f"{filtered_df['Dias'].mean():.1f}")
                st.metric("Mediana de Dias", f"{filtered_df['Dias'].median():.1f}")
            with stats_col2:
                st.metric("Máximo de Dias", f"{filtered_df['Dias'].max():.1f}")
                st.metric("Mínimo de Dias", f"{filtered_df['Dias'].min():.1f}")
        
        # Contagem por classe
        if "Classe" in filtered_df.columns:
            st.subheader("Contagem por Classe")
            st.dataframe(filtered_df["Classe"].value_counts().reset_index().rename(
                columns={"Classe": "Classe", "count": "Quantidade"}
            ))
else:
    st.info("""
    ### Bem-vindo ao Sistema de Tratamento de Dados PJE
    
    Este aplicativo permite processar e analisar dados de processos judiciais do PJE.
    
    Para começar, faça o upload de um ou mais arquivos CSV no painel lateral.
    
    **Funcionalidades disponíveis:**
    - Carregamento de múltiplos arquivos CSV
    - Filtragem de dados por diferentes critérios
    - Visualizações gráficas dos dados
    - Estatísticas e análises
    """)

# Rodapé
st.markdown("---")
st.caption("Desenvolvido para tratamento de dados do PJE - 2025")
