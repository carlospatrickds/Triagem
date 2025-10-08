import streamlit as st
import pandas as pd
import plotly.express as px
import base64
from datetime import datetime

# Configuração da página
st.set_page_config(
    page_title="Tratamento de Dados PJE",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilo CSS personalizado
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #2563EB;
        margin-bottom: 1rem;
    }
    .info-text {
        font-size: 1rem;
        color: #4B5563;
    }
    .highlight {
        background-color: #DBEAFE;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .stButton>button {
        background-color: #2563EB;
        color: white;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Título principal
st.markdown("<h1 class='main-header'>Tratamento de Dados PJE</h1>", unsafe_allow_html=True)

# Função para converter timestamp para data legível
def convert_timestamp(timestamp):
    if pd.isna(timestamp):
        return None
    try:
        # Converter timestamp para milissegundos
        if isinstance(timestamp, str):
            timestamp = float(timestamp)
        return datetime.fromtimestamp(timestamp/1000).strftime('%d/%m/%Y %H:%M')
    except:
        return timestamp

# Função para download de dataframe como CSV
def get_csv_download_link(df, filename="dados_processados.csv"):
    csv = df.to_csv(index=False, sep=';', encoding='utf-8-sig')
    b64 = base64.b64encode(csv.encode('utf-8-sig')).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Baixar arquivo CSV processado</a>'
    return href

# Função para download de dataframe como CSV (simplificada)
def get_excel_download_link(df, filename="dados_processados.csv"):
    csv = df.to_csv(index=False, sep=';', encoding='utf-8-sig')
    b64 = base64.b64encode(csv.encode('utf-8-sig')).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Baixar arquivo Excel processado</a>'
    return href

# Sidebar para upload e opções
with st.sidebar:
    st.markdown("<h2 class='sub-header'>Opções de Processamento</h2>", unsafe_allow_html=True)
    
    # Upload de arquivo
    uploaded_files = st.file_uploader("Carregar arquivos CSV", type=["csv"], accept_multiple_files=True)
    
    # Opções de processamento
    st.markdown("<h3>Configurações</h3>", unsafe_allow_html=True)
    
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
    
    date_format = st.selectbox(
        "Formato de data para conversão",
        options=["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"],
        index=0
    )
    
    # Opções de filtro
    st.markdown("<h3>Filtros</h3>", unsafe_allow_html=True)
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
                error_bad_lines=False
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
    
    st.markdown("<h2 class='sub-header'>Visualizações</h2>", unsafe_allow_html=True)
    
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
    
    # Gráfico de prioridade
    if "Prioridade" in df.columns:
        fig4 = px.pie(
            df["Prioridade"].value_counts().reset_index(),
            values="count",
            names="Prioridade",
            title="Distribuição por Prioridade",
            color_discrete_map={"Sim": "#EF4444", "Não": "#10B981"}
        )
        st.plotly_chart(fig4, use_container_width=True)

# Processamento principal
if uploaded_files:
    # Processar dados
    df = process_data(uploaded_files, delimiter, encoding)
    
    if df is not None:
        st.markdown("<h2 class='sub-header'>Dados Carregados</h2>", unsafe_allow_html=True)
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
        st.markdown("<h2 class='sub-header'>Dados Filtrados</h2>", unsafe_allow_html=True)
        st.write(f"Registros após filtros: {len(filtered_df)}")
        st.dataframe(filtered_df)
        
        # Gerar visualizações
        generate_visualizations(filtered_df)
        
        # Opções de download
        st.markdown("<h2 class='sub-header'>Download dos Dados Processados</h2>", unsafe_allow_html=True)
        st.markdown(get_csv_download_link(filtered_df), unsafe_allow_html=True)
        
        # Análises adicionais
        st.markdown("<h2 class='sub-header'>Análises Adicionais</h2>", unsafe_allow_html=True)
        
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
    st.markdown("""
    <div class="highlight">
        <h2 class='sub-header'>Bem-vindo ao Sistema de Tratamento de Dados PJE</h2>
        <p class='info-text'>Este aplicativo permite processar e analisar dados de processos judiciais do PJE.</p>
        <p class='info-text'>Para começar, faça o upload de um ou mais arquivos CSV no painel lateral.</p>
        <p class='info-text'>Funcionalidades disponíveis:</p>
        <ul>
            <li>Carregamento de múltiplos arquivos CSV</li>
            <li>Filtragem de dados por diferentes critérios</li>
            <li>Visualizações gráficas dos dados</li>
            <li>Estatísticas e análises</li>
            <li>Exportação dos dados processados</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

# Adicionar informações de rodapé
st.markdown("""
<div style="text-align: center; margin-top: 3rem; padding-top: 1rem; border-top: 1px solid #e5e7eb;">
    <p>Desenvolvido para tratamento de dados do PJE - 2025</p>
</div>
""", unsafe_allow_html=True)