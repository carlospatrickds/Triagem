import streamlit as st
import pandas as pd

# Configuração básica da página
st.set_page_config(page_title="Tratamento de Dados PJE - Mínimo", page_icon="📊")

# Título
st.title("Tratamento de Dados PJE")
st.write("Aplicação para processamento e análise de dados de processos judiciais.")

# Sidebar para upload
uploaded_file = st.sidebar.file_uploader("Carregar arquivo CSV", type=["csv"])

# Opções de processamento
delimiter = st.sidebar.selectbox("Delimitador do CSV", options=[";", ",", "\\t", "|"], index=0)
encoding = st.sidebar.selectbox("Encoding", options=["utf-8", "latin1", "iso-8859-1", "cp1252"], index=0)

# Processamento principal
if uploaded_file is not None:
    try:
        # Ler o arquivo CSV
        df = pd.read_csv(
            uploaded_file,
            delimiter=delimiter,
            encoding=encoding,
            on_bad_lines='skip'
        )
        
        # Mostrar informações básicas
        st.write(f"Total de registros: {len(df)}")
        
        # Mostrar os dados
        st.subheader("Dados Carregados")
        st.dataframe(df)
        
        # Estatísticas básicas
        st.subheader("Estatísticas Básicas")
        st.write(df.describe())
        
    except Exception as e:
        st.error(f"Erro ao processar o arquivo: {e}")
else:
    st.info("Por favor, faça o upload de um arquivo CSV para começar.")

# Rodapé
st.markdown("---")
st.caption("Desenvolvido para tratamento de dados do PJE - 2025")