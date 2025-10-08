import streamlit as st
import pandas as pd

st.title("Processador de CSV - PJE")

uploaded_file = st.file_uploader("Carregar arquivo CSV", type=["csv"])
delimiter = st.selectbox("Delimitador", [";", ",", "\\t", "|"])
encoding = st.selectbox("Encoding", ["utf-8", "latin1", "iso-8859-1"])

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file, delimiter=delimiter, encoding=encoding)
        st.write(f"Registros: {len(df)}")
        st.dataframe(df)
    except Exception as e:
        st.error(f"Erro: {e}")
else:
    st.info("Faça upload de um arquivo CSV para começar.")