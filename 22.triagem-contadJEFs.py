import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import io
import altair as alt
from fpdf import FPDF
import base64

# --- CONFIGURAÇÕES E CSS ---

# Configuração da página
st.set_page_config(
    page_title="Gestão de Processos Judiciais Unificada",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 1rem 0;
        border-bottom: 2px solid #e0e0e0;
        margin-bottom: 2rem;
    }
    .stat-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #007bff;
        margin-bottom: 1rem;
    }
    .upload-section {
        border: 2px dashed #dee2e6;
        border-radius: 0.5rem;
        padding: 2rem;
        text-align: center;
        margin-bottom: 2rem;
    }
    .assunto-completo {
        white-space: normal !important;
        max-width: 300px;
    }
    .quadro-atribuicao {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 2px solid #dee2e6;
        margin-bottom: 1rem;
    }
    .assunto-destaque {
        background-color: #fff3cd;
        padding: 0.5rem;
        border-radius: 0.25rem;
        border-left: 4px solid #ffc107;
        margin: 0.5rem 0;
        font-weight: 500;
    }
    .info-processo {
        background-color: #e9ecef;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# --- MAPA DE COLUNAS UNIFICADO ---

# Novo Nome (PADRÃO) -> Lista de Nomes Possíveis nos CSVs
COLUNA_MAP = {
    'NUMERO_PROCESSO': ['Número do Processo', 'numeroProcesso'],
    'POLO_ATIVO': ['Polo Ativo', 'poloAtivo'],
    'POLO_PASSIVO': ['Polo Passivo', 'poloPassivo'],
    'ORGAO_JULGADOR': ['Órgão Julgador', 'orgaoJulgador'],
    'ASSUNTO_PRINCIPAL': ['Assunto', 'assuntoPrincipal'],
    'TAREFA': ['Tarefa', 'nomeTarefa'],
    'ETIQUETAS': ['Etiquetas', 'tagsProcessoList'],
    'DIAS': ['Dias'],  # Coluna 'Dias' do Painel Gerencial
    'DATA_CHEGADA_RAW': ['Data Último Movimento', 'dataChegada'] # Coluna bruta de data
}

# --- FUNÇÕES AUXILIARES ---

def get_local_time():
    """Obtém o horário local do Brasil (UTC-3)"""
    utc_now = datetime.now(timezone.utc)
    brasil_tz = timezone(timedelta(hours=-3))
    return utc_now.astimezone(brasil_tz)

def mapear_e_padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia as colunas do DataFrame para um padrão único."""
    colunas_padronizadas = {}
    
    for padrao, possiveis in COLUNA_MAP.items():
        # Encontra o nome da coluna que existe no arquivo atual
        coluna_encontrada = next((col for col in possiveis if col in df.columns), None)
        
        if coluna_encontrada:
            colunas_padronizadas[coluna_encontrada] = padrao
        
    df.rename(columns=colunas_padronizadas, inplace=True)
    return df

def extrair_data_chegada(data_str):
    """Tenta extrair a data de chegada no formato DD/MM/YYYY para objeto datetime (para DATA_CHEGADA_RAW)."""
    if pd.isna(data_str):
        return pd.NaT
    data_str = str(data_str).strip()
    
    # Caso 1: Formato "DD/MM/YYYY, HH:MM:SS" (arquivo Cálculo - Elaborar)
    try:
        # Tenta extrair no formato completo DD/MM/YYYY, HH:MM:SS
        return datetime.strptime(data_str, '%d/%m/%Y, %H:%M:%S') 
    except:
        pass
    
    # Caso 2: Se falhar (ex: string parcial), tenta extrair só a data
    try:
        data_part = data_str.split(',')[0].strip()
        return datetime.strptime(data_part, '%d/%m/%Y')
    except:
        pass
    
    # Caso 3: Formato Timestamp (arquivo Painel Gerencial)
    try:
        if len(data_str) > 10 and data_str.isdigit():
            timestamp_ms = int(data_str)
            # Converter para datetime (dividindo por 1000 se for milissegundos)
            if timestamp_ms > 253402300799:  # Se for muito grande, provavelmente está em milissegundos
                timestamp_ms = timestamp_ms / 1000
            # Retorna o objeto datetime
            return datetime.fromtimestamp(timestamp_ms)
    except:
        pass
        
    return pd.NaT # Retorna pd.NaT (Not a Time) para indicar falha

def processar_dados(df):
    """
    Processa os dados do CSV, priorizando a coluna 'DATA_CHEGADA_RAW' (ou 'dataChegada') 
    para extrair a data real de chegada e o mês/ano exatos.
    """
    
    processed_df = df.copy()
    data_referencia = get_local_time().date()
    
    # Colunas essenciais que DEVEM existir após a padronização
    if 'ETIQUETAS' not in processed_df.columns:
        st.error("Coluna 'ETIQUETAS' (ou 'tagsProcessoList') não encontrada. O arquivo não está no formato esperado.")
        return processed_df
    
    # --- 1. Processar Tags ---
    
    def extrair_servidor(tags):
        if pd.isna(tags):
            return "Sem etiqueta"
        tags_list = str(tags).split(', ')
        for tag in tags_list:
            if 'Servidor' in tag or 'Supervisão' in tag:
                return tag
        return "Sem etiqueta"
    
    def extrair_vara(tags):
        if pd.isna(tags):
            return "Vara não identificada"
        tags_list = str(tags).split(', ')
        for tag in tags_list:
            if 'Vara Federal' in tag:
                return tag
        return "Vara não identificada"
        
    # Aplicar processamento de tags
    processed_df['servidor'] = processed_df['ETIQUETAS'].apply(extrair_servidor)
    processed_df['vara'] = processed_df['ETIQUETAS'].apply(extrair_vara)

    # --- 2. Processar Datas e Calcular Dias (LÓGICA PRIORITÁRIA CORRIGIDA) ---
    
    data_col_existente = False
    
    # Prioridade A: Se a coluna DATA_CHEGADA_RAW existe (Arquivo de Cálculo - Elaborar)
    if 'DATA_CHEGADA_RAW' in processed_df.columns:
        st.info("Utilizando coluna 'DATA_CHEGADA_RAW' para definir a data de chegada e o mês/ano exatos.")
        data_col_existente = True
        
        # Aplica a extração da data no formato correto
        processed_df['data_chegada_obj'] = processed_df['DATA_CHEGADA_RAW'].apply(extrair_data_chegada)
        
    # Prioridade B: Se a coluna DIAS existe (Painel Gerencial)
    elif 'DIAS' in processed_df.columns:
        st.info("Utilizando coluna 'DIAS' do arquivo e calculando a data de chegada por retroação...")
        data_col_existente = True
        
        # Converte 'DIAS' para numérico
        processed_df['DIAS'] = pd.to_numeric(processed_df['DIAS'], errors='coerce').fillna(0).astype(int)
        
        # Calcula a data de chegada retroagindo os dias da data atual
        processed_df['data_chegada_obj'] = processed_df['DIAS'].apply(
            lambda x: datetime.combine(data_referencia - timedelta(days=x), datetime.min.time()) if x is not None and x >= 0 else pd.NaT
        )

    # 3. --- Processamento Comum da Data (Se a coluna 'data_chegada_obj' foi criada) ---
    if data_col_existente and 'data_chegada_obj' in processed_df.columns:
        
        # **CORREÇÃO CRÍTICA**: Converta explicitamente para datetime E remova NaT
        processed_df['data_chegada_obj'] = pd.to_datetime(processed_df['data_chegada_obj'], errors='coerce')
        # Filtra linhas onde a data não é NaT (removendo falhas de conversão)
        processed_df = processed_df[processed_df['data_chegada_obj'].notna()]
        
        if processed_df.empty:
            st.warning("Após o processamento de datas, o DataFrame está vazio. Verifique o formato das colunas de data/dias.")
            return pd.DataFrame()
            
        # Calcular Mês e Ano (AGORA PRECISO, POIS data_chegada_obj VEM DA DATA REAL OU RETROATIVA)
        processed_df['mes'] = processed_df['data_chegada_obj'].dt.month
        processed_df['ano'] = processed_df['data_chegada_obj'].dt.year
        processed_df['mes_ano'] = processed_df['data_chegada_obj'].dt.strftime('%m/%Y')
        
        # Formatar data de chegada
        processed_df['data_chegada_formatada'] = processed_df['data_chegada_obj'].dt.strftime('%d/%m/%Y')

        # Recalcular DIAS (sempre usa a data_chegada_obj, para ser preciso, mesmo no arquivo de cálculo)
        st.info("Recalculando coluna 'DIAS' para garantir a precisão no tempo de permanência.")
        data_chegada_date = processed_df['data_chegada_obj'].dt.date
        processed_df['DIAS'] = (data_referencia - data_chegada_date).apply(lambda x: x.days if x.days >= 0 else 0)
        processed_df['DIAS'] = processed_df['DIAS'].fillna(0).astype(int)
        
        # Ordenar por data de chegada (mais recente primeiro)
        processed_df = processed_df.sort_values('data_chegada_obj', ascending=False)
    
    # Colunas de saída (usando os nomes padronizados)
    cols_to_keep = list(COLUNA_MAP.keys()) + ['servidor', 'vara', 'data_chegada_obj', 'mes', 'ano', 'mes_ano', 'data_chegada_formatada']
    cols_to_keep = [col for col in cols_to_keep if col in processed_df.columns]
    processed_df = processed_df.filter(items=cols_to_keep)

    return processed_df

def criar_estatisticas(df):
    """Cria estatísticas usando APENAS nomes de colunas padronizados."""
    
    stats = {}
    
    # Estatísticas por Polo Passivo
    if 'POLO_PASSIVO' in df.columns:
        polo_passivo_stats = df['POLO_PASSIVO'].value_counts().head(10)
        stats['polo_passivo'] = polo_passivo_stats
    else:
        stats['polo_passivo'] = pd.Series(dtype='int64')

    # Estatísticas por Mês (CORRIGIDO - apenas meses existentes)
    if 'mes' in df.columns:
        # Filtra apenas meses que realmente existem nos dados
        mes_stats = df['mes'].value_counts().sort_index()
        stats['mes'] = mes_stats
    else:
        stats['mes'] = pd.Series(dtype='int64')

    # Estatísticas por Servidor
    if 'servidor' in df.columns:
        servidor_stats = df['servidor'].value_counts()
        stats['servidor'] = servidor_stats
    else:
        stats['servidor'] = pd.Series(dtype='int64')

    # Estatísticas por Vara
    if 'vara' in df.columns:
        vara_stats = df['vara'].value_counts().head(10)
        stats['vara'] = vara_stats
    else:
        stats['vara'] = pd.Series(dtype='int64')

    # Estatísticas por Assunto
    if 'ASSUNTO_PRINCIPAL' in df.columns:
        assunto_stats = df['ASSUNTO_PRINCIPAL'].value_counts().head(10)
        stats['assunto'] = assunto_stats
    else:
        stats['assunto'] = pd.Series(dtype='int64')
    
    return stats

def criar_grafico_barras(dados, titulo, eixo_x, eixo_y):
    df_plot = pd.DataFrame({
        eixo_x: dados.index,
        eixo_y: dados.values
    })
    
    chart = alt.Chart(df_plot).mark_bar().encode(
        x=alt.X(f'{eixo_x}:N', title=eixo_x, axis=alt.Axis(labelAngle=-45), sort='-y'),
        y=alt.Y(f'{eixo_y}:Q', title=eixo_y),
        tooltip=[eixo_x, eixo_y]
    ).properties(
        title=titulo,
        width=600,
        height=400
    )
    
    return chart

def criar_grafico_pizza_com_legenda(dados, titulo):
    df_plot = pd.DataFrame({
        'categoria': dados.index,
        'valor': dados.values,
        'percentual': (dados.values / dados.values.sum() * 100).round(1)
    })
    
    # Criar labels com valores
    df_plot['label'] = df_plot['categoria'] + ' (' + df_plot['valor'].astype(str) + ' - ' + df_plot['percentual'].astype(str) + '%)'
    
    chart = alt.Chart(df_plot).mark_arc().encode(
        theta=alt.Theta(field="valor", type="quantitative"),
        color=alt.Color(field="label", type="nominal", legend=alt.Legend(title="Servidores")),
        tooltip=['categoria', 'valor', 'percentual']
    ).properties(
        title=titulo,
        width=500,
        height=400
    )
    
    return chart

def criar_relatorio_visao_geral(stats, total_processos):
    class PDF(FPDF):
        def header(self):
            self.set_font('Arial', 'B', 16)
            self.cell(0, 10, 'PODER JUDICIÁRIO', 0, 1, 'C')
            self.set_font('Arial', 'B', 14)
            self.cell(0, 10, 'JUSTIÇA FEDERAL EM PERNAMBUCO - JUIZADOS ESPECIAIS FEDERAIS', 0, 1, 'C')
            self.set_font('Arial', 'B', 12)
            self.cell(0, 10, 'PLANILHA DE CONTROLE DE PROCESSOS - PJE2X', 0, 1, 'C')
            self.ln(5)
    
    pdf = PDF()
    pdf.add_page()
    
    # Título do relatório
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, 'RELATÓRIO - VISÃO GERAL', 0, 1, 'C')
    pdf.ln(5)
    
    # Informações gerais
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 8, 'INFORMAÇÕES GERAIS', 0, 1)
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 6, f'Total de Processos: {total_processos}', 0, 1)
    pdf.cell(0, 6, f'Data de geração: {get_local_time().strftime("%d/%m/%Y %H:%M")}', 0, 1)
    pdf.ln(10)
    
    # Estatísticas por Polo Passivo
    if not stats['polo_passivo'].empty:
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, 'DISTRIBUIÇÃO POR POLO PASSIVO (Top 10)', 0, 1)
        pdf.set_font('Arial', '', 10)
        for polo, quantidade in stats['polo_passivo'].items():
            pdf.cell(0, 6, f'{polo}: {quantidade}', 0, 1)
        pdf.ln(5)
    
    # Estatísticas por Mês
    if not stats['mes'].empty:
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, 'DISTRIBUIÇÃO POR Mês', 0, 1)
        pdf.set_font('Arial', '', 10)
        for mes, quantidade in stats['mes'].items():
            pdf.cell(0, 6, f'Mês {mes}: {quantidade}', 0, 1)
        pdf.ln(5)
    
    # Estatísticas por Servidor
    if not stats['servidor'].empty:
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, 'DISTRIBUIÇÃO POR SERVIDOR', 0, 1)
        pdf.set_font('Arial', '', 10)
        for servidor, quantidade in stats['servidor'].items():
            pdf.cell(0, 6, f'{servidor}: {quantidade}', 0, 1)
        pdf.ln(5)
    
    # Estatísticas por Assunto
    if not stats['assunto'].empty:
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, 'PRINCIPAIS ASSUNTOS (Top 10)', 0, 1)
        pdf.set_font('Arial', '', 10)
        for assunto, quantidade in stats['assunto'].items():
            pdf.cell(0, 6, f'{assunto}: {quantidade}', 0, 1)
    
    # Data e hora no final
    pdf.ln(10)
    pdf.set_font('Arial', 'I', 8)
    pdf.cell(0, 6, f'Relatório gerado em: {get_local_time().strftime("%d/%m/%Y às %H:%M:%S")}', 0, 1)
    
    return pdf

def criar_relatorio_estatisticas(stats):
    class PDF(FPDF):
        def header(self):
            self.set_font('Arial', 'B', 16)
            self.cell(0, 10, 'PODER JUDICIÁRIO', 0, 1, 'C')
            self.set_font('Arial', 'B', 14)
            self.cell(0, 10, 'JUSTIÇA FEDERAL EM PERNAMBUCO - JUIZADOS ESPECIAIS FEDERAIS', 0, 1, 'C')
            self.set_font('Arial', 'B', 12)
            self.cell(0, 10, 'PLANILHA DE CONTROLE DE PROCESSOS - PJE2X', 0, 1, 'C')
            self.ln(5)
    
    pdf = PDF()
    pdf.add_page()
    
    # Título do relatório
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, 'RELATÓRIO - ESTATÍSTICAS DETALHADAS', 0, 1, 'C')
    pdf.ln(5)
    
    # Informações gerais
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 6, f'Data de geração: {get_local_time().strftime("%d/%m/%Y %H:%M")}', 0, 1)
    pdf.ln(10)
    
    # Estatísticas por Polo Passivo
    if not stats['polo_passivo'].empty:
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, 'POR POLO PASSIVO (Top 10)', 0, 1)
        pdf.set_font('Arial', '', 10)
        for polo, quantidade in stats['polo_passivo'].items():
            pdf.cell(0, 6, f'{polo}: {quantidade}', 0, 1)
        pdf.ln(5)
    
    # Estatísticas por Mês
    if not stats['mes'].empty:
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, 'POR MÊS', 0, 1)
        pdf.set_font('Arial', '', 10)
        for mes, quantidade in stats['mes'].items():
            pdf.cell(0, 6, f'Mês {mes}: {quantidade}', 0, 1)
        pdf.ln(5)
    
    # Estatísticas por Servidor
    if not stats['servidor'].empty:
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, 'POR SERVIDOR', 0, 1)
        pdf.set_font('Arial', '', 10)
        for servidor, quantidade in stats['servidor'].items():
            pdf.cell(0, 6, f'{servidor}: {quantidade}', 0, 1)
        pdf.ln(5)
    
    # Estatísticas por Vara
    if not stats['vara'].empty:
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, 'POR VARA (Top 10)', 0, 1)
        pdf.set_font('Arial', '', 10)
        for vara, quantidade in stats['vara'].items():
            pdf.cell(0, 6, f'{vara}: {quantidade}', 0, 1)
        pdf.ln(5)
    
    # Estatísticas por Assunto
    if not stats['assunto'].empty:
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, 'POR ASSUNTO (Top 10)', 0, 1)
        pdf.set_font('Arial', '', 10)
        for assunto, quantidade in stats['assunto'].items():
            pdf.cell(0, 6, f'{assunto}: {quantidade}', 0, 1)
    
    # Data e hora no final
    pdf.ln(10)
    pdf.set_font('Arial', 'I', 8)
    pdf.cell(0, 6, f'Relatório gerado em: {get_local_time().strftime("%d/%m/%Y às %H:%M:%S")}', 0, 1)
    
    return pdf

def criar_relatorio_filtros(df_filtrado, filtros_aplicados):
    class PDF(FPDF):
        def header(self):
            self.set_font('Arial', 'B', 16)
            self.cell(0, 10, 'PODER JUDICIÁRIO', 0, 1, 'C')
            self.set_font('Arial', 'B', 14)
            self.cell(0, 10, 'JUSTIÇA FEDERAL EM PERNAMBUCO - JUIZADOS ESPECIAIS FEDERAIS', 0, 1, 'C')
            self.set_font('Arial', 'B', 12)
            self.cell(0, 10, 'PLANILHA DE CONTROLE DE PROCESSOS - PJE2X', 0, 1, 'C')
            self.ln(5)
    
    pdf = PDF()
    pdf.add_page()
    
    # Título do relatório
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, 'RELATÓRIO - FILTROS APLICADOS', 0, 1, 'C')
    pdf.ln(5)
    
    # Informações dos filtros
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 8, 'FILTROS APLICADOS:', 0, 1)
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 6, filtros_aplicados, 0, 1)
    pdf.ln(5)
    
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 6, f'Total de processos filtrados: {len(df_filtrado)}', 0, 1)
    pdf.cell(0, 6, f'Data de geração: {get_local_time().strftime("%d/%m/%Y %H:%M")}', 0, 1)
    pdf.ln(10)
    
    # Tabela de processos - MOSTRAR TODOS OS PROCESSOS
    if len(df_filtrado) > 0:
        pdf.set_font('Arial', 'B', 9)
        # Usar os nomes de coluna padronizados na função chamadora
        colunas_df = ['NUMERO_PROCESSO', 'POLO_ATIVO', 'data_chegada_formatada', 'servidor', 'ASSUNTO_PRINCIPAL']
        colunas_pdf = ['Nº Processo', 'Polo Ativo', 'Data', 'Servidor', 'Assunto']
        larguras = [35, 45, 20, 30, 60]
        
        # Cabeçalho da tabela
        for i, coluna in enumerate(colunas_pdf):
            pdf.cell(larguras[i], 10, coluna, 1, 0, 'C')
        pdf.ln()
        
        # Dados da tabela - TODOS os processos filtrados
        pdf.set_font('Arial', '', 7)
        for _, row in df_filtrado.iterrows():
            n_processo = str(row['NUMERO_PROCESSO']) if pd.notna(row['NUMERO_PROCESSO']) else ''
            polo_ativo = str(row['POLO_ATIVO']) if pd.notna(row['POLO_ATIVO']) else ''
            data_chegada = str(row['data_chegada_formatada']) if pd.notna(row['data_chegada_formatada']) else ''
            servidor = str(row['servidor']) if pd.notna(row['servidor']) else ''
            assunto = str(row['ASSUNTO_PRINCIPAL']) if pd.notna(row['ASSUNTO_PRINCIPAL']) else ''
            
            pdf.cell(larguras[0], 8, n_processo[:20], 1)
            pdf.cell(larguras[1], 8, polo_ativo[:25], 1)
            pdf.cell(larguras[2], 8, data_chegada[:10], 1)
            pdf.cell(larguras[3], 8, servidor[:15], 1)
            pdf.cell(larguras[4], 8, assunto[:40], 1)
            pdf.ln()
    
    # Data e hora no final
    pdf.ln(10)
    pdf.set_font('Arial', 'I', 8)
    pdf.cell(0, 6, f'Relatório gerado em: {get_local_time().strftime("%d/%m/%Y às %H:%M:%S")}', 0, 1)
    
    return pdf

def gerar_link_download_pdf(pdf, nome_arquivo):
    """Gera link de download para o PDF"""
    try:
        # FPDF output stream
        pdf_output = pdf.output(dest='S').encode('latin1')
        b64 = base64.b64encode(pdf_output).decode()
        href = f'<a href="data:application/octet-stream;base64,{b64}" download="{nome_arquivo}">📄 Baixar Relatório PDF</a>'
        return href
    except Exception as e:
        st.error(f"Erro ao gerar PDF: {e}")
        return ""

def gerar_csv_atribuicoes(df_atribuicoes):
    """Gera CSV com as atribuições de servidor no formato final solicitado."""
    if df_atribuicoes.empty:
        return None
    
    # Colunas que DEVEM estar na session_state para gerar o CSV final
    df_csv = df_atribuicoes[['NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor']].copy()
    
    # Renomear para o formato final
    df_csv.columns = ['Número do Processo', 'Vara', 'Órgão Julgador', 'Servidor Atribuído']
    
    # Converter para CSV usando codificação latin-1 para compatibilidade com Excel
    # e separador ;
    csv = df_csv.to_csv(index=False, sep=';', encoding='latin-1')
    return csv

# --- FUNÇÃO PRINCIPAL (MAIN) ---

def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>PODER JUDICIÁRIO</h1>
        <h3>JUSTIÇA FEDERAL EM PERNAMBUCO - JUIZADOS ESPECIAIS FEDERAIS</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Upload de arquivo
    st.markdown("### 📁 Upload do Arquivo CSV do PJE")
    
    uploaded_file = st.file_uploader(
        "Selecione o arquivo CSV exportado do PJE",
        type=['csv'],
        help="Arquivo CSV com até 5.000 linhas, separado por ponto e vírgula"
    )
    
    # Lista de servidores disponíveis para atribuição (para a aba 4)
    servidores_disponiveis = [
        "Servidor 01", "Servidor 02", "Servidor 03", "Servidor 04",
        "Servidor 05", "Servidor 06", "Supervisão"
    ]
    
    if uploaded_file is not None:
        try:
            # Ler arquivo CSV com delimitador ;
            df = pd.read_csv(uploaded_file, delimiter=';', encoding='utf-8')
            
            # 1. Mapear e Padronizar Colunas
            with st.spinner('Padronizando cabeçalhos...'):
                df_padronizado = mapear_e_padronizar_colunas(df)
            
            # 2. Processar dados (calcula dias, extrai servidor, etc.)
            with st.spinner('Processando dados...'):
                processed_df = processar_dados(df_padronizado)

            if processed_df.empty:
                 return # Interrompe se o processamento de datas falhar e retornar DF vazio
            
            st.success(f"✅ Arquivo carregado com sucesso! {len(processed_df)} processos encontrados.")
            stats = criar_estatisticas(processed_df)
            
            # Inicializar session state para atribuições (incluindo ORGAO_JULGADOR)
            if 'atribuicoes_servidores' not in st.session_state:
                st.session_state.atribuicoes_servidores = pd.DataFrame(columns=['NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor', 'data_atribuicao'])
            
            # Abas para organização
            tab1, tab2, tab3, tab4 = st.tabs(["📊 Visão Geral", "📈 Estatísticas", "🔍 Filtros Avançados", "✍️ Atribuição de Servidores"])
            
            # --- TAB 1: VISÃO GERAL ---
            
            with tab1:
                st.markdown("### 📊 Dashboard - Visão Geral")
                
                col1, col2, col3, col4 = st.columns(4)
                with col4:
                    if st.button("📄 Gerar Relatório - Visão Geral", key="relatorio_visao"):
                        with st.spinner("Gerando relatório..."):
                            pdf = criar_relatorio_visao_geral(stats, len(processed_df))
                            nome_arquivo = f"relatorio_visao_geral_{get_local_time().strftime('%Y%m%d_%H%M')}.pdf"
                            href = gerar_link_download_pdf(pdf, nome_arquivo)
                            if href:
                                st.markdown(href, unsafe_allow_html=True)
                
                with col1:
                    st.metric("Total de Processos", len(processed_df))
                
                with col2:
                    servidores_ativos = processed_df[processed_df['servidor'] != 'Sem etiqueta']['servidor'].nunique() if 'servidor' in processed_df.columns else 0
                    st.metric("Servidores Envolvidos", servidores_ativos)
                
                with col3:
                    varas_identificadas = processed_df[processed_df['vara'] != 'Vara não identificada']['vara'].nunique() if 'vara' in processed_df.columns else 0
                    st.metric("Varas Federais", varas_identificadas)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if not stats['polo_passivo'].empty:
                        st.altair_chart(
                            criar_grafico_barras(stats['polo_passivo'], "Distribuição por Polo Passivo", "Polo Passivo", "Quantidade"), 
                            use_container_width=True
                        )
                
                with col2:
                    if not stats['mes'].empty:
                        st.altair_chart(
                            criar_grafico_barras(stats['mes'], "Distribuição por Mês", "Mês", "Quantidade"), 
                            use_container_width=True
                        )
            
            # --- TAB 2: ESTATÍSTICAS ---
            with tab2:
                st.markdown("### 📈 Estatísticas Detalhadas")
                
                col1, col2 = st.columns([3, 1])
                with col2:
                    if st.button("📄 Gerar Relatório - Estatísticas", key="relatorio_estatisticas"):
                        with st.spinner("Gerando relatório..."):
                            pdf = criar_relatorio_estatisticas(stats)
                            nome_arquivo = f"relatorio_estatisticas_{get_local_time().strftime('%Y%m%d_%H%M')}.pdf"
                            href = gerar_link_download_pdf(pdf, nome_arquivo)
                            if href:
                                st.markdown(href, unsafe_allow_html=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### Por Polo Passivo")
                    st.dataframe(stats['polo_passivo'], use_container_width=True)
                    
                    st.markdown("#### Por Servidor")
                    st.dataframe(stats['servidor'], use_container_width=True)
                
                with col2:
                    st.markdown("#### Por Mês")
                    st.dataframe(stats['mes'], use_container_width=True)
                    
                    st.markdown("#### Por Vara")
                    st.dataframe(stats['vara'], use_container_width=True)

            # --- TAB 3: FILTROS AVANÇADOS ---
            with tab3:
                 st.markdown("### 🔍 Filtros Avançados")
                 # ... (O código de filtros avançados permanece inalterado) ...
                 if 'servidor' in processed_df.columns:
                     col1, col2 = st.columns(2)
                     
                     with col1:
                         servidores_filtro = st.multiselect(
                             "Filtrar por Servidor:",
                             options=sorted(processed_df['servidor'].unique()),
                             default=None
                         )
                         
                         assunto_filtro = st.multiselect(
                             "Filtrar por Assunto:",
                             options=sorted(processed_df['ASSUNTO_PRINCIPAL'].dropna().unique()),
                             default=None
                         )
                         
                         if 'mes' in processed_df.columns:
                             meses_filtro = st.multiselect(
                                 "Filtrar por Mês:",
                                 options=sorted(processed_df['mes'].unique()),
                                 default=None
                             )
                         else:
                             meses_filtro = []
                     
                     with col2:
                         varas_filtro = st.multiselect(
                             "Filtrar por Vara:",
                             options=sorted(processed_df['vara'].unique()),
                             default=None
                         )
                         
                         polo_passivo_filtro = st.multiselect(
                             "Filtrar por Polo Passivo:",
                             options=sorted(processed_df['POLO_PASSIVO'].dropna().unique()),
                             default=None
                         )
                     
                     df_filtrado = processed_df.copy()
                     filtros_aplicados = "Filtros aplicados: "
                     
                     if servidores_filtro:
                         df_filtrado = df_filtrado[df_filtrado['servidor'].isin(servidores_filtro)]
                         filtros_aplicados += f"Servidores: {', '.join(servidores_filtro)}; "
                     
                     if assunto_filtro:
                         df_filtrado = df_filtrado[df_filtrado['ASSUNTO_PRINCIPAL'].isin(assunto_filtro)]
                         filtros_aplicados += f"Assuntos: {', '.join(assunto_filtro)}; "
                     
                     if meses_filtro:
                         df_filtrado = df_filtrado[df_filtrado['mes'].isin(meses_filtro)]
                         filtros_aplicados += f"Meses: {', '.join(map(str, meses_filtro))}; "
                     
                     if varas_filtro:
                         df_filtrado = df_filtrado[df_filtrado['vara'].isin(varas_filtro)]
                         filtros_aplicados += f"Varas: {', '.join(varas_filtro)}; "
                     
                     if polo_passivo_filtro:
                         df_filtrado = df_filtrado[df_filtrado['POLO_PASSIVO'].isin(polo_passivo_filtro)]
                         filtros_aplicados += f"Polo Passivo: {', '.join(polo_passivo_filtro)}; "
                     
                     st.markdown(f"**Processos encontrados:** {len(df_filtrado)}")
                     
                     if len(df_filtrado) > 0:
                         df_exibicao = df_filtrado.copy()
                         colunas_exibicao = {
                             'NUMERO_PROCESSO': 'Nº Processo',
                             'POLO_ATIVO': 'Polo Ativo', 
                             'POLO_PASSIVO': 'Polo Passivo',
                             'ASSUNTO_PRINCIPAL': 'Assunto Principal',
                             'servidor': 'Servidor',
                             'vara': 'Vara',
                             'data_chegada_formatada': 'Data Chegada',
                             'DIAS': 'Dias'
                         }
                         colunas_disponiveis = [col for col in colunas_exibicao.keys() if col in df_exibicao.columns]
                         df_exibicao = df_exibicao[colunas_disponiveis]
                         df_exibicao.rename(columns=colunas_exibicao, inplace=True)
                         
                         st.dataframe(df_exibicao, use_container_width=True)
                         
                         if st.button("📄 Gerar Relatório - Filtros Aplicados", key="relatorio_filtros"):
                             with st.spinner("Gerando relatório..."):
                                 pdf = criar_relatorio_filtros(df_exibicao, filtros_aplicados)
                                 nome_arquivo = f"relatorio_filtros_{get_local_time().strftime('%Y%m%d_%H%M')}.pdf"
                                 href = gerar_link_download_pdf(pdf, nome_arquivo)
                                 if href:
                                     st.markdown(href, unsafe_allow_html=True)

            # --- TAB 4: ATRIBUIR SERVIDORES ---
            with tab4:
                st.markdown("### ✍️ Atribuição de Servidores")
                
                # Identificar processos APENAS sem etiqueta de servidor
                processos_sem_etiqueta = processed_df[
                    (processed_df['servidor'] == "Sem etiqueta")
                ].copy()
                
                # Atualizar lista de processos disponíveis (remover os já atribuídos nesta sessão)
                processos_ja_atribuidos = st.session_state.atribuicoes_servidores['NUMERO_PROCESSO'].tolist() if not st.session_state.atribuicoes_servidores.empty else []
                processos_disponiveis = processos_sem_etiqueta[
                    ~processos_sem_etiqueta['NUMERO_PROCESSO'].isin(processos_ja_atribuidos)
                ]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### 📋 Processos para Atribuição")
                    st.markdown(f"**Processos sem servidor atribuído:** {len(processos_disponiveis)}")
                    
                    if len(processos_disponiveis) > 0:
                        # Seleção de processo para edição
                        processo_selecionado = st.selectbox(
                            "Selecione um processo para atribuir servidor:",
                            options=processos_disponiveis['NUMERO_PROCESSO'].
