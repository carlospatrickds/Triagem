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
    .assunto-destaque {
        background-color: #e8f4fd;
        padding: 0.5rem;
        border-radius: 0.25rem;
        border-left: 3px solid #007bff;
        margin: 0.5rem 0;
    }
    .info-processo {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# --- INICIALIZAÇÃO DA SESSION STATE ---
if 'atribuicoes_servidores' not in st.session_state:
    st.session_state.atribuicoes_servidores = pd.DataFrame(columns=[
        'NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor', 'data_atribuicao', 'POLO_ATIVO', 'ASSUNTO_PRINCIPAL'
    ])

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
    'DIAS': ['Dias'],  # Coluna 'Dias' do primeiro arquivo
    'DATA_CHEGADA_RAW': ['Data Último Movimento', 'dataChegada'] # Coluna bruta de data para processamento
}

# --- LISTA DE SERVIDORES DISPONÍVEIS ---
servidores_disponiveis = [
    "Servidor 1", "Servidor 2", "Servidor 3", "Servidor 4", "Servidor 5",
    "Servidor 6", "Servidor 7", "Servidor 8", "Servidor 9", "Servidor 10",
    "Supervisão"
]

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

def processar_dados(df):
    """Processa os dados do CSV, usando APENAS nomes de colunas padronizados."""
    
    # Criar cópia para não modificar o original
    processed_df = df.copy()
    
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
        return "Não atribuído"
    
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

    # --- 2. Processar Datas e Calcular Dias ---
    
    if 'DATA_CHEGADA_RAW' in processed_df.columns:
        
        def extrair_data_chegada(data_str):
            """Tenta extrair a data de chegada no formato DD/MM/YYYY para objeto datetime."""
            if pd.isna(data_str):
                return None
            data_str = str(data_str)
            
            # Caso 1: Formato "DD/MM/YYYY, HH:MM:SS" (modelotester)
            try:
                data_part = data_str.split(',')[0].strip()
                return datetime.strptime(data_part, '%d/%m/%Y')
            except:
                pass
            
            # Caso 2: Formato Timestamp (Processos_Painel_Gerencial_PJE+R)
            try:
                # O primeiro arquivo usa um timestamp em milissegundos
                if len(data_str) > 10 and data_str.isdigit():
                    return pd.to_datetime(int(data_str), unit='ms').to_pydatetime()
            except:
                pass
                
            return None

        # Aplica a extração da data
        processed_df['data_chegada_obj'] = processed_df['DATA_CHEGADA_RAW'].apply(extrair_data_chegada)
        
        # Calcula Mês e Dia
        processed_df['mes'] = processed_df['data_chegada_obj'].dt.month
        processed_df['dia'] = processed_df['data_chegada_obj'].dt.day
        
        # Formatar data de chegada (apenas data)
        processed_df['data_chegada_formatada'] = processed_df['data_chegada_obj'].dt.strftime('%d/%m/%Y')
        
        # Calcular coluna 'DIAS' se não existir
        if 'DIAS' not in processed_df.columns:
            st.info("Calculando coluna 'DIAS' a partir da data de chegada...")
            # Definindo uma data de referência (ex: data de extração do modelo 1)
            data_referencia = pd.to_datetime('2025-10-07') 
            
            # Calcular a diferença em dias
            processed_df['DIAS'] = (data_referencia - processed_df['data_chegada_obj']).dt.days
            processed_df['DIAS'] = processed_df['DIAS'].fillna(0).astype(int)
        
        # Ordenar por data de chegada (mais recente primeiro)
        processed_df = processed_df.sort_values('data_chegada_obj', ascending=False)
        
    # Colunas de saída (usando os nomes padronizados)
    cols_to_keep = list(COLUNA_MAP.keys()) + ['servidor', 'vara', 'data_chegada_obj', 'mes', 'dia', 'data_chegada_formatada']
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

    # Estatísticas por Mês
    if 'mes' in df.columns:
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

def gerar_csv_atribuicoes(df_atribuicoes):
    """Gera CSV com as atribuições de servidores"""
    if df_atribuicoes.empty:
        return None
    
    # Criar DataFrame para exportação
    df_export = df_atribuicoes[['NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor']].copy()
    df_export.columns = ['Número do Processo', 'Vara', 'Órgão Julgador', 'Servidor Atribuído']
    
    # Gerar CSV
    output = io.StringIO()
    df_export.to_csv(output, index=False, sep=';', encoding='latin-1')
    return output.getvalue()

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
            # Cabeçalho
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
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 8, 'DISTRIBUIÇÃO POR POLO PASSIVO (Top 10)', 0, 1)
    pdf.set_font('Arial', '', 10)
    for polo, quantidade in stats['polo_passivo'].items():
        pdf.cell(0, 6, f'{polo}: {quantidade}', 0, 1)
    pdf.ln(5)
    
    # Estatísticas por Mês
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 8, 'DISTRIBUIÇÃO POR MÊS', 0, 1)
    pdf.set_font('Arial', '', 10)
    for mes, quantidade in stats['mes'].items():
        pdf.cell(0, 6, f'Mês {mes}: {quantidade}', 0, 1)
    pdf.ln(5)
    
    # Estatísticas por Servidor
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 8, 'DISTRIBUIÇÃO POR SERVIDOR', 0, 1)
    pdf.set_font('Arial', '', 10)
    for servidor, quantidade in stats['servidor'].items():
        pdf.cell(0, 6, f'{servidor}: {quantidade}', 0, 1)
    pdf.ln(5)
    
    # Estatísticas por Assunto
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
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 8, 'POR POLO PASSIVO (Top 10)', 0, 1)
    pdf.set_font('Arial', '', 10)
    for polo, quantidade in stats['polo_passivo'].items():
        pdf.cell(0, 6, f'{polo}: {quantidade}', 0, 1)
    pdf.ln(5)
    
    # Estatísticas por Mês
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 8, 'POR MÊS', 0, 1)
    pdf.set_font('Arial', '', 10)
    for mes, quantidade in stats['mes'].items():
        pdf.cell(0, 6, f'Mês {mes}: {quantidade}', 0, 1)
    pdf.ln(5)
    
    # Estatísticas por Servidor
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 8, 'POR SERVIDOR', 0, 1)
    pdf.set_font('Arial', '', 10)
    for servidor, quantidade in stats['servidor'].items():
        pdf.cell(0, 6, f'{servidor}: {quantidade}', 0, 1)
    pdf.ln(5)
    
    # Estatísticas por Vara
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 8, 'POR VARA (Top 10)', 0, 1)
    pdf.set_font('Arial', '', 10)
    for vara, quantidade in stats['vara'].items():
        pdf.cell(0, 6, f'{vara}: {quantidade}', 0, 1)
    pdf.ln(5)
    
    # Estatísticas por Assunto
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
        colunas = ['Nº Processo', 'Polo Ativo', 'Data', 'Servidor', 'Assunto']
        larguras = [35, 45, 20, 30, 60]
        
        # Cabeçalho da tabela
        for i, coluna in enumerate(colunas):
            pdf.cell(larguras[i], 10, coluna, 1, 0, 'C')
        pdf.ln()
        
        # Dados da tabela - TODOS os processos filtrados
        pdf.set_font('Arial', '', 7)
        for _, row in df_filtrado.iterrows():
            n_processo = str(row['Nº Processo']) if pd.notna(row['Nº Processo']) else ''
            polo_ativo = str(row['Polo Ativo']) if pd.notna(row['Polo Ativo']) else ''
            data_chegada = str(row['Data Chegada']) if pd.notna(row['Data Chegada']) else ''
            servidor = str(row['Servidor']) if pd.notna(row['Servidor']) else ''
            assunto = str(row['Assunto Principal']) if pd.notna(row['Assunto Principal']) else ''
            
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
        pdf_output = pdf.output()
        b64 = base64.b64encode(pdf_output).decode()
        href = f'<a href="data:application/octet-stream;base64,{b64}" download="{nome_arquivo}">📄 Baixar Relatório PDF</a>'
        return href
    except Exception as e:
        st.error(f"Erro ao gerar PDF: {e}")
        return ""

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
    
    if uploaded_file is not None:
        try:
            # Ler arquivo CSV
            df = pd.read_csv(uploaded_file, delimiter=';', encoding='utf-8')
            
            # 1. Mapear e Padronizar Colunas
            with st.spinner('Padronizando cabeçalhos...'):
                df_padronizado = mapear_e_padronizar_colunas(df)
            
            # Mostrar informações básicas do arquivo
            st.success(f"✅ Arquivo carregado com sucesso! {len(df_padronizado)} processos encontrados.")
            
            # 2. Processar dados (calcula dias, extrai servidor, etc.)
            with st.spinner('Processando dados...'):
                processed_df = processar_dados(df_padronizado)
                stats = criar_estatisticas(processed_df)
            
            # Abas para organização - AGORA COM 4 ABAS
            tab1, tab2, tab3, tab4 = st.tabs(["📊 Visão Geral", "📈 Estatísticas", "🔍 Filtros Avançados", "✍️ Atribuir Servidores"])
            
            with tab1:
                st.markdown("### 📊 Dashboard - Visão Geral")
                
                # Botão para gerar relatório
                col1, col2, col3, col4 = st.columns(4)
                with col4:
                    if st.button("📄 Gerar Relatório - Visão Geral", key="relatorio_visao"):
                        with st.spinner("Gerando relatório..."):
                            pdf = criar_relatorio_visao_geral(stats, len(processed_df))
                            nome_arquivo = f"relatorio_visao_geral_{get_local_time().strftime('%Y%m%d_%H%M')}.pdf"
                            href = gerar_link_download_pdf(pdf, nome_arquivo)
                            if href:
                                st.markdown(href, unsafe_allow_html=True)
                
                # Métricas principais
                with col1:
                    st.metric("Total de Processos", len(processed_df))
                
                with col2:
                    servidores_unicos = processed_df['servidor'].nunique() if 'servidor' in processed_df.columns else 0
                    st.metric("Servidores Envolvidos", servidores_unicos)
                
                with col3:
                    varas_unicas = processed_df['vara'].nunique() if 'vara' in processed_df.columns else 0
                    st.metric("Varas Federais", varas_unicas)
                
                # Gráficos principais
                col1, col2 = st.columns(2)
                
                with col1:
                    if not stats['polo_passivo'].empty:
                        st.altair_chart(
                            criar_grafico_barras(
                                stats['polo_passivo'], 
                                "Distribuição por Polo Passivo", 
                                "Polo Passivo", 
                                "Quantidade"
                            ), 
                            use_container_width=True
                        )
                    
                    with st.expander("📊 Ver dados - Polo Passivo"):
                        st.dataframe(stats['polo_passivo'])
                
                with col2:
                    if not stats['mes'].empty:
                        st.altair_chart(
                            criar_grafico_barras(
                                stats['mes'], 
                                "Distribuição por Mês", 
                                "Mês", 
                                "Quantidade"
                            ), 
                            use_container_width=True
                        )
                    
                    with st.expander("📊 Ver dados - Distribuição por Mês"):
                        st.dataframe(stats['mes'])
                
                # Gráficos secundários
                col3, col4 = st.columns(2)
                
                with col3:
                    if not stats['servidor'].empty:
                        st.altair_chart(
                            criar_grafico_pizza_com_legenda(
                                stats['servidor'],
                                "Distribuição por Servidor"
                            ),
                            use_container_width=True
                        )
                    
                    with st.expander("📊 Ver dados - Distribuição por Servidor"):
                        st.dataframe(stats['servidor'])
                
                with col4:
                    if not stats['assunto'].empty:
                        df_assunto = pd.DataFrame({
                            'Assunto': stats['assunto'].index,
                            'Quantidade': stats['assunto'].values
                        })
                        
                        chart_assunto = alt.Chart(df_assunto).mark_bar().encode(
                            x='Quantidade:Q',
                            y=alt.Y('Assunto:N', sort='-x', title='Assunto'),
                            tooltip=['Assunto', 'Quantidade']
                        ).properties(
                            title="Principais Assuntos",
                            width=600,
                            height=400
                        )
                        st.altair_chart(chart_assunto, use_container_width=True)
                    
                    with st.expander("📊 Ver dados - Principais Assuntos"):
                        st.dataframe(stats['assunto'])
            
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
            
            with tab3:
                st.markdown("### 🔍 Filtros Avançados")
                
                if 'servidor' not in processed_df.columns:
                    st.error("Não foi possível processar a coluna de Servidor ('Etiquetas'/'tagsProcessoList'). Os filtros podem estar incompletos.")
                    return

                col1, col2, col3 = st.columns(3)
                
                with col1:
                    servidor_filter = st.multiselect(
                        "Filtrar por Servidor",
                        options=sorted(processed_df['servidor'].unique()),
                        default=None
                    )
                    
                    mes_filter = st.multiselect(
                        "Filtrar por Mês",
                        options=sorted(processed_df['mes'].dropna().unique()),
                        default=None
                    )
                
                with col2:
                    polo_passivo_filter = st.multiselect(
                        "Filtrar por Polo Passivo",
                        options=sorted(processed_df['POLO_PASSIVO'].unique()),
                        default=None
                    )
                    
                    assunto_filter = st.multiselect(
                        "Filtrar por Assunto",
                        options=sorted(processed_df['ASSUNTO_PRINCIPAL'].dropna().unique()),
                        default=None
                    )
                
                with col3:
                    vara_filter = st.multiselect(
                        "Filtrar por Vara",
                        options=sorted(processed_df['vara'].unique()),
                        default=None
                    )
                
                # Aplicar filtros
                filtered_df = processed_df.copy()
                filtros_aplicados = []
                
                if servidor_filter:
                    filtered_df = filtered_df[filtered_df['servidor'].isin(servidor_filter)]
                    filtros_aplicados.append(f"Servidor: {', '.join(servidor_filter)}")
                
                if mes_filter:
                    filtered_df = filtered_df[filtered_df['mes'].isin(mes_filter)]
                    filtros_aplicados.append(f"Mês: {', '.join(map(str, mes_filter))}")
                
                if polo_passivo_filter:
                    filtered_df = filtered_df[filtered_df['POLO_PASSIVO'].isin(polo_passivo_filter)]
                    filtros_aplicados.append(f"Polo Passivo: {', '.join(polo_passivo_filter)}")
                
                if assunto_filter:
                    filtered_df = filtered_df[filtered_df['ASSUNTO_PRINCIPAL'].isin(assunto_filter)]
                    filtros_aplicados.append(f"Assunto: {', '.join(assunto_filter)}")
                
                if vara_filter:
                    filtered_df = filtered_df[filtered_df['                    vara_filter = st.multiselect(
                        "Filtrar por Vara",
                        options=sorted(processed_df['vara'].unique()),
                        default=None
                    )
                
                # Aplicar filtros
                filtered_df = processed_df.copy()
                filtros_aplicados = []
                
                if servidor_filter:
                    filtered_df = filtered_df[filtered_df['servidor'].isin(servidor_filter)]
                    filtros_aplicados.append(f"Servidor: {', '.join(servidor_filter)}")
                
                if mes_filter:
                    filtered_df = filtered_df[filtered_df['mes'].isin(mes_filter)]
                    filtros_aplicados.append(f"Mês: {', '.join(map(str, mes_filter))}")
                
                if polo_passivo_filter:
                    filtered_df = filtered_df[filtered_df['POLO_PASSIVO'].isin(polo_passivo_filter)]
                    filtros_aplicados.append(f"Polo Passivo: {', '.join(polo_passivo_filter)}")
                
                if assunto_filter:
                    filtered_df = filtered_df[filtered_df['ASSUNTO_PRINCIPAL'].isin(assunto_filter)]
                    filtros_aplicados.append(f"Assunto: {', '.join(assunto_filter)}")
                
                if vara_filter:
                    filtered_df = filtered_df[filtered_df['vara'].isin(vara_filter)]
                    filtros_aplicados.append(f"Vara: {', '.join(vara_filter)}")
                
                filtros_texto = " | ".join(filtros_aplicados) if filtros_aplicados else "Nenhum filtro aplicado"
                
                st.metric("Processos Filtrados", len(filtered_df))
                
                if len(filtered_df) > 0:
                    # Exibir dados filtrados
                    colunas_filtro = [
                        'NUMERO_PROCESSO', 'POLO_ATIVO', 'POLO_PASSIVO', 'data_chegada_formatada',
                        'mes', 'dia', 'servidor', 'vara', 'ASSUNTO_PRINCIPAL'
                    ]
                    
                    # Filtra apenas colunas que realmente existem após o processamento
                    colunas_existentes = [col for col in colunas_filtro if col in filtered_df.columns]
                    display_filtered = filtered_df[colunas_existentes].copy()
                    
                    # Renomeia para exibição no Streamlit e para o PDF
                    display_filtered.columns = [
                        'Nº Processo', 'Polo Ativo', 'Polo Passivo', 'Data Chegada',
                        'Mês', 'Dia', 'Servidor', 'Vara', 'Assunto Principal'
                    ][:len(display_filtered.columns)]
                    
                    st.dataframe(display_filtered, use_container_width=True)
                    
                    # Botão para gerar relatório PDF
                    st.markdown("---")
                    st.markdown("### 📄 Gerar Relatório com Filtros")
                    
                    if st.button("🖨️ Gerar Relatório PDF com Filtros Atuais", key="relatorio_filtros"):
                        with st.spinner("Gerando relatório..."):
                            try:
                                pdf = criar_relatorio_filtros(display_filtered, filtros_texto)
                                nome_arquivo = f"relatorio_filtros_{get_local_time().strftime('%Y%m%d_%H%M')}.pdf"
                                href = gerar_link_download_pdf(pdf, nome_arquivo)
                                if href:
                                    st.markdown(href, unsafe_allow_html=True)
                                else:
                                    st.error("Erro ao gerar o relatório PDF")
                            except Exception as e:
                                st.error(f"Erro ao gerar PDF: {e}")
                
                else:
                    st.warning("Nenhum processo encontrado com os filtros aplicados.")
            
            # --- TAB 4: ATRIBUIR SERVIDORES ---
            with tab4:
                st.markdown("### ✍️ Atribuição de Servidores")
                
                # Identificar processos APENAS sem etiqueta de servidor
                processos_sem_etiqueta = processed_df[
                    (processed_df['servidor'] == "Sem etiqueta") | 
                    (processed_df['servidor'] == "Não atribuído")
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
                            options=processos_disponiveis['NUMERO_PROCESSO'].tolist(),
                            key="processo_edicao"
                        )
                        
                        if processo_selecionado:
                            # Informações do processo selecionado
                            processo_info = processos_disponiveis[
                                processos_disponiveis['NUMERO_PROCESSO'] == processo_selecionado
                            ].iloc[0]
                            
                            st.markdown("**Informações do Processo:**")
                            st.markdown(f'<div class="info-processo">', unsafe_allow_html=True)
                            st.markdown(f"**Número:** {processo_info['NUMERO_PROCESSO']}")
                            st.markdown(f"**Polo Ativo:** {processo_info.get('POLO_ATIVO', 'N/A')}")
                            st.markdown(f"**Polo Passivo:** {processo_info.get('POLO_PASSIVO', 'N/A')}")
                            
                            # ASSUNTO EM DESTAQUE
                            assunto = processo_info.get('ASSUNTO_PRINCIPAL', 'N/A')
                            st.markdown(f'<div class="assunto-destaque"><strong>Assunto:</strong> {assunto}</div>', unsafe_allow_html=True)
                            
                            # Determinar Vara Final (usar Órgão Julgador se 'vara' for "Vara não identificada")
                            vara_atual = processo_info.get('vara', 'Vara não identificada')
                            orgao_julgador = processo_info.get('ORGAO_JULGADOR', 'N/A')
                            
                            if vara_atual == "Vara não identificada":
                                vara_final = orgao_julgador
                            else:
                                vara_final = vara_atual
                            
                            st.markdown(f"**Vara:** {vara_final}")
                            st.markdown(f"**Órgão Julgador:** {orgao_julgador}")
                            st.markdown(f"**Data de Chegada:** {processo_info.get('data_chegada_formatada', 'N/A')}")
                            st.markdown('</div>', unsafe_allow_html=True)
                            
                            # Seleção de servidor
                            novo_servidor = st.selectbox(
                                "Atribuir servidor:",
                                options=servidores_disponiveis,
                                key="novo_servidor"
                            )
                            
                            # Botão para aplicar a alteração
                            if st.button("💾 Aplicar Atribuição", key="aplicar_edicao"):
                                
                                # Criar registro da atribuição
                                atribuicao = {
                                    'NUMERO_PROCESSO': processo_info['NUMERO_PROCESSO'],
                                    'vara': vara_final,
                                    'ORGAO_JULGADOR': orgao_julgador,
                                    'servidor': novo_servidor,
                                    'data_atribuicao': get_local_time().strftime('%d/%m/%Y %H:%M'),
                                    'POLO_ATIVO': processo_info.get('POLO_ATIVO', ''),
                                    'ASSUNTO_PRINCIPAL': processo_info.get('ASSUNTO_PRINCIPAL', '')
                                }
                                
                                # Adicionar à session state
                                nova_atribuicao_df = pd.DataFrame([atribuicao])
                                st.session_state.atribuicoes_servidores = pd.concat(
                                    [st.session_state.atribuicoes_servidores, nova_atribuicao_df], 
                                    ignore_index=True
                                ).drop_duplicates(subset=['NUMERO_PROCESSO'], keep='last')
                                
                                st.success(f"✅ Servidor **'{novo_servidor}'** atribuído ao processo **{processo_selecionado}**!")
                                st.rerun()
                                
                    else:
                        st.success("🎉 Todos os processos já possuem servidor atribuído (ou foram atribuídos nesta sessão)!")
                
                with col2:
                    st.markdown("#### ✅ Processos Atribuídos")
                    
                    if not st.session_state.atribuicoes_servidores.empty:
                        st.markdown(f"**Total de processos atribuídos:** {len(st.session_state.atribuicoes_servidores)}")
                        
                        # Exibir processos atribuídos
                        df_exibicao_atribuidos = st.session_state.atribuicoes_servidores[[
                            'NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor', 'data_atribuicao'
                        ]].copy()
                        
                        df_exibicao_atribuidos.columns = ['Nº Processo', 'Vara', 'Órgão Julgador', 'Servidor', 'Data/Hora Atribuição']
                        st.dataframe(df_exibicao_atribuidos, use_container_width=True)
                        
                        # Botão para download do CSV
                        st.markdown("---")
                        st.markdown("#### 📥 Download das Atribuições")
                        
                        csv_atribuicoes = gerar_csv_atribuicoes(st.session_state.atribuicoes_servidores)
                        if csv_atribuicoes:
                            # Base64 encoding para o download com latin-1
                            csv_b64 = base64.b64encode(csv_atribuicoes.encode('latin-1')).decode()
                            href = f'<a href="data:text/csv;base64,{csv_b64}" download="atribuicoes_servidores_{get_local_time().strftime("%Y%m%d_%H%M")}.csv">📊 Baixar CSV com Atribuições</a>'
                            st.markdown(href, unsafe_allow_html=True)
                            st.info("O arquivo CSV contém as colunas: Número do Processo, Vara, Órgão Julgador e Servidor Atribuído")
                        
                        if st.button("Limpar Atribuições (Resetar Tabela)", type="secondary"):
                            st.session_state.atribuicoes_servidores = pd.DataFrame(columns=[
                                'NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor', 'data_atribuicao', 'POLO_ATIVO', 'ASSUNTO_PRINCIPAL'
                            ])
                            st.rerun()
                            
                    else:
                        st.info("Nenhum processo atribuído ainda. Use o quadro à esquerda para fazer as primeiras atribuições.")
        
        except pd.errors.ParserError:
            st.error("Erro ao ler o arquivo CSV. Certifique-se de que o separador é o **ponto e vírgula (;)** e a codificação é UTF-8.")
        except KeyError as e:
            st.error(f"Coluna essencial não encontrada após a padronização: {e}. Verifique se o seu arquivo possui as colunas de data e etiquetas.")
        except Exception as e:
            st.error(f"Ocorreu um erro inesperado: {e}")
    
    else:
        # Tela inicial quando não há arquivo
        st.markdown("""
        <div class="upload-section">
            <h3>👋 Bem-vindo ao Sistema de Gestão de Processos Judiciais</h3>
            <p>Faça o upload do arquivo CSV exportado do PJE para começar a análise. Funciona com formatos de painel variados!</p>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
