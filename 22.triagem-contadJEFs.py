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
    'DIAS': ['Dias'],  # Coluna 'Dias' do primeiro arquivo
    'DATA_CHEGADA_RAW': ['Data Último Movimento', 'dataChegada'] # Coluna bruta de data para processamento
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
        return "Sem etiqueta"  # Alterado para considerar apenas processos SEM etiqueta
    
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
                    timestamp_ms = int(data_str)
                    # Converter para datetime (dividindo por 1000 se for milissegundos)
                    if timestamp_ms > 253402300799:  # Se for muito grande, provavelmente está em milissegundos
                        timestamp_ms = timestamp_ms / 1000
                    return datetime.fromtimestamp(timestamp_ms)
            except:
                pass
                
            return None

        # Aplica a extração da data
        processed_df['data_chegada_obj'] = processed_df['DATA_CHEGADA_RAW'].apply(extrair_data_chegada)
        
        # Remove datas inválidas (None)
        processed_df = processed_df[processed_df['data_chegada_obj'].notna()]
        
        # Calcula Mês e Ano
        processed_df['mes'] = processed_df['data_chegada_obj'].dt.month
        processed_df['ano'] = processed_df['data_chegada_obj'].dt.year
        processed_df['mes_ano'] = processed_df['data_chegada_obj'].dt.strftime('%m/%Y')
        
        # Formatar data de chegada (apenas data)
        processed_df['data_chegada_formatada'] = processed_df['data_cheGada_obj'].dt.strftime('%d/%m/%Y')
        
        # Calcular coluna 'DIAS' se não existir
        if 'DIAS' not in processed_df.columns:
            st.info("Calculando coluna 'DIAS' a partir da data de chegada...")
            # Usando a data atual como referência
            data_referencia = get_local_time().date() # Usar apenas a data para cálculo de dias
            
            # Calcular a diferença em dias
            processed_df['DIAS'] = (data_referencia - processed_df['data_chegada_obj'].dt.date).dt.days
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
        colunas_df = ['Nº Processo', 'Polo Ativo', 'Data Chegada', 'Servidor', 'Assunto Principal']
        colunas_pdf = ['Nº Processo', 'Polo Ativo', 'Data', 'Servidor', 'Assunto']
        larguras = [35, 45, 20, 30, 60]
        
        # Cabeçalho da tabela
        for i, coluna in enumerate(colunas_pdf):
            pdf.cell(larguras[i], 10, coluna, 1, 0, 'C')
        pdf.ln()
        
        # Dados da tabela - TODOS os processos filtrados
        pdf.set_font('Arial', '', 7)
        for _, row in df_filtrado.iterrows():
            n_processo = str(row[colunas_df[0]]) if pd.notna(row[colunas_df[0]]) else ''
            polo_ativo = str(row[colunas_df[1]]) if pd.notna(row[colunas_df[1]]) else ''
            data_chegada = str(row[colunas_df[2]]) if pd.notna(row[colunas_df[2]]) else ''
            servidor = str(row[colunas_df[3]]) if pd.notna(row[colunas_df[3]]) else ''
            assunto = str(row[colunas_df[4]]) if pd.notna(row[colunas_df[4]]) else ''
            
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
    """Gera CSV com as atribuições de servidor - AGORA COM 4 COLUNAS (Nº Processo, Vara, Órgão Julgador, Servidor Atribuído)"""
    if df_atribuicoes.empty:
        return None
    
    # Criar DataFrame com 4 colunas específicas (os nomes já vêm padronizados)
    df_csv = df_atribuicoes[['NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor']].copy()
    
    # Renomear para o formato final
    df_csv.columns = ['Número do Processo', 'Vara', 'Órgão Julgador', 'Servidor Atribuído']
    
    # Converter para CSV
    csv = df_csv.to_csv(index=False, sep=';', encoding='utf-8')
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
            
            # Inicializar session state para atribuições
            if 'atribuicoes_servidores' not in st.session_state:
                # O DataFrame deve ter as colunas padronizadas que serão usadas na função gerar_csv_atribuicoes
                st.session_state.atribuicoes_servidores = pd.DataFrame(columns=['NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor'])
            
            # Abas para organização - AGORA COM GUIA SEPARADA PARA ATRIBUIÇÃO
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
                    # Excluir "Sem etiqueta" da contagem de servidores ativos
                    servidores_ativos = processed_df[processed_df['servidor'] != 'Sem etiqueta']['servidor'].nunique() if 'servidor' in processed_df.columns else 0
                    st.metric("Servidores Envolvidos", servidores_ativos)
                
                with col3:
                    varas_unicas = processed_df['vara'].nunique() if 'vara' in processed_df.columns else 0
                    # Excluir "Vara não identificada"
                    varas_identificadas = processed_df[processed_df['vara'] != 'Vara não identificada']['vara'].nunique() if 'vara' in processed_df.columns else 0
                    st.metric("Varas Federais", varas_identificadas)
                
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
                
                # Botão para gerar relatório
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
                
                if 'servidor' in processed_df.columns:
                    # FILTROS COMPLETOS - 5 OPÇÕES
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
                    
                    # Aplicar filtros
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
                    
                    # Mostrar resultados filtrados
                    st.markdown(f"**Processos encontrados:** {len(df_filtrado)}")
                    
                    if len(df_filtrado) > 0:
                        # Preparar dados para exibição
                        df_exibicao = df_filtrado.copy()
                        
                        # Renomear colunas para exibição
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
                        
                        # Selecionar e renomear colunas disponíveis
                        colunas_disponiveis = [col for col in colunas_exibicao.keys() if col in df_exibicao.columns]
                        df_exibicao = df_exibicao[colunas_disponiveis]
                        df_exibicao.rename(columns=colunas_exibicao, inplace=True)
                        
                        # Exibir tabela
                        st.dataframe(df_exibicao, use_container_width=True)
                        
                        # Botão para gerar relatório dos filtros
                        if st.button("📄 Gerar Relatório - Filtros Aplicados", key="relatorio_filtros"):
                            with st.spinner("Gerando relatório..."):
                                pdf = criar_relatorio_filtros(df_exibicao, filtros_aplicados)
                                nome_arquivo = f"relatorio_filtros_{get_local_time().strftime('%Y%m%d_%H%M')}.pdf"
                                href = gerar_link_download_pdf(pdf, nome_arquivo)
                                if href:
                                    st.markdown(href, unsafe_allow_html=True)
            
            # --- TAB 4: ATRIBUIR SERVIDORES (LÓGICA ADICIONADA) ---
            with tab4:
                st.markdown("### ✍️ Atribuir Servidores e Gerar Planilha de Controle")

                # 1. Filtro dos Processos Abertos para Atribuição
                # Filtrar apenas processos sem etiqueta de servidor
                df_para_atribuir = processed_df[processed_df['servidor'] == 'Sem etiqueta'].copy()

                servidores_existentes = sorted([s for s in processed_df['servidor'].unique() if s != 'Sem etiqueta'])

                if 'servidor' in processed_df.columns and not servidores_existentes:
                     st.warning("Não foram encontradas etiquetas de servidores para realizar atribuições.")
                
                if not df_para_atribuir.empty and servidores_existentes:
                    
                    col_sel, col_stat = st.columns([1, 2])
                    
                    with col_sel:
                        # Seletor do Servidor Alvo
                        servidor_alvo = st.selectbox(
                            "Selecione o Servidor para Atribuir:",
                            options=['Selecione...'] + servidores_existentes,
                            key="servidor_alvo_select"
                        )
                        
                        st.markdown(f"**Processos Sem Atribuição:** **{len(df_para_atribuir)}**")
                        
                        # Exibir o dataframe para seleção
                        df_exibicao_atribuir = df_para_atribuir.rename(columns={
                            'NUMERO_PROCESSO': 'Nº Processo', 
                            'ASSUNTO_PRINCIPAL': 'Assunto Principal',
                            'POLO_ATIVO': 'Polo Ativo',
                            'data_chegada_formatada': 'Data Chegada'
                        })
                        
                        # Define as colunas a serem exibidas e o DataEditor
                        cols_editor = ['Nº Processo', 'Polo Ativo', 'Assunto Principal', 'Data Chegada', 'DIAS']
                        
                        # Permite a seleção múltipla de processos
                        processos_selecionados_df = st.data_editor(
                            df_exibicao_atribuir[cols_editor],
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Nº Processo": st.column_config.TextColumn("Nº Processo", width="small"),
                                "Assunto Principal": st.column_config.TextColumn("Assunto Principal", width="medium"),
                            },
                            num_rows="dynamic",
                            key="data_editor_atribuicao"
                        )
                        
                        # Reúne os números de processo selecionados (lista de strings)
                        processos_selecionados_list = processos_selecionados_df['Nº Processo'].tolist()
                        
                        # Lógica de Atribuição (ao pressionar um botão)
                        if st.button(f"Atribuir {len(processos_selecionados_list)} Processo(s) Selecionado(s)", disabled=(servidor_alvo == 'Selecione...') or not processos_selecionados_list):
                            
                            # DataFrame temporário com os dados completos dos processos selecionados
                            novas_atribuicoes = df_para_atribuir[
                                df_para_atribuir['NUMERO_PROCESSO'].isin(processos_selecionados_list)
                            ].copy()
                            
                            # Adicionar coluna 'servidor' com o Servidor Alvo
                            novas_atribuicoes['servidor'] = servidor_alvo 
                            
                            # Selecionar as colunas necessárias para o controle final
                            cols_controle = ['NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor']
                            
                            # Concatenar com as atribuições existentes (keep='last' para sobrescrever se o processo já estava atribuído)
                            st.session_state.atribuicoes_servidores = pd.concat([
                                st.session_state.atribuicoes_servidores, 
                                novas_atribuicoes[cols_controle]
                            ], ignore_index=True).drop_duplicates(subset=['NUMERO_PROCESSO'], keep='last')

                            st.success(f"✅ **{len(processos_selecionados_list)}** processo(s) atribuído(s) a **{servidor_alvo}**! Gere o CSV abaixo.")
                            st.rerun() # Recarregar a página para atualizar o DataEditor e a contagem
                            
                    st.markdown("---")
                    
                    # Se houver atribuições salvas na sessão, mostra a tabela de controle e o botão de download
                    if not st.session_state.atribuicoes_servidores.empty:
                        st.markdown("#### 📥 Planilha de Controle de Atribuições para Download")

                        # Gerar CSV no formato final
                        csv_output = gerar_csv_atribuicoes(st.session_state.atribuicoes_servidores)
                        nome_arquivo = f"atribuicoes_servidores_{get_local_time().strftime('%Y%m%d_%H%M')}.csv"

                        st.download_button(
                            label="⬇️ Baixar CSV de Atribuições",
                            data=csv_output,
                            file_name=nome_arquivo,
                            mime='text/csv',
                        )

                        st.markdown("##### Processos Atribuídos (Nesta Sessão)")
                        
                        # Exibir o DataFrame de atribuições (já renomeado dentro de gerar_csv_atribuicoes)
                        df_controle_exibicao = st.session_state.atribuicoes_servidores.copy()
                        df_controle_exibicao.rename(columns={
                            'NUMERO_PROCESSO': 'Nº Processo',
                            'ORGAO_JULGADOR': 'Órgão Julgador',
                            'servidor': 'Servidor Atribuído'
                        }, inplace=True)

                        st.dataframe(df_controle_exibicao[['Nº Processo', 'vara', 'Órgão Julgador', 'Servidor Atribuído']], use_container_width=True)
                        
                        if st.button("Limpar Atribuições (Resetar Tabela)", type="secondary"):
                            st.session_state.atribuicoes_servidores = pd.DataFrame(columns=['NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor'])
                            st.rerun()
                elif not servidores_existentes:
                     st.info("Nenhuma etiqueta de Servidor ('Servidor XX') foi encontrada na base para realizar a atribuição. Verifique as etiquetas no seu arquivo CSV.")
                else:
                    st.info("Todos os processos da base de dados já possuem uma etiqueta de Servidor ou 'Supervisão'.")
        
        except pd.errors.ParserError:
            st.error("Erro ao ler o arquivo CSV. Certifique-se de que o separador é o **ponto e vírgula (;)** e a codificação é UTF-8.")
        except KeyError as e:
            st.error(f"Coluna essencial não encontrada após a padronização: {e}. Verifique se o seu arquivo possui as colunas de data e etiquetas.")
        except Exception as e:
            st.error(f"Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    main()
