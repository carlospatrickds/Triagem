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

# CSS customizado (Adicionado estilo para a aba de Atribuição)
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
    /* NOVO: Estilo para destaque de assunto na aba de atribuição */
    .assunto-destaque {
        background-color: #ffeeb9; /* Amarelo claro */
        border-left: 5px solid #ffcc00;
        padding: 10px;
        margin: 10px 0;
        border-radius: 4px;
    }
</style>
""", unsafe_allow_html=True)

# --- LISTA FIXA DE SERVIDORES ---
SERVIDORES_DISPONIVEIS = [
    "Servidor 01",
    "Servidor 02",
    "Servidor 03",
    "Servidor 04",
    "Servidor 05",
    "Servidor 06",
    "Servidor 07 - ES",
    "Servidor 09 - ES",
    "Supervisão 08"
]

# --- MAPA DE COLUNAS UNIFICADO (ATUALIZADO) ---

# Novo Nome (PADRÃO) -> Lista de Nomes Possíveis nos CSVs
COLUNA_MAP = {
    'NUMERO_PROCESSO': ['Número do Processo', 'numeroProcesso', 'Nº Processo'], 
    'POLO_ATIVO': ['Polo Ativo', 'poloAtivo'],
    'POLO_PASSIVO': ['Polo Passivo', 'poloPassivo'],
    'ORGAO_JULGADOR': ['Órgão Julgador', 'orgaoJulgador', 'Vara'], 
    'ASSUNTO_PRINCIPAL': ['Assunto', 'assuntoPrincipal', 'Assunto Principal'], 
    'TAREFA': ['Tarefa', 'nomeTarefa'],
    'ETIQUETAS': ['Etiquetas', 'tagsProcessoList'],
    # A coluna 'DIAS' agora é crucial para o cálculo do Painel Gerencial
    'DIAS_TRANSCORRIDOS': ['Dias'],  
    'DATA_ULTIMO_MOVIMENTO_RAW': ['Data Último Movimento'], # Coluna bruta de data (Painel Gerencial)
    'DATA_CHEGADA_RAW': ['dataChegada'], # Coluna bruta de data (Tarefa Simples)
    'DATA_CHEGADA_FORMATADA_INPUT': ['Data Chegada'] # Coluna formatada (Arquivos já processados/exportados)
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
        # Se não encontrar a coluna de Etiquetas, adiciona uma vazia para evitar erro
        processed_df['ETIQUETAS'] = "Sem etiqueta"
    
    # --- 1. Processar Tags ---
    
    def extrair_servidor(tags):
        if pd.isna(tags):
            return "Sem etiqueta"
        tags_list = str(tags).split(', ')
        for tag in tags_list:
            if tag in SERVIDORES_DISPONIVEIS:
                return tag
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
    if 'ORGAO_JULGADOR' in processed_df.columns:
         processed_df['vara'] = processed_df['ETIQUETAS'].apply(extrair_vara)
         # Se a vara não foi identificada pela etiqueta, tenta usar o Orgão Julgador
         processed_df.loc[processed_df['vara'] == "Vara não identificada", 'vara'] = processed_df['ORGAO_JULGADOR']
    else:
        processed_df['vara'] = processed_df['ETIQUETAS'].apply(extrair_vara)
    
    # --- 2. Processar Datas e Calcular Dias (LÓGICA DE RETROAÇÃO) ---
    
    processed_df['data_chegada_obj'] = pd.NaT
    
    # --- Lógica de Prioridade de Data ---
    
    # A. Prioridade 1: Data Chegada de arquivos já processados/exportados (DD/MM/YYYY)
    if 'DATA_CHEGADA_FORMATADA_INPUT' in processed_df.columns:
        processed_df['data_chegada_obj'] = pd.to_datetime(
            processed_df['DATA_CHEGADA_FORMATADA_INPUT'], 
            format='%d/%m/%Y', 
            errors='coerce'
        )
        # st.info("Prioridade 1: Usando coluna 'Data Chegada' de arquivo processado.")

    # B. Prioridade 2: Cálculo Retroativo (Data Último Movimento - Dias Transcorridos) - Painel Gerencial
    if processed_df['data_chegada_obj'].isna().all() and 'DATA_ULTIMO_MOVIMENTO_RAW' in processed_df.columns and 'DIAS_TRANSCORRIDOS' in processed_df.columns:
        
        def extrair_e_calcular_data(row):
            data_mov_raw = row['DATA_ULTIMO_MOVIMENTO_RAW']
            dias_transcorridos = row['DIAS_TRANSCORRIDOS']
            
            if pd.isna(data_mov_raw) or pd.isna(dias_transcorridos):
                return pd.NaT

            data_mov_raw = str(data_mov_raw)
            try:
                dias_transcorridos = int(dias_transcorridos) # Deve ser um inteiro
            except ValueError:
                return pd.NaT
            
            try:
                # 1. Tentar formato Timestamp (o mais comum no PJE+R)
                if len(data_mov_raw) > 10 and data_mov_raw.isdigit():
                    data_mov_obj = pd.to_datetime(int(data_mov_raw), unit='ms').normalize()
                    # Subtrai os dias para obter a data de chegada real
                    return data_mov_obj - timedelta(days=dias_transcorridos)
                
            except:
                pass
                
            return pd.NaT # Se falhar

        # Aplicar a função de cálculo retroativo APENAS onde a data ainda é NaT
        processed_df.loc[processed_df['data_chegada_obj'].isna(), 'data_chegada_obj'] = processed_df.apply(
            extrair_e_calcular_data, axis=1
        )
        # st.info("Prioridade 2: Calculando 'Data Chegada' retroativamente (Último Movimento - Dias).")
        
        # MANTEMOS A COLUNA DIAS ORIGINAL DO PAINEL GERENCIAL
        processed_df['DIAS'] = processed_df['DIAS_TRANSCORRIDOS'].fillna(0).astype(int)

    # C. Prioridade 3: Data Chegada de arquivo de tarefa simples (DD/MM/YYYY, HH:MM:SS)
    if processed_df['data_chegada_obj'].isna().all() and 'DATA_CHEGADA_RAW' in processed_df.columns:
        
        def extrair_data_chegada_raw(data_str):
            if pd.isna(data_str):
                return pd.NaT
            data_str = str(data_str)
            
            # Formato "DD/MM/YYYY, HH:MM:SS" (Geralmente Tarefa Simples)
            try:
                data_part = data_str.split(',')[0].strip()
                return datetime.strptime(data_part, '%d/%m/%Y').date()
            except:
                pass
            
            return pd.NaT
        
        # Aplicar a extração da data
        processed_df.loc[processed_df['data_chegada_obj'].isna(), 'data_chegada_obj'] = processed_df['DATA_CHEGADA_RAW'].apply(extrair_data_chegada_raw)
        # st.info("Prioridade 3: Usando coluna 'dataChegada' de arquivo de tarefa simples.")
    
    # --- Continuação do Processamento de Data ---
    
    # Filtra linhas onde a data não pôde ser extraída para evitar erros
    processed_df.dropna(subset=['data_chegada_obj'], inplace=True)

    if not processed_df.empty:
        # Calcula Mês e Dia a partir da DATA DE CHEGADA CALCULADA
        processed_df['mes'] = processed_df['data_chegada_obj'].dt.month
        processed_df['dia'] = processed_df['data_chegada_obj'].dt.day
        
        # Formatar data de chegada (apenas data)
        processed_df['data_chegada_formatada_final'] = processed_df['data_chegada_obj'].dt.strftime('%d/%m/%Y')
        
        # Se a coluna DIAS não veio do Painel Gerencial (Prioridade 2), calcula o DIAS.
        if 'DIAS' not in processed_df.columns:
            # Definindo uma data de referência (data de hoje)
            data_referencia = pd.to_datetime(get_local_time().date())
            
            # Calcular a diferença em dias
            processed_df['DIAS'] = (data_referencia - processed_df['data_chegada_obj']).dt.days
            processed_df['DIAS'] = processed_df['DIAS'].fillna(0).astype(int)
        
        # Ordenar por data de chegada (mais recente primeiro)
        processed_df = processed_df.sort_values('data_chegada_obj', ascending=False)
    
    
    # Colunas de saída (usando os nomes padronizados)
    # Remove as colunas RAW e FORMATADA_INPUT
    cols_to_remove = ['DATA_ULTIMO_MOVIMENTO_RAW', 'DATA_CHEGADA_RAW', 'DATA_CHEGADA_FORMATADA_INPUT', 'DIAS_TRANSCORRIDOS']
    cols_to_keep = [col for col in list(COLUNA_MAP.keys()) + ['servidor', 'vara', 'data_chegada_obj', 'mes', 'dia', 'data_chegada_formatada_final', 'DIAS'] if col not in cols_to_remove]
    
    # Garante que não haja duplicatas de colunas
    cols_to_keep = list(dict.fromkeys(cols_to_keep))
    
    # Filtra e renomeia
    processed_df = processed_df.filter(items=cols_to_keep)
    
    if 'data_chegada_formatada_final' in processed_df.columns:
        processed_df.rename(columns={'data_chegada_formatada_final': 'data_chegada_formatada'}, inplace=True)
    
    return processed_df

# --- Funções de Estatísticas, Relatórios e Download (Inalteradas) ---

def criar_estatisticas(df):
    """Cria estatísticas usando APENAS nomes de colunas padronizados."""
    
    stats = {}
    
    # Estatísticas por Polo Passivo
    stats['polo_passivo'] = df['POLO_PASSIVO'].value_counts().head(10) if 'POLO_PASSIVO' in df.columns else pd.Series(dtype='int64')

    # Estatísticas por Mês
    stats['mes'] = df['mes'].value_counts().sort_index() if 'mes' in df.columns else pd.Series(dtype='int64')

    # Estatísticas por Servidor
    if 'servidor' in df.columns:
        servidor_stats = df[~df['servidor'].isin(['Sem etiqueta', 'Não atribuído'])]['servidor'].value_counts()
        nao_atribuidos_count = df[df['servidor'].isin(['Sem etiqueta', 'Não atribuído'])].shape[0]
        if nao_atribuidos_count > 0:
            servidor_stats['Sem ou Não Atribuído'] = nao_atribuidos_count
            
        stats['servidor'] = servidor_stats
    else:
        stats['servidor'] = pd.Series(dtype='int64')

    # Estatísticas por Vara
    stats['vara'] = df['vara'].value_counts().head(10) if 'vara' in df.columns else pd.Series(dtype='int64')

    # Estatísticas por Assunto
    stats['assunto'] = df['ASSUNTO_PRINCIPAL'].value_counts().head(10) if 'ASSUNTO_PRINCIPAL' in df.columns else pd.Series(dtype='int64')
    
    return stats

def criar_grafico_barras(dados, titulo, eixo_x, eixo_y):
    df_plot = pd.DataFrame({
        eixo_x: dados.index,
        eixo_y: dados.values
    })
    
    # Mapeamento do mês para nome, se for um gráfico de mês
    if eixo_x.lower() == 'mês':
        mes_map = {
            1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun', 
            7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
        }
        df_plot['Mês Nome'] = df_plot[eixo_x].map(mes_map).fillna(df_plot[eixo_x].astype(str))
        eixo_x_display = 'Mês Nome'
    else:
        eixo_x_display = eixo_x
    
    chart = alt.Chart(df_plot).mark_bar().encode(
        x=alt.X(f'{eixo_x_display}:N', title=eixo_x, axis=alt.Axis(labelAngle=-45), sort='-y'),
        y=alt.Y(f'{eixo_y}:Q', title=eixo_y),
        tooltip=[eixo_x_display, eixo_y]
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

# Funções de Relatórios PDF (Inalteradas)
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
    mes_map = {1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'}
    for mes, quantidade in stats['mes'].items():
        pdf.cell(0, 6, f'{mes_map.get(mes, f"Mês {mes}")}: {quantidade}', 0, 1)
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
    mes_map = {1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'}
    for mes, quantidade in stats['mes'].items():
        pdf.cell(0, 6, f'{mes_map.get(mes, f"Mês {mes}")}: {quantidade}', 0, 1)
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
            # **ATENÇÃO:** As colunas aqui (Nº Processo, Polo Ativo, etc) precisam
            # corresponder aos nomes finais usados na main() antes de chamar esta função!
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
        # Pega a string de bytes do PDF
        pdf_output = pdf.output(dest='S').encode('latin-1')
        b64 = base64.b64encode(pdf_output).decode('latin-1')
        href = f'<a href="data:application/octet-stream;base64,{b64}" download="{nome_arquivo}">📄 Baixar Relatório PDF</a>'
        return href
    except Exception as e:
        st.error(f"Erro ao gerar PDF: {e}")
        return ""

def gerar_csv_atribuicoes(df):
    """Gera o conteúdo CSV das atribuições manuais."""
    if df.empty:
        return ""
        
    df_temp = df.copy()
    
    # Colunas a serem exportadas e seus novos nomes
    df_temp = df_temp[[
        'NUMERO_PROCESSO', 
        'vara', 
        'ORGAO_JULGADOR', 
        'servidor', 
        'data_atribuicao',
        'POLO_ATIVO',
        'ASSUNTO_PRINCIPAL'
    ]]
    df_temp.columns = [
        'Numero do Processo', 
        'Vara (Tag)', 
        'Orgao Julgador (Original)', 
        'Servidor Atribuido', 
        'Data e Hora da Atribuicao',
        'Polo Ativo',
        'Assunto Principal'
    ]
    
    # Exportar para CSV com ponto e vírgula como delimitador
    csv_output = df_temp.to_csv(index=False, sep=';', encoding='latin-1')
    return csv_output

# --- FUNÇÃO PRINCIPAL (MAIN) ---

def main():
    # Inicialização da Session State
    if 'atribuicoes_servidores' not in st.session_state:
        st.session_state.atribuicoes_servidores = pd.DataFrame(columns=[
            'NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor', 'data_atribuicao', 'POLO_ATIVO', 'ASSUNTO_PRINCIPAL'
        ])
        
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>PODER JUDICIÁRIO</h1>
        <h3>JUSTIÇA FEDERAL EM PERNAMBUCO - JUIZADOS ESPECIAIS FEDERAIS</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Upload de arquivo (AGORA MÚLTIPLOS)
    st.markdown("### 📁 Upload dos Arquivos CSV do PJE")
    
    uploaded_files = st.file_uploader(
        "Selecione um ou mais arquivos CSV exportados do PJE (separador: ponto e vírgula)",
        type=['csv'],
        accept_multiple_files=True
    )
    
    if uploaded_files:
        
        # --- Lógica de Leitura e Unificação de Múltiplos Arquivos ---
        all_dfs = []
        
        for uploaded_file in uploaded_files:
            try:
                # Tenta ler com a codificação padrão e delimitador
                df = pd.read_csv(uploaded_file, delimiter=';', encoding='utf-8')
            except UnicodeDecodeError:
                # Tenta ler com Latin-1 se o UTF-8 falhar
                try:
                    df = pd.read_csv(uploaded_file, delimiter=';', encoding='latin-1')
                except Exception as e:
                    st.warning(f"⚠️ Não foi possível ler o arquivo **{uploaded_file.name}**. Pulando. (Erro: {e})")
                    continue
            except pd.errors.ParserError:
                 st.warning(f"⚠️ Erro de leitura no arquivo **{uploaded_file.name}**. Verifique se o separador é o ponto e vírgula (;). Pulando.")
                 continue
            except Exception as e:
                st.warning(f"⚠️ Erro inesperado ao ler **{uploaded_file.name}**. Pulando. (Erro: {e})")
                continue
                
            # Mapear e Padronizar Colunas para garantir que o 'NUMERO_PROCESSO' seja consistente
            df_padronizado = mapear_e_padronizar_colunas(df.copy())
            
            # Garante que a coluna de chave existe antes de adicionar
            if 'NUMERO_PROCESSO' in df_padronizado.columns:
                all_dfs.append(df_padronizado)
            else:
                st.error(f"❌ O arquivo **{uploaded_file.name}** não possui a coluna de Número do Processo. Não será incluído na análise.")

        # --- FIM da Lógica de Leitura ---
        
        if not all_dfs:
            st.error("Nenhum arquivo válido pôde ser lido para a análise.")
            return

        with st.spinner(f'Unificando dados de {len(all_dfs)} arquivo(s) e removendo duplicatas...'):
            # 1. Unir todos os DataFrames
            df_unificado = pd.concat(all_dfs, ignore_index=True)
            
            # 2. Remover duplicatas, mantendo a primeira ocorrência 
            df_final = df_unificado.drop_duplicates(subset=['NUMERO_PROCESSO'], keep='first')
        
        st.success(f"✅ Análise unificada de **{len(uploaded_files)}** arquivo(s). **{len(df_final)}** processos únicos encontrados.")
        
        # 3. Processar dados (recalcula mês/dia, extrai servidor, etc.)
        with st.spinner('Processando dados...'):
            processed_df = processar_dados(df_final)
            
        # 4. Aplicar Atribuições Manuais 
        if not st.session_state.atribuicoes_servidores.empty:
            df_atribuicoes = st.session_state.atribuicoes_servidores[['NUMERO_PROCESSO', 'servidor']].copy()

            for index, row in df_atribuicoes.iterrows():
                match_index = processed_df.index[processed_df['NUMERO_PROCESSO'] == row['NUMERO_PROCESSO']]
                if not match_index.empty:
                    processed_df.loc[match_index, 'servidor'] = row['servidor']
                    
        # Recalcula estatísticas após as atribuições serem aplicadas
        stats = criar_estatisticas(processed_df)
                    
        # Abas para organização
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Visão Geral", "📈 Estatísticas", "🔍 Filtros Avançados", "✍️ Atribuição Manual"])
        
        # --- Conteúdo das Abas ---
        
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
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total de Processos Únicos", len(processed_df))
            
            with col2:
                # Filtra apenas servidores reais (excluindo 'Sem etiqueta' e 'Não atribuído')
                servidores_reais = processed_df[~processed_df['servidor'].isin(['Sem etiqueta', 'Não atribuído'])]['servidor'].nunique()
                st.metric("Servidores Atribuídos", servidores_reais)
            
            with col3:
                varas_unicas = processed_df['vara'].nunique() if 'vara' in processed_df.columns else 0
                st.metric("Varas Federais", varas_unicas)
            
            with col4:
                 st.metric("Processos Sem Atribuição", len(processed_df[processed_df['servidor'].isin(['Sem etiqueta', 'Não atribuído'])]))

            # Gráficos principais
            col1, col2 = st.columns(2)
            
            with col1:
                if not stats['polo_passivo'].empty:
                    st.altair_chart(
                        criar_grafico_barras(
                            stats['polo_passivo'], 
                            "Distribuição por Polo Passivo (Top 10)", 
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
                            "Distribuição por Mês (Data de Chegada)", 
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
                        title="Principais Assuntos (Top 10)",
                        width=600,
                        height=400
                    )
                    st.altair_chart(chart_assunto, use_container_width=True)
                
                with st.expander("📊 Ver dados - Principais Assuntos"):
                    st.dataframe(stats['assunto'])

        # --- Tab 2: Estatísticas ---
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
                st.markdown("#### Por Mês (Data de Chegada)")
                st.dataframe(stats['mes'], use_container_width=True)
                
                st.markdown("#### Por Vara")
                st.dataframe(stats['vara'], use_container_width=True)

        # --- Tab 3: Filtros Avançados ---
        with tab3:
            st.markdown("### 🔍 Filtros Avançados")
            
            if processed_df.empty or 'servidor' not in processed_df.columns:
                st.warning("Não há dados válidos ou a coluna de Servidor não foi encontrada. Filtros indisponíveis.")
                return

            col1, col2, col3 = st.columns(3)
            
            servidor_options = sorted(processed_df['servidor'].unique())
            
            # Verificações de colunas para evitar KeyError
            mes_options = []
            if 'mes' in processed_df.columns:
                mes_options = sorted(processed_df['mes'].dropna().unique())
                
            assunto_options = []
            if 'ASSUNTO_PRINCIPAL' in processed_df.columns:
                assunto_options = sorted(processed_df['ASSUNTO_PRINCIPAL'].dropna().unique())
                
            polo_passivo_options = []
            if 'POLO_PASSIVO' in processed_df.columns:
                polo_passivo_options = sorted(processed_df['POLO_PASSIVO'].dropna().unique())
            
            vara_options = []
            if 'vara' in processed_df.columns:
                vara_options = sorted(processed_df['vara'].unique())
            
            with col1:
                servidor_filter = st.multiselect(
                    "Filtrar por Servidor",
                    options=servidor_options,
                    default=None
                )
                
                mes_filter = st.multiselect(
                    "Filtrar por Mês (Chegada)",
                    options=mes_options, 
                    default=None
                )
            
            with col2:
                polo_passivo_filter = st.multiselect(
                    "Filtrar por Polo Passivo",
                    options=polo_passivo_options,
                    default=None
                )
                
                assunto_filter = st.multiselect(
                    "Filtrar por Assunto",
                    options=assunto_options,
                    default=None
                )
            
            with col3:
                vara_filter = st.multiselect(
                    "Filtrar por Vara",
                    options=vara_options,
                    default=None
                )
            
            # Aplicar filtros
            filtered_df = processed_df.copy()
            filtros_aplicados = []
            
            if servidor_filter:
                filtered_df = filtered_df[filtered_df['servidor'].isin(servidor_filter)]
                filtros_aplicados.append(f"Servidor: {', '.join(servidor_filter)}")
            
            if mes_filter and 'mes' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['mes'].isin(mes_filter)]
                filtros_aplicados.append(f"Mês (Chegada): {', '.join(map(str, mes_filter))}")
            
            if polo_passivo_filter and 'POLO_PASSIVO' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['POLO_PASSIVO'].isin(polo_passivo_filter)]
                filtros_aplicados.append(f"Polo Passivo: {', '.join(polo_passivo_filter)}")
            
            if assunto_filter and 'ASSUNTO_PRINCIPAL' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['ASSUNTO_PRINCIPAL'].isin(assunto_filter)]
                filtros_aplicados.append(f"Assunto: {', '.join(assunto_filter)}")
            
            if vara_filter and 'vara' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['vara'].isin(vara_filter)]
                filtros_aplicados.append(f"Vara: {', '.join(vara_filter)}")
            
            filtros_texto = " | ".join(filtros_aplicados) if filtros_aplicados else "Nenhum filtro aplicado"
            
            st.metric("Processos Filtrados", len(filtered_df))
            
            if len(filtered_df) > 0:
                # Exibir dados filtrados
                colunas_filtro = [
                    'NUMERO_PROCESSO', 'POLO_ATIVO', 'POLO_PASSIVO', 'data_chegada_formatada',
                    'mes', 'DIAS', 'servidor', 'vara', 'ASSUNTO_PRINCIPAL'
                ]
                
                # Filtra apenas colunas que realmente existem após o processamento
                colunas_existentes = [col for col in colunas_filtro if col in filtered_df.columns]
                display_filtered = filtered_df[colunas_existentes].copy()
                
                # Renomeia para exibição no Streamlit e para o PDF
                display_filtered.columns = [
                    'Nº Processo', 'Polo Ativo', 'Polo Passivo', 'Data Chegada',
                    'Mês', 'Dias', 'Servidor', 'Vara', 'Assunto Principal'
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

        # --- Tab 4: Atribuição Manual ---
        with tab4:
            st.markdown("### ✍️ Atribuição Manual de Servidores")
            
            # Identificar processos APENAS sem etiqueta de servidor
            processos_sem_etiqueta = processed_df[
                # Filtra processos que não têm etiqueta OU que têm etiqueta genérica/de servidor diferente da lista
                (processed_df['servidor'].isin(["Sem etiqueta", "Não atribuído"])) 
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
                        # --- QUADRO DE INFORMAÇÕES DO PROCESSO ---
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
                        st.markdown(f"**Data de Chegada:** {processo_info.get('data_chegada_formatada', 'N/A')} (Há **{processo_info.get('DIAS', '0')}** dias)")
                        # --- FIM DO QUADRO DE INFORMAÇÕES ---
                        
                        # Seleção de servidor (usando a lista fixa definida no início do código)
                        novo_servidor = st.selectbox(
                            "Atribuir servidor:",
                            options=SERVIDORES_DISPONIVEIS, # LISTA FIXA APLICADA AQUI
                            key="novo_servidor"
                        )
                        
                        # Botão para aplicar a alteração
                        if st.button("💾 Aplicar Atribuição", key="aplicar_edicao"):
                            
                            # Criar registro da atribuição
                            atribuicao = {
                                'NUMERO_PROCESSO': processo_info['NUMERO_PROCESSO'],
                                'vara': vara_final,
                                'ORGAO_JULGADOR': orgao_julgador, # Mapeado para o nome padronizado
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
                st.markdown("#### ✅ Processos Atribuídos Nesta Sessão")
                
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
                        st.session_state.atribuicoes_servidores = pd.DataFrame(columns=['NUMERO_PROCESSO', 'vara', 'ORGAO_JULGADOR', 'servidor', 'data_atribuicao', 'POLO_ATIVO', 'ASSUNTO_PRINCIPAL'])
                        st.rerun()
                        
                else:
                    st.info("Nenhum processo atribuído ainda. Use o quadro à esquerda para fazer as primeiras atribuições.")
        
        # --- FIM DAS ABAS ---
    
    else:
        # Tela inicial quando não há arquivo
        st.markdown("""
        <div class="upload-section">
            <h3>👋 Bem-vindo ao Sistema de Gestão de Processos Judiciais</h3>
            <p>Faça o upload do(s) arquivo(s) CSV exportado(s) do PJE (tanto painel geral quanto tarefas específicas). O sistema irá **unificar os dados** e remover processos duplicados automaticamente, garantindo a análise mais completa e consistente possível.</p>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
