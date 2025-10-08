# Tratamento de Dados PJE

Aplicação web para processamento e análise de dados de processos judiciais do PJE (Processo Judicial Eletrônico).

## Funcionalidades

- Carregamento de múltiplos arquivos CSV
- Processamento automático de datas e valores
- Filtragem de dados por diferentes critérios
- Visualizações gráficas interativas
- Estatísticas e análises
- Exportação dos dados processados

## Requisitos

- Python 3.8 ou superior
- Bibliotecas listadas em `requirements.txt`

## Instalação

1. Clone este repositório:
   ```
   git clone https://github.com/seu-usuario/tratamento-dados-pje.git
   cd tratamento-dados-pje
   ```

2. Instale as dependências:
   ```
   pip install -r requirements.txt
   ```

## Uso

1. Execute a aplicação Streamlit:
   ```
   streamlit run app_tratamento_dados.py
   ```

2. Acesse a aplicação no navegador (geralmente em http://localhost:8501)

3. Faça o upload dos arquivos CSV através do painel lateral

4. Utilize os filtros e visualize as análises geradas automaticamente

## Deploy

A aplicação pode ser facilmente implantada no Streamlit Cloud:

1. Faça fork deste repositório para sua conta GitHub
2. Acesse [share.streamlit.io](https://share.streamlit.io/)
3. Faça login com sua conta GitHub
4. Selecione o repositório e o arquivo principal (`app_tratamento_dados.py`)
5. Clique em "Deploy"

## Estrutura de Arquivos

- `app_tratamento_dados.py`: Aplicação principal
- `requirements.txt`: Dependências do projeto
- `README.md`: Documentação

## Formatos de Arquivo Suportados

A aplicação foi projetada para processar arquivos CSV do PJE com as seguintes características:

- Delimitador: ponto e vírgula (;)
- Encoding: UTF-8 ou Latin1
- Colunas esperadas: "Número do Processo", "Classe", "Órgão Julgador", etc.

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para detalhes.

## Contribuições

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.