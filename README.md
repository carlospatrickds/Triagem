# Processador de CSV - PJE

## Instruções Rápidas

1. **Instalação**: `pip install streamlit pandas`
2. **Execução**: `streamlit run app_basico.py`

## Versões Disponíveis

- `app_basico.py` - Versão ultra simplificada (recomendada)
- `app_minimo.py` - Versão com mais recursos
- `app_tratamento_dados.py` - Versão completa

## Solução de Problemas

Se encontrar erros:
1. Verifique se o Python está instalado
2. Use a versão mais básica (`app_basico.py`)
3. Instale dependências individualmente: `pip install streamlit==1.15.0 pandas==1.3.5`

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