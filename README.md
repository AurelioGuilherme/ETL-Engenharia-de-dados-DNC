# ETL-Engenharia-de-dados-DNC


## 🧠 Contexto

A PotatoCore é uma empresa que lida com dados da Lotofácil, um jogo de loteria popular no Brasil. Atualmente, os dados são fornecidos em uma planilha de Excel e precisam ser processados e preparados para análise. Como Engenheiro de Dados, sua missão é otimizar o processo ETL (Extração, Transformação e Carregamento) para facilitar a obtenção de insights a partir desses dados.

O processo atual de ETL é manual e consome muito tempo. Além disso, pode haver inconsistências nos dados devido à natureza manual do processo. O objetivo é criar um pipeline automatizado que permita a extração, transformação e carregamento eficientes dos dados da Lotofácil para facilitar a análise.

## Utilização:
* Toda a análise e o processo de ETL foi diponibilizada no notebook `lotofacil.ipynb` presente dentro da pasta Notebook
* Para utilizar basta clonar o repositório e executar run all
  
## 👀 Em breve 
* Em versões futuras o ETL será feito atravez de um app.py;
* Também esta em processo de desenvolvimento o processo de ETL em um container Docker utilizando o Apache Airflow

### Docker
O Docker é uma plataforma de virtualização de aplicativos que simplifica a criação, distribuição e execução de aplicativos em ambientes isolados chamados de contêineres. Ao invés de virtualizar todo o sistema operacional, como faz a virtualização tradicional, o Docker virtualiza apenas os recursos necessários para executar um aplicativo específico. Isso inclui o sistema de arquivos, as bibliotecas e as dependências.

Para utilizar o processo de ETL é necessário instalar o [Docker Desktop](https://www.docker.com/)
* Após ter instalado o Docker, faça login e inicie-o em sua maquina local:
  
 Para a execução do Docker é necessário que a virtualização de sua maquina local esteja ativa.

 * Tendo os arquivos arquivos deste repositório em uma pasta em sua maquina local, acesse o caminho do mesmo no terminal
   - No Windows basta executar o comando `cd {path_file}`
* Executar o comando no terminal `docker compose up airflow-init` para criar o container Docker contendo o Airflow
* Executar o comando no terminal `docker-compose up -d` para iniciar o container
* Acessar o airflow atravez do seu navegador acessando `localhost:8080`

|Párametro| Valor|
|-|-|
| URL|http://localhost:8080/|
|Usuário| airflow|
|Senha | airflow |







