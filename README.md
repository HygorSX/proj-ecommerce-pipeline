# Projeto de Pipeline de E-commerce com Airflow

Este projeto contém uma configuração básica para executar um ambiente Apache Airflow usando Docker e a Astro CLI. O objetivo é fornecer uma estrutura inicial para o desenvolvimento de pipelines de dados de e-commerce.

## Pré-requisitos

Antes de começar, garanta que você tenha as seguintes ferramentas instaladas em sua máquina:

- [Docker](https://docs.docker.com/get-docker/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

## Como Executar o Projeto Localmente

Siga os passos abaixo para configurar e executar o ambiente Airflow.

1. **Clone o repositório:**

   ```bash
   git clone git@github.com:HygorSX/proj-ecommerce-pipeline.git
   cd proj_ecommerce_pipeline
   ```

2. **Inicie o ambiente Airflow:**

   Este comando irá baixar a imagem Docker necessária, construir o projeto e iniciar todos os contêineres do Airflow (webserver, scheduler, etc.).

   ```bash
   astro dev start
   ```

3. **Acesse a Interface do Airflow:**

   Após a inicialização dos contêineres, a interface web do Airflow estará disponível no seu navegador em:

   [http://localhost:8080](http://localhost:8080)

   - **Login:** `admin`
   - **Senha:** `admin`

   O DAG de exemplo `example_astronauts` estará visível e ativado na UI.

## Estrutura do Projeto

- `dags/`: Diretório para seus DAGs Python.
- `Dockerfile`: Define o ambiente de execução do Airflow.
- `requirements.txt`: Adicione as dependências Python do seu projeto aqui.
- `packages.txt`: Adicione as dependências de sistema operacional (pacotes APT) aqui.
- `.gitignore`: Especifica arquivos e diretórios a serem ignorados pelo Git.

## Próximos Passos

Com o ambiente em execução, você pode começar a desenvolver seus próprios DAGs na pasta `dags/`. Qualquer novo arquivo `.py` que você adicionar lá será automaticamente detectado pelo Airflow.