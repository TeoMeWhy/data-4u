# Data for You

Repositório destinado a adição de dados no datalake mantido pelo canal [Téo Me Why](https://www.twitch.tv/teomewhy).

Todos os códigos necessários e informações básicas sobre os dados serão disponibilizados nestes repositório.

Estes dados são disponíveis em um datalake construido utilizando o Databricks com AWS. Todos os inscritos no canal [Téo Me Why](twitch.tv/teomewhy) tem acesso ao Datalake.

## Dados existentes

|Nome|Contexto|Fonte|
|---|---|---|
|DataSUS|Dados do Sistema de Informação Hospitalar (SIH) e de Nascidos Vivos (SINASC)| [datasus.saude.gov.br](https://datasus.saude.gov.br/transferencia-de-arquivos/)|
|Dota2|Dados de partidas profissionais de Dota2 desde 2012| [opendota.com](https://www.opendota.com/)|
|IGDB|Dados de jogos digitais, franquias, empresas desenvolvedoras e publicadoras| [igdb.com](https://www.igdb.com/)|
|Olist|Dados de transações realizadas em um ecommerce|[kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)|
|Tabnews|Dados de posts realizados no TabNews|[tabnews.com.br](https://tabnews.com.br/)|

## Estrutura do projeto

Para consultar os dados seguimos o padrão: `{catálogo}.{database}.{tabela}´.

Assim, temos 3 catálogos diferentes:

Bronze: Dados brutos a partir das fontes em formato Delta
Silver: Dados padronizados de forma mais fácil de leitura e utilização
Gold: Dados agregados em formato de relatórios para serem utilizados em ferramentas de visualização

Você pode consumir os dados da seguinte maneira com SQL:

```sql
SELECT *
FROM silver.olist.pedido
```

## Como solicitar novos dados?

Abra uma issue neste projeto com o seguinte template:

```
Título: Nome da fonte de dados

- Descrição da fonte de dados: 
  - Do que diz respeito?
  - Qual o contexto deste dado?
  - Quantos anos de histórico?
  - Qual o volume?

- Link para acesso aos dados.

- Por que este dado é relevante e deveria estar no datalake?

```
