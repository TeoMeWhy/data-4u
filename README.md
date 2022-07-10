# Data for You

Repositório destinado a adição de dados no datalake mantido pelo canal [Téo Me Why](https://www.twitch.tv/teomewhy).

Todos os códigos necessários e informações básicas sobre os dados serão disponibilizados nestes repositório.

## Dados existentes

|Nome|Contexto|Fonte|Schema|
|---|---|---|---|
|Censo escolar|Microdados do Censo Escolar da Educacação Básica|[:link:](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar)|bronze_censo_escolar|
|Enem|Microdados do Enem|[:link:](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem)|bronze_enem|
|Gamers Club|Estatísticas de partidas, medalhas, players|[:link:](https://www.kaggle.com/datasets/gamersclub/brazilian-csgo-plataform-dataset-by-gamers-club)|bronze_gc|
|Olist|Vendas e-commerce|[:link:](https://www.kaggle.com/datasets/gamersclub/brazilian-csgo-plataform-dataset-by-gamers-club)|bronze_olist|

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
