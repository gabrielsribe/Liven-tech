# Desafio de Data Engineering – Liven
## Gabriel de Souza Ribeiro

Este repositório tem como objetivo disponibilizar a solução do desafio técnico para a vaga de Senior Data Engineer na empresa Liven. Nele, estão incluídos o notebook com o pipeline de processamento e os diagramas referentes à modelagem dos dados.

## Sobre o desafio

O desafio consiste em propor uma modelagem de dados para a base de um E-Commerce. A base de dados está disponível no seguinte link[https://www.kaggle.com/datasets/terencicp/e-commerce-dataset-by-olist-as-an-sqlite-database]:

## Ferramentas utilizadas

Para a resolução do desafio, optou-se por seguir uma abordagem mais simples, utilizando de ferramentas abertas. Sendo elas:

 - Google Collab Notebooks[https://colab.research.google.com/], para a construção do código do pipeline de dados, utilizando Pyspark.
 - App Diagrams[https://app.diagrams.net]


 ## Solução:

 A primeira etapa do desafio consistia em propor uma modelagem de dados onde a partir da base de dados disponibilizada, fosse possivel atender os seguintes casos de uso:

**Faturamento por Categoria de Produto**
- Quantidade de compras e valor faturado (R$) por categoria de produto
- Filtros: data da compra, vendedor, produto, tipo de pagamento

**Avaliações de Produtos**
- Distribuição de avaliações por nota e volume de compras por faixa
- Filtros: data da avaliação, cliente, estado do cliente

Para isso, após analise do schema disponilizado, propoem-se a seguinte estrutura de dados:
![alt text](diagrams/schema.png?raw=true "Schema")

A seguir, será detalhada a lógica por trás de cada decisão tomada na modelagem dimensional.

## Tabelas fato: FATO_VENDAS e FATO_AVALIACOES
### Por que duas tabelas de fato?

`FATO_VENDAS` 

O objetivo dessa tabela é capturar todas as métricas transacionais relacionadas a vendas.
Contém medidas de negócio como valor_faturado, quantidade_itens e frete

Permite análises de:

- Faturamento por categoria/produto/vendedor
- Performance de canais de venda
- Eficiência logística (prazos, fretes)

`FATO_AVALIACOES`

O objetivo dessa tabela é medir a satisfação do cliente e relacioná-la com o comportamento de compra.
Contém métricas qualitativas (review_score) e quantitativas (volume_compras)

Permite análises de:

- Satisfação por segmento de clientes
- Correlação entre volume de compras e avaliações
- Identificação de clientes valiosos (alto volume + alta satisfação)
- Atende ao requisito de "Avaliações de Produtos"


Vantagens dessa separação:

- Coesão: Cada fato tem um propósito bem definido

- Performance: Consultas mais eficientes para cada caso de uso analítico

- Flexibilidade: Evolução independente



## Tabelas Dimensão
Estrutura das Tabelas de Dimensão:


|Dimensão|Objetivo
|--|--|
|`DIM_PRODUTO`|Armazena atributos dos produtos|
|`DIM_CLIENTE`|Armazena atributos dos clientes|
|`DIM_VENDEDOR`|Armazena atributos dos vendedores|
|`DIM_TEMPO`|Padroniza a análise temporal|


Foi aplicada uma desnormalização nas tabelas de origem, de modo que cada tabela de dimensão contém todos os atributos necessários para as análises, evitando a necessidade de joins complexos durante as consultas. Por exemplo, a dimensão de clientes (`DIM_CLIENTE`) já inclui informações como cidade e estado, permitindo filtragens diretas por localização.

Foram utilizadas também surrogate keys nas tabelas dimensionais, como id_produto e id_cliente, representadas por inteiros sequenciais em vez de identificadores naturais como SKUs ou CPFs. Essa abordagem traz vantagens como melhor performance em operações de join, por serem inteiros, além de oferecer isolamento em relação a mudanças nos sistemas de origem.

## Pipeline de dados

Para a construção do pipeline, toda a lógica foi implementada em um único notebook, com o objetivo de facilitar o processo de avaliação. Embora, em um ambiente produtivo, as etapas fossem naturalmente separadas seguindo a arquitetura em camadas (bronze, silver e gold), a centralização da lógica neste caso favorece a visualização completa do fluxo em um só lugar. Além disso, o notebook contém diversos comentários e comandos de depuração, como `display()`, que foram mantidos intencionalmente para facilitar a análise dos dados intermediários, permitindo que os resultados sejam visualizados diretamente, sem a necessidade de executar novamente os blocos. Por fim, alguns comandos como `write()` estão presentes apenas de forma ilustrativa, sem realizar a escrita de fato, com o intuito de demonstrar como a escrita dos dados poderia ser feita, incluindo sugestões de particionamento.