# project-kafka-spark

## Visão Geral

Este projeto demonstra uma arquitetura de processamento de dados em tempo real utilizando Apache Kafka, Apache Spark, Prometheus e Grafana, totalmente orquestrada via Docker Compose.

## Arquitetura

- **Producer**: Um script `producer.py` gera registros fictícios de transações financeiras e publica no tópico Kafka `financial_transactions`.
- **Kafka**: Cluster composto por 3 controllers e 3 brokers para alta disponibilidade e tolerância a falhas.
- **Spark Consumer**: Um consumidor Spark lê os dados do tópico `financial_transactions`, realiza agregações por `id_vendedor` e publica os resultados no tópico `aggregated_transactions`. O cluster Spark possui 3 workers para processamento distribuído.
- **Monitoramento**: Toda a infraestrutura é monitorada pelo Prometheus, que coleta métricas dos serviços (incluindo logs do JMX Exporter dos brokers Kafka).
- **Visualização**: O Grafana exibe dashboards customizados com base nas métricas coletadas pelo Prometheus.
- **Orquestração**: Todos os serviços rodam em containers Docker, definidos em um arquivo `docker-compose.yml`.
- **Administração Kafka**: Console para administração dos tópicos Kafka disponível em [http://localhost:8080](http://localhost:8080).

## Como Executar

1. **Clone o repositório**
2. **Suba os containers**
   ```sh
   docker-compose up --build
   ```
3. **Acesse os serviços**
   - Kafka Brokers: `localhost:29092`, `localhost:39092`, `localhost:49092`
   - Prometheus: [http://localhost:9090](http://localhost:9090)
   - Grafana: [http://localhost:3000](http://localhost:3000) (usuário/senha padrão: admin/admin)
   - Console de administração Kafka: [http://localhost:8080](http://localhost:8080)

## Estrutura dos Principais Arquivos

- `producer.py`: Geração e envio de dados fictícios para o Kafka.
- `jobs/spark_consumer.py`: Consumo, processamento e agregação dos dados via Spark.
- `docker-compose.yml`: Orquestração dos containers.
- `monitoring/prometheus/prometheus.yml`: Configuração do Prometheus.
- `grafana-data/`: Dados e dashboards do Grafana.
- `volumes/jmx_exporter/kafka-broker.yml`: Configuração do JMX Exporter para métricas do Kafka.

## Observações

- Certifique-se de ter Docker e Docker Compose instalados.
- Os tópicos Kafka são criados automaticamente pelo producer ou podem ser criados manualmente.
- Os dashboards do Grafana podem ser customizados conforme a necessidade.

---

Sinta-se à vontade para adaptar este README conforme o seu projeto evoluir!