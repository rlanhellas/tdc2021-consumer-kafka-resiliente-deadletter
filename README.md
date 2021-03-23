# TDC 2021 - Consumer Kafka Resiliente

Este projeto possui um consumer com as seguintes features:
1. Retry Simples
2. Deadletter

Para ver o projeto completo acesse: https://github.com/rlanhellas/tdc2021-consumer-kafka-resiliente-retry-seletivo

Para rodar o projeto aconselhamos o uso do OpenJDK 11 + Maven 3.6.3 ou superior.

### Como executar ?
Você precisará das seguintes variáveis de ambiente:

    CONFLUENT_CLOUD_KEY=4TNCBYG7GPNE2HDZ;
    CONFLUENT_CLOUD_SECRET=YOUR_SECRET;
    POSTGRES_URL=jdbc:postgresql://localhost:5432/cliente;
    POSTGRES_PASSWORD=YOUR_PASSWORD;

Após cadastradas as variáveis de ambiente basta executar o comando maven abaixo:

    mvn spring-boot:run