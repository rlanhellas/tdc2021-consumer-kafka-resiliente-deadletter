package br.com.tdc2021.lanhellas.consumerkafkaresiliente.consumidorkafka;

import br.com.tdc2021.lanhellas.consumerkafkaresiliente.entidade.Cliente;
import br.com.tdc2021.lanhellas.consumerkafkaresiliente.repositorio.ClienteRepositorio;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hibernate.exception.JDBCConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class ConsumidorCliente {

    @Value("${app.tempo-nack-ms}")
    private long tempoNackMs;

    @Value("${app.topico-deadletter}")
    private String topicoDeadletter;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ClienteRepositorio clienteRepositorio;
    private final KafkaTemplate<String, Cliente> kafkaTemplate;

    public ConsumidorCliente(KafkaTemplate<String, Cliente> kafkaTemplate, ClienteRepositorio clienteRepositorio) {
        this.kafkaTemplate = kafkaTemplate;
        this.clienteRepositorio = clienteRepositorio;
    }

    @KafkaListener(topics = "${app.topico-cliente}")
    public void consumir(@Payload Cliente cliente,
                         @Header(value = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) String key,
                         @Header(KafkaHeaders.RECEIVED_TOPIC) String topico,
                         @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts,
                         Acknowledgment ack) {
        try {
            logger.info("Iniciando consumo do tópico {}, key {}, Cpf Cliente {}", topico, key, cliente.getCpf());
            clienteRepositorio.save(cliente);
            commit(ack);
        } catch (Exception e) {
            /*
             * Se ocorrer um erro de conexão com a base (ex: timeout por firewall, credenciais inválidas ..) então tentaremos
             * novamente pois não adianta passar para a próxima mensagem, já que todas precisam ser salvas na base.
             * */
            if (e.getCause() instanceof JDBCConnectionException) {
                logger.error("Problemas ao comunicar com a base de dados, tentaremos novamente em 10segundos", e);
                ack.nack(tempoNackMs);
            } else {
                /*
                 * Qualquer outro erro não mapeado, estamos apenas dando commit na mensagem e enviando para o deadletter
                 * */
                logger.error("Erro desconhecido ao tentar salvar", e);
                if (enviarParaDLQ(cliente, topico, e.getMessage())) {
                    commit(ack);
                } else {
                    ack.nack(tempoNackMs);
                }
            }
        }
    }

    private void commit(Acknowledgment ack) {
        ack.acknowledge();
        logger.info("Commit realizado");
    }

    private boolean enviarParaDLQ(Cliente cliente, String topicoOriginal, String msgErro) {
        var record = new ProducerRecord<String, Cliente>(topicoDeadletter, cliente);
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, topicoOriginal.getBytes());
        record.headers().add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, msgErro.getBytes());
        try {
            logger.warn("Enviando cliente cpf {} para DLQ no tópico {}", cliente.getCpf(), topicoDeadletter);
            kafkaTemplate.send(record).get();
            return true;
        } catch (Exception e) {
            logger.error("Ocorreram erros ao tentar enviar para o DLQ", e);
            return false;
        }
    }

}
