const { Kafka } = require('kafkajs');


const kafka = new Kafka({
    clientId: "rocket_app2",
    brokers: ['localhost:9092']
})

const consumer = kafka.consumer({groupId: 'pagto'});

const producer = kafka.producer();

const run = async () => {

    await consumer.connect();
    await consumer.subscribe({ topic: 'ORDEM_RECEBIDA', fromBeginning: true});

    await consumer.run({
        eachMessage: async ({ topic, partition, message}) => {
            let pedido = JSON.parse(message.value.toString());
            await processarPagamento(pedido)
        },
    })

}

const processarPagamento = async (pedido) => {
    pedido.status = "PAGO";

    pedido.payment_id = Math.floor(Date.now() / 1000);

    await producer.connect();
    await producer.send({
        topic: "ORDEM_PAGA",
        messages:[
            {value: JSON.stringify(pedido)},
        ],
    })

    await producer.disconnect()
}

run().catch(console.error)

/* 
bin/kafka-topics.sh --create --topic ORDEM_RECEBIDA --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic ORDEM_PAGA --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic ORDEM_FATURADA --bootstrap-server localhost:9092 */