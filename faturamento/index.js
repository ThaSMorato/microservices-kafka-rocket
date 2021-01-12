const { Kafka } = require('kafkajs');


const kafka = new Kafka({
    clientId: "rocket_app2",
    brokers: ['127.0.0.1:9092']
})

const consumer = kafka.consumer({groupId: 'invoice'});

const producer = kafka.producer();

const run = async () => {

    await consumer.connect();
    await consumer.subscribe({ topic: 'ORDEM_PAGA', fromBeginning: true});

    await consumer.run({
        eachMessage: async ({ topic, partition, message}) => {
            let pedido = JSON.parse(message.value.toString());
            console.log(pedido);
            await faturarPagamento(pedido)
        },
    })

}

const faturarPagamento = async (pedido) => {

    pedido.nfe = `nfe_${pedido.id}_2020.xml`;

    pedido.status = 'FATURADO';

    await producer.connect();
    await producer.send({
        topic: "ORDEM_FATURADA",
        messages:[
            {value: JSON.stringify(pedido)},
        ],
    })

    await producer.disconnect()
}

run().catch(console.error)
