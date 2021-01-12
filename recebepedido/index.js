const express = require('express')
const bodyParser = require('body-parser');
const { Kafka } = require('kafkajs');

const app = express()
app.use(bodyParser.json());

const port = 3001 
const kafka = new Kafka({
    clientId: "rocket_app2",
    brokers: ['127.0.0.1:9092']
})

app.get('/', (req, res) => {
  res.send('Hello World!')
})

app.post('/recebepedido', async (req, res) => {
    let pedido = req.body;

    pedido.id = Math.floor(Date.now() / 1000);
    pedido.status = "PENDENTE PAGAMENTO";

    const producer = kafka.producer();

    await producer.connect();
    await producer.send({
        topic: 'ORDEM_RECEBIDA',
        messages:[
            {value: JSON.stringify(pedido)},
        ],
    })

    await producer.disconnect()

    return res.json({'order': pedido, 'mensage': 'Pedido realizado com sucesso, Estamos analizando seu pagamento. Você receberá informações em breve.'});

})

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})