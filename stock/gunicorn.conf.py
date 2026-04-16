def on_starting(server):
    pass

    


def post_fork(server, worker):
    from services.kafka_client import Client
    import app

    stock_kafka = Client(app.service_name, [])
    app.kafka_producer = stock_kafka.producer