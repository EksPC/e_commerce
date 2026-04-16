
def on_starting(server):
    pass  # schema handled by init_db.sql


def post_fork(server, worker):
    from services.kafka_client import Client
    import app
    # Producer only — consumer lives in its own process now
    order_kafka = Client(app.service_name, [])
    app.kafka_producer = order_kafka.producer

