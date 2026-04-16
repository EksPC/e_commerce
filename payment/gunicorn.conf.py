# gunicorn.conf.py

def on_starting(server):
    """
    Runs once in the master process before workers fork.
    Schema init is a synchronous, one-off operation — no need for async here.
    Using a plain psycopg connection avoids any event loop conflicts with Gunicorn's
    own internals and is simpler to reason about.
    """
    pass
        

def post_fork(server, worker):
    from services.kafka_client import Client
    import app

    payment_kafka = Client(app.service_name, [])
    app.kafka_producer = payment_kafka.producer