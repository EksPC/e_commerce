def on_starting(server):
    
    pass

def post_fork(server, worker):
    from services.kafka_client import Client
    import app 

    orchestrator_kafka = Client(app.service_name, [f'{app.service_name}.request'])
    app.kafka_consumer = orchestrator_kafka.consumer
    

