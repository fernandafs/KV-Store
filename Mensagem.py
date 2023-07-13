import pickle


'''
 Classe Mensagem: Transferência de mensagens entre cliente e servidor.
'''
class Mensagem:
    def __init__(self, tipo, key=None, value=None, timestamp=None, timestamp_cliente=None, timestamp_servidor=None,
                 cliente_address=None):
        self.tipo = tipo
        self.key = key
        self.value = value
        self.cliente_address = cliente_address
        self.timestamp = timestamp
        self.timestamp_cliente = timestamp_cliente
        self.timestamp_servidor = timestamp_servidor

    def serializar(self):
        return pickle.dumps(self)

    @staticmethod
    def deserializar(data):
        return pickle.loads(data)
