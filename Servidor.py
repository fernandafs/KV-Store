import time
from datetime import datetime
import socket
import threading

from Mensagem import Mensagem


'''
 Classe Servidor: Responsável por permitir armazenar 
 pares key-value, acessá-los e coordenar o 
 sistema servidor-líder.
'''
class Servidor:
    def __init__(self, ip_server, porta_server, leader_ip_server, leader_port_server, servidors):
        self.ip = ip_server
        self.porta = porta_server
        self.leader_ip = leader_ip_server
        self.leader_port = leader_port_server
        self.tabela_hash = {}
        self.servidores = servidors
        self.replication_ok_count = 0
        self.replication_ok_lock = threading.Lock()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((ip_server, porta_server))
        self.server_socket.listen(5)

    ''' 
        Função utilizada para iniciar o servidor, 
        usa thread para conseguir diversos clientes ao mesmo tempo.
    '''
    def iniciar(self):
        print("Servidor iniciado em {}:{}".format(self.ip, self.porta))
        threading.Thread(target=self.ouvir_clientes).start()

    '''
        Aceita a conexão com o cliente e aciona a função que lida com as
        requisições, também com o uso de threads.
    '''
    def ouvir_clientes(self):
        while True:
            client_socket, address = self.server_socket.accept()
            threading.Thread(target=self.lidar_com_requisicao, args=(client_socket, address)).start()

    '''
        Trata a requisição do cliente, fazendo o redirecionamento aos métodos adequados.
    '''
    def lidar_com_requisicao(self, client_socket, address):
        while True:
            try:
                data = client_socket.recv(1024)
                if not data:
                    break

                mensagem = Mensagem.deserializar(data)

                if mensagem.tipo == 'PUT':
                    self.processar_put(address, mensagem)
                elif mensagem.tipo == 'REPLICATION':
                    self.processar_replication(mensagem)
                elif mensagem.tipo == 'REPLICATION_OK':
                    self.processar_replication_ok(mensagem)
                elif mensagem.tipo == 'GET':
                    self.processar_get(mensagem)

            except Exception as e:
                print("Erro ao lidar com a requisição do cliente {}: {}".format(address, str(e)))
                break

        client_socket.close()

    '''
        Função responsável pela requisição de PUT. Caso o servidor ao qual recebeu não seja o líder,
        ele o direciona para tal. O Líder adiciona em sua tabela de hash e direciona para a replicação
        nos demais servidores.
    '''
    def processar_put(self, address, mensagem):
        if self.leader_ip == self.ip and self.leader_port == self.porta:
            print("Cliente {}: PUT key: {} value: {}".format(address, mensagem.key, mensagem.value))
            mensagem.timestamp_servidor = datetime.now()
            mensagem.timestamp = mensagem.timestamp_servidor
            self.tabela_hash[mensagem.key] = {
                'value': mensagem.value,
                'timestamp': mensagem.timestamp
            }

            time.sleep(10) # teste para delay de replicação

            self.replicar_dados(mensagem)  # Realiza a replicação para os outros servidores

        else:
            print("Encaminhando PUT key: {} value: {}".format(mensagem.key, mensagem.value))

            mensagem_enc_put = Mensagem(
                tipo='PUT',
                key=mensagem.key,
                value=mensagem.value,
                cliente_address=mensagem.cliente_address
            )
            self.enviar_mensagem_ao_lider(mensagem_enc_put)

    '''
        Função para direcionar a requisição de REPLICATION. Ele perpassa por toda a lista de 
        servidores, e caso não seja o próprio líder, organiza a mensagem de replicação e chama a função
        que envia aos servidores.
    '''
    def replicar_dados(self, mensagem):
        for server_ip, server_port in self.servidores:
            if (server_ip, server_port) != (self.leader_ip, self.leader_port):
                mensagem_replication = Mensagem(
                    tipo='REPLICATION',
                    key=mensagem.key,
                    value=mensagem.value,
                    timestamp=mensagem.timestamp,
                    cliente_address=mensagem.cliente_address
                )
                self.enviar_mensagem_a_outro_servidor(server_ip, server_port, mensagem_replication)

    '''
        Função responsável por lídar com a requisição de REPLICATION. O servidor adiciona 
        o valor dentro da key, com o respectivo timestamp e envia a mensagem de REPLICATION_OK
        para o líder.
    '''
    def processar_replication(self, mensagem):

        self.tabela_hash[mensagem.key] = {
            'value': mensagem.value,
            'timestamp': mensagem.timestamp
        }

        print("Replication key: {} value: {} ts: {}".format(mensagem.key, mensagem.value, mensagem.timestamp))

        mensagem_resposta = Mensagem(
            tipo='REPLICATION_OK',
            key=mensagem.key,
            value=mensagem.value,
            timestamp=mensagem.timestamp,
            cliente_address=mensagem.cliente_address
        )
        self.enviar_mensagem_ao_lider(mensagem_resposta)

    '''
        Função responsável pela requisição de REPLICATION_OK. A cada replicação o contador aumenta, e
        ao chegar ao tamanho de servidores - 1, envia a mensagem de PUT_OK ao cliente. Finalizando 
        o processo, o contador é zerado.
    '''
    def processar_replication_ok(self, mensagem):
        with self.replication_ok_lock:
            self.replication_ok_count += 1
            if self.replication_ok_count >= len(self.servidores) - 1: # Líder recebeu REPLICATION_OK de todos os servidores
                print("Enviando PUT_OK ao Cliente {} da key: {}  ts: {}"
                      .format(mensagem.cliente_address, mensagem.key, mensagem.timestamp))
                mensagem_resposta = Mensagem(
                    tipo='PUT_OK',
                    key=mensagem.key,
                    value=mensagem.value,
                    timestamp=mensagem.timestamp
                )
                self.replication_ok_count = 0
                self.enviar_mensagem_ao_cliente(mensagem.cliente_address, mensagem_resposta)

    '''
        Função responsável pela requisição GET. Existem três casos dependendo do resultado:
        se a key não existir na tabela hash, ele retorna "None". Caso exista e o timestamp key seja
        maior ou igual que o timestamp cliente, ele retorna o valor da key. Por fim, caso exista
        porém o valor do timestamp key seja menor que o timestamp cliente, retorna a mensagem de
        TRY_SERVER_OR_LATER.
    '''
    def processar_get(self, mensagem):
        print("Cliente {}: GET key: {}".format((self.ip, self.porta), mensagem.key))
        if mensagem.timestamp_cliente == None:
            mensagem.timestamp_cliente = datetime.min

        if mensagem.key in self.tabela_hash:
            data = self.tabela_hash[mensagem.key]
            if data['timestamp'] >= mensagem.timestamp_cliente:
                mensagem_resposta = Mensagem(
                    tipo='GET',
                    key=mensagem.key,
                    value=data['value'],
                    timestamp_cliente=mensagem.timestamp_cliente,
                    timestamp_servidor=data['timestamp'],
                    cliente_address=mensagem.cliente_address
                )
            else:
                mensagem_resposta = Mensagem(
                    tipo='TRY_OTHER_SERVER_OR_LATER',
                    key=mensagem.key,
                    cliente_address=mensagem.cliente_address
                )
        else:
            mensagem_resposta = Mensagem(
                tipo='GET',
                key=mensagem.key,
                value=None,
                timestamp_cliente=mensagem.timestamp_cliente,
                timestamp_servidor=datetime.min,
                cliente_address=mensagem.cliente_address
            )

        self.enviar_mensagem_ao_cliente(mensagem.cliente_address, mensagem_resposta)

    '''
     As funções abaixo de envio de mensagem são responsáveis por se conectar com o servidor ou cliente,
     baseado na necessidade, enviando as informações necessárias.
    '''
    @staticmethod
    def enviar_mensagem(sock, mensagem):
        sock.send(mensagem.serializar())

    def enviar_mensagem_a_outro_servidor(self, ip_client, porta_client, mensagem):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((ip_client, porta_client))
        self.enviar_mensagem(client_socket, mensagem)
        client_socket.close()

    def enviar_mensagem_ao_lider(self, mensagem):
        self.enviar_mensagem_a_outro_servidor(self.leader_ip, self.leader_port, mensagem)

    def enviar_mensagem_ao_cliente(self, cliente_address, mensagem):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((cliente_address[0], cliente_address[1]))
        self.enviar_mensagem(client_socket, mensagem)
        client_socket.close()


if __name__ == '__main__':
    ip = input("Digite o IP deste servidor: ")
    porta = int(input("Digite a porta deste servidor: "))
    leader_ip = input("Digite o IP do líder: ")
    leader_port = int(input("Digite a porta do líder: "))

    servidores = [
        ('127.0.0.1', 10099),
        ('127.0.0.1', 10098),
        ('127.0.0.1', 10097)
    ]

    servidor = Servidor(ip, porta, leader_ip, leader_port, servidores)
    servidor.iniciar()
