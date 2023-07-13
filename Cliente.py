import socket
import random
import threading
from datetime import datetime
import datetime

from Mensagem import Mensagem


'''
    Classe Cliente: Responsável por realizar requisições em 
    qualquer servidor, tanto para inserir informações key-value 
    no sistema, quanto para obtê-las.
'''
class Cliente:
    def __init__(self, servidors):
        self.servidores = servidors
        self.timestamp = datetime.datetime.min
        self.put_ok_recebido = False
        self.get_ok_recebido = False
        self.requisicao_get = 0

    '''
        Faz o direcionamento para os métodos adequados de acordo com a requisição do usuário.
    '''
    def iniciar(self):
        while True:
            opcao = input("Menu Interativo:\n1. INIT\n2. PUT\n3. GET\nEscolha uma opção: ")
            if opcao == '1':
                self.inicializar()
            elif opcao == '2':
                self.put_ok_recebido =  False
                self.enviar_put()
                while not self.put_ok_recebido: #Fornecer o menu novamente quando a mensagem de PUT_OK for exibida.
                    i = 1
            elif opcao == '3':
                self.get_ok_recebido = False #Fornecer o menu novamente quando a mensagem de GET ou TRY_SERVER for exibida.
                self.enviar_get()
                while not self.get_ok_recebido:
                    i = 1

    '''
        Função responsável pela requisição de INIT. 
        Pede a lista de servidores (IP e Porta).
    '''
    def inicializar(self):
        for i in range(len(self.servidores)):
            ip = input("Digite o IP do servidor {}: ".format(i+1))
            porta = int(input("Digite a porta do servidor {}: ".format(i+1)))
            self.servidores[i] = (ip, porta)

    '''
        Função responsável por permitir que o cliente consiga receber do líder 
        as mensagens respostas para PUT e GET para exibir as mensagens adequadas.
        Utiliza thread para lidar com estas requisições.
    '''
    def enable_client_server(self, client_socket, ip, porta):
        # Cria um socket de servidor para aceitar a conexão
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_ip = client_socket.getsockname()[0]
        client_port = client_socket.getsockname()[1]
        server_socket.bind((client_ip, client_port))
        server_socket.listen()

        def handle_requests():
            while True:
                conn, addr = server_socket.accept()
                data = conn.recv(1024)
                mensagem_resposta = Mensagem.deserializar(data)

                if mensagem_resposta.tipo == 'PUT_OK':
                    self.timestamp = max(self.timestamp, mensagem_resposta.timestamp)
                    print("PUT_OK key: {} value: {} timestamp: {} realizada no servidor {}:{}".format(
                        mensagem_resposta.key, mensagem_resposta.value, mensagem_resposta.timestamp, ip, porta))
                    self.put_ok_recebido = True
                elif mensagem_resposta.tipo == 'GET':
                    self.timestamp = max(self.timestamp, mensagem_resposta.timestamp_servidor)
                    print("GET key: {} value: {} obtido do servidor {}:{}, meu timestamp {} e do servidor: {}".format(
                        mensagem_resposta.key, mensagem_resposta.value, ip, porta,
                        self.timestamp, mensagem_resposta.timestamp_servidor ))
                    self.get_ok_recebido = True
                elif mensagem_resposta.tipo == 'TRY_OTHER_SERVER_OR_LATER':
                    print("TRY_OTHER_SERVER_OR_LATER: Tente outro servidor ou tente mais tarde")
                    self.get_ok_recebido = True

        # Cria uma thread para lidar com as requisições
        request_thread = threading.Thread(target=handle_requests)
        request_thread.daemon = True
        request_thread.start()

    '''
        Função responsável por enviar a requisição de PUT ao servidor. Pede a 
        chave e o valor a ser adicionado.
    '''
    def enviar_put(self):
        key = input("Digite a chave (key): ")
        value = input("Digite o valor (value): ")

        servidor = random.choice(self.servidores)
        mensagem = Mensagem(
            tipo='PUT',
            key=key,
            value=value,
            timestamp_cliente=self.timestamp
        )
        self.enviar_mensagem(servidor[0], servidor[1], mensagem)

    '''
        Função responsável por enviar a requisição de GET ao servidor. Pede 
        apenas a chave. Para testar TRY_SERVER_OR_LATER, estou forçando o primeiro
        get ir ao líder e o segundo para um servidor que não é líder. Dessa forma é 
        possível testar o delay de replicação.
    '''
    def enviar_get(self):
        self.requisicao_get += 1
        key = input("Digite a chave (key): ")
        servidor = random.choice(self.servidores)
        mensagem = Mensagem(
            tipo='GET',
            key=key,
            timestamp_cliente=self.timestamp
        )
        if self.requisicao_get == 1:
            self.enviar_mensagem('127.0.0.1', 1098, mensagem) # enviar mensagem ao líder
        elif self.requisicao_get == 2:
            self.enviar_mensagem('127.0.0.1', 1097, mensagem) # enviar mensagem não líder
        else:
            self.enviar_mensagem(servidor[0], servidor[1], mensagem)

    '''
        Função responsável para enviar as mensagens para um servidor aleatório.
        Novamente usa thread para permitir o envio enquanto aguarda as mensagens de retorno.
    '''
    def enviar_mensagem(self, ip, porta, mensagem):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((ip, porta))
        mensagem.cliente_address = client_socket.getsockname()
        client_socket.send(mensagem.serializar())

        threading.Thread(target=self.enable_client_server, args=(client_socket, ip, porta)).start()

if __name__ == '__main__':
    servidores = [('', 0), ('', 0), ('', 0)]  # Inicialização vazia dos servidores
    cliente = Cliente(servidores)
    cliente.iniciar()
