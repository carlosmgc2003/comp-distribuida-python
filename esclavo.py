import zmq
import sqlite3
from subprocess import run
from time import perf_counter

REQUEST_TIMEOUT = 2000

class Esclavo:
    def __init__(self, ip_server):
        self.ip_server = ip_server
        self.datos_recibidos = []
        self.contexto = None
        self.socket = None
        self.iniciar_contexto()
        self.conexion = sqlite3.connect('datos_esclavo.db')
        self.cursor = self.conexion.cursor()
        print('INICIO CORRECTO'.center(50, '='))

    def iniciar_contexto(self):
        self.contexto = zmq.Context()
        self.socket = self.contexto.socket(zmq.REQ)

    def recibir_datos(self):
        """Estando conectado a un servidor de problema, le notifica que esta listo para recibir
        datos y los recibe."""
        # Decir que estamos disponible
        self.datos_recibidos.clear()
        self.socket.connect(f'tcp://{self.ip_server}:5000')
        poll = zmq.Poller()
        poll.register(self.socket, zmq.POLLIN)
        while not self.datos_recibidos:
            self.socket.send_json({"mens": "disponible"})
            espero_respuesta = True
            while espero_respuesta:
                socks = dict(poll.poll(REQUEST_TIMEOUT))
                if socks.get(self.socket) == zmq.POLLIN:
                    respuesta = self.socket.recv_json()
                    if not respuesta:
                        break
                    if respuesta['mens'] == 'trabajo':
                        espero_respuesta = False
                        self.socket.send_json({'mens': 'envie datos'})
                        respuesta = self.socket.recv_json()
                        while respuesta['mens'] == 'dato':
                            tripla = (respuesta["fila"], respuesta["pos"], respuesta["valor"], respuesta["resul"])
                            self.datos_recibidos.append(tripla)
                            self.socket.send_json({'mens': 'RX'})
                            respuesta = self.socket.recv_json()
                            if respuesta['mens'] == 'dato':
                                continue
                            elif respuesta['mens'] == 'fin fila':
                                self.socket.send_json({'mens': 'fila RX'})
                                respuesta = self.socket.recv_json()
                                continue
                            else:
                                break
                        print('DATOS RECIBIDOS'.center(50, '='))
                        print(f'Se recibieron {len(self.datos_recibidos)} datos.')
                        self.socket.disconnect(f'tcp://{self.ip_server}:5000')
                else:
                    self.socket.setsockopt(zmq.LINGER, 0)
                    self.socket.close()
                    poll.unregister(self.socket)
                    self.socket = self.contexto.socket(zmq.REQ)
                    self.socket.connect(f'tcp://{self.ip_server}:5000')
                    poll.register(self.socket, zmq.POLLIN)
                    self.socket.send_json({"mens": "disponible"})



    def escribir_datos(self):
        """Almacena los datos recibidos del servidor en una BD SQLite. Si existe lo elimina"""
        self.cursor.execute("DROP TABLE IF EXISTS datos;")
        self.cursor.execute("CREATE TABLE datos(triplaId INTEGER PRIMARY KEY, "
                            "fila INTEGER, "
                            "pos_elemento INTEGER, "
                            "valor REAL, "
                            "resultado REAL);")
        for tupla in self.datos_recibidos:
            self.cursor.execute("INSERT INTO datos(fila, "
                                "pos_elemento, "
                                "valor, "
                                "resultado) "
                                "VALUES (?, ?, ?, ?)",
                                tupla)
        self.conexion.commit()

    def calcular_solucion(self):
        print('PROCESANDO'.center(50, '='))
        start = perf_counter()
        run("./comp_distribuida", capture_output=True)
        end = perf_counter()
        execution_time = (end - start)
        print(f'Tiempo de calculo {execution_time} seg.'.center(50, '='))
        print('DATOS CALCULADOS'.center(50, '='))

    def enviar_solucion(self):
        """Lee los datos calculados por el programa en C++ para luego enviarselo al servidor de problemas"""
        self.socket.connect(f'tcp://{self.ip_server}:5000')
        self.socket.send_json({'mens': 'solucion lista'})
        respuesta = self.socket.recv_json()
        if respuesta['mens'] == 'listo RX':
            self.cursor.execute("SELECT variable, solucion FROM solucion;")
            for dupla in self.cursor.fetchall():
                self.socket.send_json({'mens': 'solucion',
                                       'variable': dupla[0],
                                       'solucion': dupla[1]})
                respuesta = self.socket.recv_json()
                if respuesta['mens'] == 'RX':
                    # Aca queda el lugar para el manejo de errores
                    continue
            else:
                self.socket.send_json({'mens': 'fin solucion'})
                respuesta = self.socket.recv_json()
                if respuesta['mens'] == 'solucion RX':
                    print("SOLUCION ENVIADA".center(50, '='))
                self.socket.disconnect(f'tcp://{self.ip_server}:5000')

    def cerrar_contexto(self):
        self.socket.close()
        self.contexto.destroy()
        self.iniciar_contexto()

if __name__ == "__main__":
    ip_servidor = input("Ingrese la IP del Servidor [localhost]: ")
    if len(ip_servidor) == 0:
        ip_servidor = 'localhost'
    esclavo = Esclavo(ip_servidor)
    while True:
        esclavo.recibir_datos()
        esclavo.escribir_datos()
        esclavo.calcular_solucion()
        esclavo.enviar_solucion()
        esclavo.cerrar_contexto()
