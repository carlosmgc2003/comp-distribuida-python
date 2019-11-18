import zmq
import sqlite3


class Esclavo:
    def __init__(self, ip_server):
        self.ip_server = ip_server
        self.contexto = zmq.Context()
        self.socket = self.contexto.socket(zmq.REQ)
        self.socket.connect(f'tcp://{ip_server}:5000')
        self.datos_recibidos = []
        self.conexion = sqlite3.connect('datos_esclavo.db')
        self.cursor = self.conexion.cursor()

    def recibir_datos(self):
        """Estando conectado a un servidor de problema, le notifica que esta listo para recibir
        datos y los recibe."""
        # Decir que estamos disponible
        self.socket.send_json({"mens": "disponible"})
        respuesta = self.socket.recv_json()
        if respuesta['mens'] == 'trabajo':
            self.socket.send_json({'mens': 'envie datos'})
            respuesta = self.socket.recv_json()
            while respuesta['mens'] == 'dato':
                tripla = (respuesta["fila"], respuesta["pos"], respuesta["valor"], respuesta["resul"])
                self.datos_recibidos.append(tripla)
                print(f'Fila: {respuesta["fila"]}, '
                      f'Pos: {respuesta["pos"]}, '
                      f'Valor: {respuesta["valor"]}, '
                      f'Resultado: {respuesta["resul"]}')
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
            print(f'Se recibieron {len(self.datos_recibidos)} datos.')

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
        """Llama a un programa externo en C++ para realizar el calculo numerico del jacobi parcial"""
        pass

    def enviar_solucion(self):
        """Lee los datos calculados por el programa en C++ para luego enviarselo al servidor de problemas"""
        pass
        # self.socket.send_json({'mens':'solucion'})
        # self.cursor.execute("SELECT variable, solucion FROM solucion;")
        # for dupla in self.cursor.fetchall():


if __name__ == "__main__":
    ip_servidor = input("Ingrese la IP del Servidor:")
    esclavo = Esclavo(ip_servidor)
    esclavo.recibir_datos()
    esclavo.escribir_datos()
