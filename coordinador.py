import zmq
import time
import analizador

TAM_BATCH = 1599




class Servidor:
    def __init__(self):
        # TODO: meter TAM_BATCH en el servidor, ya que es propio de el.
        self.parser = analizador.AnalizaArchivos()
        self.parser.analiza_archivos('vector.txt', modo='vector')
        self.parser.analiza_archivos('matriz.txt', modo='matriz')
        self.contexto = zmq.Context()
        self.socket = None
        self.respuesta = None
        self.trabajos_pendientes = self.calcular_trabajos()
        self.iniciar_comunicacion()

    def iniciar_comunicacion(self):
        self.socket = self.contexto.socket(zmq.REP)
        self.socket.bind('tcp://0.0.0.0:5000')

    def calcular_trabajos(self):
        trabajos_totales = self.parser.cantidad_filas() // TAM_BATCH
        lista_trabajos = []
        for i in range(trabajos_totales):
            fila_inicial = i * TAM_BATCH
            fila_final = fila_inicial + TAM_BATCH + 1;
            lista_trabajos.append((fila_inicial, fila_final))
        return lista_trabajos

    def escuchar(self):
        while len(self.trabajos_pendientes) > 0:
            self.dirigir_entrantes()
        else:
            print("Todos los trabajos enviados")

    def dirigir_entrantes(self):
        self.respuesta = self.socket.recv_json()
        if self.respuesta['mens'] == 'disponible':
            self.enviar_trabajo(self.trabajos_pendientes.pop())
        if self.respuesta['mens'] == 'solucion':
            self.recibir_solucion()

    def enviar_trabajo(self, indices):
        self.socket.send_json({'mens': 'trabajo'})
        primer_fila = indices[0]
        ultima_fila = indices[1]
        # Bucle de envio de filas
        for nro_fila in range(primer_fila, ultima_fila):
            # Pido los datos a SQLITE
            self.parser.cursor.execute("SELECT fila, pos_elemento, valor, resultado_valor "
                                       "FROM triplas LEFT JOIN resultados "
                                       "ON triplas.fila = resultados.resultado_fila "
                                       "WHERE fila = ?;", (nro_fila,))
            # Bucle de envio de datos
            for dato in self.parser.cursor.fetchall():
                respuesta = self.socket.recv_json()
                if respuesta['mens'] == 'envie datos' or respuesta['mens'] == 'RX' or respuesta[
                    'mens'] == 'fila RX':
                    self.socket.send_json({'mens': 'dato',
                                           'fila': dato[0],
                                           'pos': dato[1],
                                           'valor': dato[2],
                                           'resul': dato[3]}
                                          )
            else:  # Cuando se temina el envio de datos mandamos el fin de fila
                respuesta = self.socket.recv_json()
                if respuesta['mens'] == 'RX':
                    self.socket.send_json({'mens': 'fin fila'})
        else:  # Cuando se termina el envio de filas mandamos fin de trabajo
            respuesta = self.socket.recv_json()
            if respuesta['mens'] == 'fila RX':
                self.socket.send_json({'mens': 'fin trabajo'})

    def recibir_solucion(self):
        pass

if __name__ == "__main__":
    servidor = Servidor()
    servidor.parser.cursor.execute('SELECT COUNT(*) FROM triplas;')
    print(f'Cantidad de triplas = {servidor.parser.cursor.fetchone()[0]}')
    servidor.parser.cursor.execute('SELECT COUNT(*) FROM resultados;')
    print(f'Cantidad de resultados (no nulos) = {servidor.parser.cursor.fetchone()[0]}')
    print(f'Cantidad de trabajos: {servidor.calcular_trabajos()}')
    print("Listo para escuchar")

    # Bucle de envio de trabajos
    servidor.escuchar()
