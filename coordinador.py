import zmq

import analizador

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np

TAM_BATCH = 800


class Servidor:
    def __init__(self):
        # TODO: meter TAM_BATCH en el servidor, ya que es propio de el.
        self.parser = analizador.AnalizaArchivos()
        self.parser.analiza_archivos('vector.txt', modo='vector')
        self.parser.analiza_archivos('matriz.txt', modo='matriz')
        self.contexto = zmq.Context()
        self.socket = None
        self.respuesta = None
        self.solucion = []
        self.trabajos_pendientes = self.calcular_trabajos()
        self.iniciar_comunicacion()

    def iniciar_comunicacion(self):
        """Asignar rol y numero al puerto"""
        self.socket = self.contexto.socket(zmq.REP)
        self.socket.bind('tcp://0.0.0.0:5000')

    def calcular_trabajos(self):
        """Genera una lista de duplas con los indices de inicio y fin de cada trabajo
        que se entregará a los esclavos"""
        trabajos_totales = self.parser.cantidad_filas() // TAM_BATCH
        lista_trabajos = []
        for i in range(trabajos_totales):
            fila_inicial = i * TAM_BATCH
            fila_final = fila_inicial + TAM_BATCH
            lista_trabajos.append((fila_inicial, fila_final))
        return lista_trabajos

    def escuchar(self):
        """ MainLoop principal de la clase donde se debe volver despues de cada actividad concretada."""
        while len(self.solucion) < self.parser.cantidad_filas():
            self.respuesta = self.socket.recv_json()
            self.dirigir_entrantes()
            print(f'Hay {len(self.solucion)} soluciones en total'.center(50, '='))
        else:
            # Aca va que hacer cuando ya no se necesita escuchar mas la red, es decir
            # que se termino de solucionar el problema.
            print("Tarea Completada")

    def dirigir_entrantes(self):
        """ Con cada mensaje principal entrante lo despacha al metodo que corresponde para su procesamiento"""
        if self.respuesta['mens'] == 'disponible':
            # Hay un esclavo disponible para procesar, le enviamos trabajo y lo eliminamos de los pendientes
            trabajo_a_despachar = self.trabajos_pendientes.pop()
            self.enviar_trabajo(trabajo_a_despachar)
            print(f'Se entregó el trabajo de fila {trabajo_a_despachar[0]} a fila {trabajo_a_despachar[1]}.')
            print(f'Quedan {len(self.trabajos_pendientes) + 1} trabajos pendientes.')
        if self.respuesta['mens'] == 'solucion lista':
            self.recibir_solucion()

    def enviar_trabajo(self, indices):
        """ Envia todos los datos de un trabajo a un esclavo
        :keyword indices = tuple(indice_inicial, indice_final)"""
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
        self.socket.send_json({'mens': 'listo RX'})
        self.respuesta = self.socket.recv_json()
        solucion_parcial = []
        while self.respuesta['mens'] == 'solucion':
            dupla = self.respuesta['variable'], self.respuesta['solucion']
            solucion_parcial.append(dupla)
            self.socket.send_json({'mens': 'RX'})
            self.respuesta = self.socket.recv_json()
        else:
            self.socket.send_json({'mens': 'solucion RX'})
            print(f'Se recibieron {len(solucion_parcial)} soluciones')
            for item in solucion_parcial:
                self.solucion.append(item)

if __name__ == "__main__":
    servidor = Servidor()
    print("SERVIDOR INICIADO".center(50, '='))
    servidor.parser.cursor.execute('SELECT COUNT(*) FROM triplas;')
    print(f'Cantidad de triplas = {servidor.parser.cursor.fetchone()[0]}'.center(50, '='))
    servidor.parser.cursor.execute('SELECT COUNT(*) FROM resultados;')
    print(f'Cantidad de resultados (no nulos) = {servidor.parser.cursor.fetchone()[0]}'.center(50, '='))
    print("ARCHIVO DE DATOS ANALIZADO".center(50, '='))
    print(f'Cantidad de trabajos: {servidor.calcular_trabajos()}')
    print("Listo para escuchar".center(50, '='))
    servidor.escuchar()
    print("SOLUCION".center(50, '='))
    solucion_ordenada = sorted(servidor.solucion, key=lambda tup: tup[0])

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    x = np.arange(0, 40)
    y = np.arange(0, 40)
    x, y = np.meshgrid(x, y)
    z = np.array([item[1] for item in solucion_ordenada]).reshape((40, 40))
    z = np.rot90(z)
    ax.plot_surface(x, y, z, cmap=cm.coolwarm, linewidth=0, antialiased=False)
    plt.title(f'Gráfico de solución para tamaño de batch de {TAM_BATCH}')
    plt.show()
