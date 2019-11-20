import zmq

import analizador

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np
from pprint import pprint



class Servidor:
    def __init__(self, veces, vector_result, matriz, tam_batch):
        self.veces = veces
        self.tam_batch = tam_batch
        self.parser = analizador.AnalizaArchivos()
        self.parser.analiza_archivos(vector_result, modo='vector')
        self.parser.analiza_archivos(matriz, modo='matriz')
        self.contexto = zmq.Context()
        self.socket = None
        self.respuesta = None
        self.solucion = []
        self.lista_pendientes = self.calcular_trabajos()
        self.iniciar_comunicacion()
        self.mensaje_inicial()

    def mensaje_inicial(self):
        print("SERVIDOR INICIADO".center(50, '='))
        self.parser.cursor.execute('SELECT COUNT(*) FROM triplas;')
        print(f'Cantidad de triplas = {self.parser.cursor.fetchone()[0]}'.center(50, '='))
        self.parser.cursor.execute('SELECT COUNT(*) FROM resultados;')
        print(f'Cantidad de resultados (no nulos) = {self.parser.cursor.fetchone()[0]}'.center(50, '='))
        print("ARCHIVO DE DATOS ANALIZADO".center(50, '='))
        print(f'Cantidad de trabajos: {len(self.calcular_trabajos())}'.center(50, '='))
        print("Listo para escuchar".center(50, '='))
        self.escuchar()

    def mostrar_solucion(self):
        print("SOLUCION".center(50, '='))
        pprint(self.solucion, compact=True)
        self.graficar_solucion()

    def iniciar_comunicacion(self):
        """Asignar rol y numero al puerto"""
        self.socket = self.contexto.socket(zmq.REP)
        self.socket.bind('tcp://0.0.0.0:5000')

    def calcular_trabajos(self):
        """Genera una lista de duplas con los indices de inicio y fin de cada trabajo
        que se entregará a los esclavos"""
        trabajos_totales = self.parser.cantidad_filas() // self.tam_batch
        lista_trabajos = []
        for i in range(trabajos_totales):
            fila_inicial = i * self.tam_batch
            fila_final = fila_inicial + self.tam_batch
            lista_trabajos.append((fila_inicial, fila_final))
        return lista_trabajos

    def escuchar(self):
        """ MainLoop principal de la clase donde se debe volver despues de cada actividad concretada."""
        while self.veces > 0:
            while len(self.solucion) < self.parser.cantidad_filas():
                self.respuesta = self.socket.recv_json()
                self.dirigir_entrantes()
            else:
                # Aca va que hacer cuando ya no se necesita escuchar mas la red, es decir
                # que se termino de solucionar el problema.
                print("PASADA COMPLETADA".center(50, '!'))
                if self.veces > 1:
                    self.solucion.clear()
                    self.lista_pendientes = self.calcular_trabajos()
            self.veces -= 1

    def dirigir_entrantes(self):
        """ Con cada mensaje principal entrante lo despacha al metodo que corresponde para su procesamiento"""
        if self.respuesta['mens'] == 'disponible':
            print("ENVIANDO TRABAJO".center(50, '='))
            # Hay un esclavo disponible para procesar, le enviamos trabajo y lo eliminamos de los pendientes
            trabajo_a_despachar = self.lista_pendientes.pop()
            enviado = self.enviar_trabajo(trabajo_a_despachar)
            if not enviado:
                self.lista_pendientes.append(trabajo_a_despachar)
        if self.respuesta['mens'] == 'solucion lista':
            print("RECIBIENDO SOLUCION".center(50, '='))
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
            self.parser.cursor.execute("SELECT triplas.fila, "
                                       "triplas.pos_elemento, "
                                       "triplas.valor_coef, "
                                       "resultados.resultado_valor, "
                                       "semilla.valor_semilla "
                                       "FROM triplas "
                                       "LEFT JOIN resultados ON triplas.fila = resultados.resultado_fila "
                                       "LEFT JOIN semilla ON triplas.fila = semilla.variable "
                                       "WHERE fila = ? AND variable = ?;", (nro_fila, nro_fila))
            # Bucle de envio de datos
            for dato in self.parser.cursor.fetchall():
                respuesta = self.socket.recv_json()
                if respuesta['mens'] == 'envie datos' or respuesta['mens'] == 'RX' or respuesta[
                    'mens'] == 'fila RX':
                    self.socket.send_json({'mens': 'dato',
                                           'fila': dato[0],
                                           'pos': dato[1],
                                           'valor': dato[2],
                                           'resul': dato[3],
                                           'semil': dato[4]}
                                          )
            else:  # Cuando se temina el envio de datos mandamos el fin de fila
                respuesta = self.socket.recv_json()
                if respuesta['mens'] == 'RX':
                    self.socket.send_json({'mens': 'fin fila'})
        else:  # Cuando se termina el envio de filas mandamos fin de trabajo
            respuesta = self.socket.recv_json()
            if respuesta['mens'] == 'fila RX':
                self.socket.send_json({'mens': 'fin trabajo'})
                print("TRABAJO ENVIADO".center(50, '='))
                return True
        return False

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
            print(f'Se recibieron {len(solucion_parcial)} soluciones'.center(50, '='))
            self.solucion.extend(sorted(solucion_parcial, key=lambda tup: tup[0]))
        self.parser.actualizar_semilla(self.solucion)
        self.solucion.sort(key=lambda tup: tup[0])
        print("SOLUCION RECIBIDA".center(50, '='))

    def graficar_solucion(self):
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        x = np.arange(0, 40)
        y = np.arange(0, 40)
        x, y = np.meshgrid(x, y)
        z = np.array([item[1] for item in self.solucion]).reshape((40, 40))
        z = np.rot90(z)
        ax.plot_surface(x, y, z, cmap=cm.coolwarm, linewidth=0, antialiased=False)
        plt.title(f'Gráfico de solución para tamaño de batch de {self.tam_batch}')
        plt.show()



if __name__ == "__main__":
    app = Servidor(veces=3, tam_batch=400, vector_result='vector.txt', matriz='matriz.txt')
    app.mostrar_solucion()
