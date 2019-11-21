import zmq

import analizador

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np
from datetime import datetime


class Lista_Trabajos(list):
    def __init__(self, tam_batch, cant_filas, veces):
        super().__init__()
        self.tam_batch = tam_batch
        self.cant_filas = cant_filas
        self.veces = veces
        self.generar_trabajos()
        self.sort(key=lambda trab: trab.orden)

    def generar_trabajos(self):
        trabajos_totales = self.cant_filas // self.tam_batch
        for i in range(trabajos_totales):
            fila_inicial = i * self.tam_batch
            fila_final = fila_inicial + self.tam_batch - 1
            self.append(Trabajo(extremos=(fila_inicial, fila_final)))
        cant_iniciales = len(self)
        for _ in range(self.veces - 1):
            for i in range(cant_iniciales):
                self.append(Trabajo(extremos=self[i].extremos))

    def get_trabajo_pendiente(self):
        for trabajo in self:
            if trabajo.estado == 'pendiente':
                trabajo.estado = 'listo'
                return trabajo
        else:
            return False


class Trabajo:
    static_orden = 0

    @staticmethod
    def incrementar_static_orden():
        Trabajo.static_orden += 1

    @staticmethod
    def get_static_orden():
        return Trabajo.static_orden

    def __init__(self, extremos, estado='pendiente'):
        self.extremos = extremos
        self.estado = estado
        self.orden = Trabajo.get_static_orden()
        Trabajo.incrementar_static_orden()

    def __getitem__(self, indice):
        return self.extremos[indice]

    def __eq__(self, other):
        try:
            assert isinstance(other, str)
            return self.estado == other
        except AssertionError:
            print("Solo se puede comparar con un estado valido")

    def __repr__(self):
        return f'{self.orden}. {self.extremos}:{self.estado}'




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
        self.lista_trabajos = Lista_Trabajos(self.tam_batch,
                                             self.parser.cantidad_filas(),
                                             self.veces)
        self.iniciar_comunicacion()
        self.mensaje_inicial()

    def mensaje_inicial(self):
        print("SERVIDOR INICIADO".center(50, '='))
        self.parser.cursor.execute('SELECT COUNT(*) FROM triplas;')
        print(f'Cantidad de triplas = {self.parser.cursor.fetchone()[0]}'.center(50, '='))
        self.parser.cursor.execute('SELECT COUNT(*) FROM resultados;')
        print(f'Cantidad de resultados (no nulos) = {self.parser.cursor.fetchone()[0]}'.center(50, '='))
        print("ARCHIVO DE DATOS ANALIZADO".center(50, '='))
        print(f'Cantidad de trabajos: {len(self.lista_trabajos)}'.center(50, '='))
        print("Listo para escuchar".center(50, '='))
        self.escuchar()

    def mostrar_solucion(self):
        print("SOLUCION".center(50, '='))
        self.graficar_solucion()

    def iniciar_comunicacion(self):
        """Asignar rol y numero al puerto"""
        self.socket = self.contexto.socket(zmq.REP)
        self.socket.bind('tcp://0.0.0.0:5000')


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
            self.veces -= 1

    def dirigir_entrantes(self):
        """ Con cada mensaje principal entrante lo despacha al metodo que corresponde para su procesamiento"""
        if self.respuesta['mens'] == 'disponible':
            print("ENVIANDO TRABAJO".center(50, '='))
            # Hay un esclavo disponible para procesar, le enviamos trabajo y lo eliminamos de los pendientes
            trabajo_a_despachar = self.lista_trabajos.get_trabajo_pendiente()
            if trabajo_a_despachar:
                self.enviar_trabajo(trabajo_a_despachar)
        if self.respuesta['mens'] == 'solucion lista':
            print("RECIBIENDO SOLUCION".center(50, '='))
            self.recibir_solucion()

    def enviar_trabajo(self, trabajo: Trabajo):
        """ Envia todos los datos de un trabajo a un esclavo
        :keyword trabajo = tuple(indice_inicial, indice_final)"""
        self.socket.send_json({'mens': 'trabajo'})
        primer_fila = trabajo[0]
        ultima_fila = trabajo[1]
        # Bucle de envio de filas
        for nro_fila in range(primer_fila, ultima_fila + 1):
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
                trabajo.estado = 'enviado'
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
        self.diferencia = None
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        dim = int(self.parser.cantidad_filas() ** 0.5)
        x = np.arange(0, dim)
        y = np.arange(0, dim)
        x, y = np.meshgrid(x, y)
        z = np.array([item[1] for item in self.solucion]).reshape((dim, dim))
        z = np.rot90(z)
        fecha = datetime.now()
        np.save(f'respuesta-{fecha}', z)
        self.diferencia = z
        ax.plot_surface(x, y, z, cmap=cm.coolwarm, linewidth=0, antialiased=False)
        plt.title(f'Gráfico de solución para tamaño de batch de {self.tam_batch}, {self.lista_trabajos.veces} veces')
        plt.show()


if __name__ == "__main__":
    app = Servidor(veces=1, tam_batch=1600, vector_result='./ejercicios/vector.txt', matriz='./ejercicios/matriz.txt')
    app.mostrar_solucion()
