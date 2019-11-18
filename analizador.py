import sqlite3
import csv


class AnalizaArchivos:
    """Clase que inicializa la base de datos"""

    def __init__(self):
        self.db = self.iniciar_conexion()
        self.cursor = self.db.cursor()
        self.inicializar_bd()

    def analiza_archivos(self, file, separador=' ', modo="matriz"):
        """Abre un archivo separado por comas válido y lo reduce en la base de datos SQLite"""
        with open(file) as archivo_csv:
            lector_csv = csv.reader(archivo_csv, delimiter=separador)
            if modo == "matriz":
                for numero_linea, linea in enumerate(lector_csv):
                    for pos_elemento, elemento in enumerate(linea):
                        try:
                            numero = float(elemento)
                            if numero != 0.0:
                                nueva_tupla = (numero_linea, pos_elemento, numero)
                                self.cursor.execute(
                                    "INSERT INTO triplas (fila, pos_elemento, valor)  VALUES (?, ?, ?);",
                                    nueva_tupla)
                        except(ValueError):
                            continue
                    self.db.commit()
            elif modo == "vector":
                for numero_linea, linea in enumerate(lector_csv):
                    for pos_elemento, elemento in enumerate(linea):
                        nueva_tupla = (numero_linea, float(elemento))
                        self.cursor.execute("INSERT INTO resultados (resultado_fila, resultado_valor)  VALUES (?, ?);",
                                            nueva_tupla)
                    self.db.commit()
            else:
                print("Modo no válido!")

    @staticmethod
    def iniciar_conexion():
        """Genera una conexion SQLite y la retorna"""
        conexion = sqlite3.connect('datos_coordinador.db')
        return conexion

    def inicializar_bd(self):
        """Inicializa las tablas necesarias para guardar el problema en la BD
        Si existen las elimina para facilitar las pruebas"""
        self.cursor.execute("DROP TABLE IF EXISTS triplas;")
        self.cursor.execute("DROP TABLE IF EXISTS resultados;")
        self.cursor.execute("CREATE TABLE resultados("
                            "resultadoId INTEGER PRIMARY KEY,"
                            "resultado_fila INTEGER,"
                            "resultado_valor REAL);")
        self.cursor.execute("CREATE TABLE triplas("
                            "triplaId INTEGER PRIMARY KEY,"
                            "fila INTEGER,"
                            "pos_elemento INTEGER,"
                            "valor REAL,"
                            "FOREIGN KEY(fila) REFERENCES resultados(resultado_fila));")
        self.db.commit()

    def cantidad_filas(self):
        """Devuelve la cantidad de filas del problema"""
        self.cursor.execute('SELECT COUNT(DISTINCT fila) FROM triplas;')
        return self.cursor.fetchone()[0]
# if __name__ == '__main__':
#     pass
