import zmq
import sqlite3


def esclavo():
    contexto = zmq.Context()
    socket = contexto.socket(zmq.REQ)
    socket.connect('tcp://localhost:5000')

    recibidos = []
    while True:
        # Decir que estamos disponible
        socket.send_json({"mens": "disponible"})
        respuesta = socket.recv_json()
        if respuesta['mens'] == 'trabajo':
            socket.send_json({'mens' : 'envie datos'})
            respuesta = socket.recv_json()
            while respuesta['mens'] == 'dato':
                tripla = (respuesta["fila"],respuesta["pos"],respuesta["valor"],respuesta["resul"])
                recibidos.append(tripla)
                print(f'Fila: {respuesta["fila"]}, '
                      f'Pos: {respuesta["pos"]}, '
                      f'Valor: {respuesta["valor"]}, '
                      f'Resultado: {respuesta["resul"]}')
                socket.send_json({'mens': 'RX'})
                respuesta = socket.recv_json()
                if respuesta['mens'] == 'dato':
                    continue
                elif respuesta['mens'] == 'fin fila':
                    socket.send_json({'mens' : 'fila RX'})
                    respuesta = socket.recv_json()
                    continue
                else:
                    socket.close()
                    break
        break
    return recibidos


if __name__ == "__main__":
        recibidos = esclavo()
        conexion = sqlite3.connect('datos_esclavo.db')
        cursor = conexion.cursor()
        cursor.execute("DROP TABLE IF EXISTS datos;")
        cursor.execute("CREATE TABLE datos(triplaId INTEGER PRIMARY KEY, "
                       "fila INTEGER, "
                       "pos_elemento INTEGER, "
                       "valor REAL, "
                       "resultado REAL);")
        for tupla in recibidos:
            cursor.execute("INSERT INTO datos(fila, "
                           "pos_elemento, "
                           "valor, "
                           "resultado) "
                           "VALUES (?, ?, ?, ?)",
                           tupla)
        conexion.commit()





