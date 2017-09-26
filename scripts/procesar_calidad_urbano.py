import subprocess
import conection_sql as conex
import socket
import os
#equipo=socket.gethostbyname(socket.gethostname())

path_proyecto="D:/proyecto-segmentacion-urbana"
path_calidad= os.path.join(path_proyecto,'calidad')



def crear_carpetas_calidad():
    if os.path.exists(path_proyecto) == False:
        os.mkdir(path_proyecto)


    if os.path.exists(path_calidad) == False:
        os.mkdir(path_calidad)
    else:
        for archivo in os.listdir(path_calidad):
            path_archivo=os.path.join(path_calidad,archivo)
            os.remove(path_archivo)

crear_carpetas_calidad()
equipo=socket.gethostname()


conex.actualizar_existencia_capas_por_zona()

for el in range(2700):
    lista = conex.obtener_lista_zonas_calidad(cant_zonas=1)

    if len(lista)>0:
        for el in lista:
            proceso = subprocess.Popen("python calidad_urbano.py {} {} ".format(el[0],el[1]), shell = True,stderr=subprocess.PIPE)
            #proceso.communicate()
            errores = proceso.stderr.read()
            errores_print = '{}'.format(errores)
            print errores_print
#
            if len(errores_print) > 0:
                print 'algo salido mal'
                conex.actualizar_flag_calidad_input_zonas(el[0], el[1], flag=2,equipo=equipo)
#
#
            else:
                print 'nada salio mal'
                conex.actualizar_flag_calidad_input_zonas(el[0], el[1], flag=1,equipo=equipo)

    else:
        break