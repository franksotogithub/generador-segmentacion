
import subprocess
import conection_sql as conex
import sys
from datetime import *
import os
import socket
import  reporte_distrital




fase='CPV2017'
for el in range(100):
    lista = conex.obtener_lista_zonas_reproceso()
    
    if len(lista)>0:
        for el in lista:
            print el
            ubigeo=el[0]
            zona=el[1]
            tipo=1
            proceso = subprocess.Popen("python d:\Dropbox\scripts\segmentacion_corregidos_2.py {} {} {}".format(ubigeo, zona,tipo), shell=True,stderr=subprocess.PIPE)
            errores = proceso.stderr.read()
            errores_print = '{}'.format(errores)
            print errores_print

            if len(errores_print) > 0:
                print 'algo salido mal'
            else:
                print 'nada salido mal'
                conex.actualizar_flag_reproceso_segm(ubigeo=ubigeo, zona=zona)

    else:
        break

