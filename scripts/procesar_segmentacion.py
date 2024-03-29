# -*- coding: utf-8 -*-
import subprocess
import conection_sql as conex
import sys
from datetime import *
import os
import socket
import  reporte_distrital
import generar_puntos_viviendas_incremento as g


path_proyecto="D:/proyecto-segmentacion-urbana"
path_segm= os.path.join(path_proyecto, 'segmentacion')
path_secciones=os.path.join(path_segm,'tb_secciones')
path_clips=os.path.join(path_segm,'tb_clips_frentes')


def crear_carpetas_segmentacion():

    list_paths=[path_proyecto,
        path_segm,
        path_secciones,
        path_clips
        ]

    for el in  list_paths:
        if os.path.exists(el) == False:
            os.mkdir(el)

    if os.path.exists(path_segm) == True:

        for archivo in os.listdir(path_segm):
            path_archivo=os.path.join(path_segm, archivo)
            try:
                os.remove(path_archivo)
            except:
                continue
crear_carpetas_segmentacion()
equipo=socket.gethostname()
fase='CPV2017'


for el in range(1):
    lista = conex.obtener_lista_zonas_segmentacion(cant_zonas=1,fase=fase)


    if len(lista)>0:
        for el in lista:
            print el
            ubigeo=el[0]
            zona=el[1]

            g.generar(data=[[ubigeo,zona,1]])
            proceso = subprocess.Popen("python d:\Dropbox\scripts\segmentacion_2.py {} {} {}".format(ubigeo, zona, fase), shell=True,
                                       stderr=subprocess.PIPE)
            errores = proceso.stderr.read()
            errores_print = '{}'.format(errores)
            print errores_print
            if len(errores_print) > 0:
                print 'algo salido mal'
                conex.actualizar_flag_proc_segm(ubigeo,zona,flag=2,equipo=equipo,fase=fase,error=errores_print)
            else:
                print 'nada salio mal'
                conex.actualizar_flag_proc_segm(ubigeo, zona, flag=1,equipo=equipo,fase=fase)
            estado_dist=conex.obtener_flag_segm_u_distrito(ubigeo=el[0],fase=fase)
            if estado_dist>0:
                conex.actualizar_monitoreo_segmentacion(ubigeo=ubigeo,fase=fase)

    else:
        break


