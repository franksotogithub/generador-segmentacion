# -*- coding: utf-8 -*-
import math
import numpy as np
import random
import os
import shutil
import arcpy
import expresiones_consulta_arcpy as expresion
import  datetime
import  conection_sql as conx
import listado_urbano as listado
import subprocess


c = conx.Conexion()
cursor = c.cursor()



sql = """select  b.ubigeo,b.zona  from  dbo.MARCO_ZONA b
         where SUBSTRING(b.ubigeo,1,2) in('21') AND FASE='CPV2017' and flag_proc_segm=1
        order by b.ubigeo
    """

cursor.execute(sql)



for row in cursor:
    ubigeo=row[0]
    zona=row[1]

    print ubigeo,zona
    proceso = subprocess.Popen("python segmentacion_corregidos_zona.py {} {}  ".format(ubigeo, zona),
                               shell=True,
                               stderr=subprocess.PIPE)

    errores = proceso.stderr.read()
    errores_print = '{}'.format(errores)
    print errores_print

    #exportar_listado_urbano_distrito(ubigeo,fase)
c.close()





