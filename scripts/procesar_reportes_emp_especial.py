import subprocess
import conection_sql as conx
import sys
from datetime import *
import os
import socket
import  reporte_distrital


c = conx.Conexion()
cursor = c.cursor()

fase='CPV2017'
sql = """
        begin
        select ubigeo from CPV_SEGMENTACION.dbo.MARCO_DISTRITO
        where  fase='{fase}' and substring(ubigeo,1,2)  > '14'
        order by 1
        end
        """.format(fase=fase)

cursor.execute(sql)

for row in cursor:
    ubigeo=row[0]
    proceso = subprocess.Popen("python d:/Dropbox/scripts/reporte_emp_especial.py {} ".format(ubigeo),
                               shell=True, stderr=subprocess.PIPE)
    errores = proceso.stderr.read()
    errores_print = '{}'.format(errores)


