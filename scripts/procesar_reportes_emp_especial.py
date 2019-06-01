import subprocess
import conection_sql as conx

c = conx.Conexion()
cursor = c.cursor()

for i in range(1):
    fase='CPV2017'
    sql = """
            begin
	        select  ubigeo from CPV_SEGMENTACION.dbo.MARCO_DISTRITO
            where  fase='{fase}' and substring(ubigeo,1,2)>23 
            order by 1 
            end
            """.format(fase=fase)

    cursor.execute(sql)

    for row in cursor:
        ubigeo=row[0]
        print ubigeo
        proceso = subprocess.Popen("python d:/Dropbox/scripts/reporte_emp_especial.py {} ".format(ubigeo),
                                   shell=True, stderr=subprocess.PIPE)
        errores = proceso.stderr.read()
        errores_print = '{}'.format(errores)

