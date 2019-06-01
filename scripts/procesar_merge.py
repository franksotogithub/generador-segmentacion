import conection_sql as conx
import subprocess






def actualizar_zona(ubigeo,zona,flag,fase='CPV2017'):
    c=conx.Conexion()
    cursor=c.cursor()
    sql = """update b
        set b.flag_imp_total={flag}
        from dbo.MARCO_ZONA b
        where b.ubigeo='{ubigeo}' and b.zona='{zona}' and b.fase='{fase}'
    """.format(ubigeo=ubigeo, zona=zona,flag=flag,fase=fase)

    cursor.execute(sql)
    c.commit()
    c.close





def procesar_merge():


    for i in range(1000):
        list=conx.obtener_lista_zonas_merge(1)

        for el in list:
            ubigeo=el[0]
            zona = el[1]
            print ubigeo,zona
            proceso = subprocess.Popen("python D:\Dropbox\scripts\merge_pdfs.py {} {} ".format(ubigeo, zona), shell=True,
                                       stderr=subprocess.PIPE)

            errores = proceso.stderr.read()
            errores_print = '{}'.format(errores)
            print errores_print

            if len(errores_print) > 0 and(len(errores_print)>120):
                print 'algo salido mal'
                actualizar_zona(ubigeo,zona,0)

            else:
                print 'nada salio mal'
                actualizar_zona(ubigeo, zona, 1)

    #c.close()

procesar_merge()

