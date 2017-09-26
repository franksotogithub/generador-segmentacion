import sys
import listado_urbano as listado
import conection_sql as conx
import math
import pyPdf

import pymssql
#server = "172.18.1.41"

path_proyecto_segm = "D:/proyecto-segmentacion-urbana/"
path_ini = "D:/proyecto-segmentacion-urbana/segmentacion"
path_out = "D:"
path_croquis=path_out + "\\croquis"
path_listados=path_out + "\\listados"
path_croquis_listado=path_out + "\\croquis-listado"
path_etiquetas=path_out + "\\etiquetas"
path_out_final="\\\\192.168.201.115\\cpv2017\\croquis-listado\\urbano"
path_urbano_croquis_listado= path_out_final+"\\REPORTES-DISTRITALES"




##############################obeteniedo reportes distritales
def ObtenerReporteDistrital(ubigeo,distope,fase):
    conn = conx.Conexion()
    cursor = conn.cursor()



    if distope!='00':
        sql_query_cabecera = """
                select a.ubigeo,a.ccdd,a.nombdep ,a.ccpp,a.nombprov,a.ccdi,a.nombdist,b.nombdistope
                from
                marco_distrito a
                inner join dbo.DISTRITO_OPE b on a.ubigeo=b.ubigeo
                where b.id='{}{}'  and  b.FASE='{}' """.format(ubigeo,distope,fase)

    else:
        sql_query_cabecera = """
            select a.ubigeo,a.ccdd,a.nombdep ,a.ccpp,a.nombprov,a.ccdi,a.nombdist,a.nombdist nombdistope
            from
            marco_distrito a
            where a.ubigeo='{}'  and  a.FASE='{}' """.format(ubigeo, fase)

    cursor.execute(sql_query_cabecera)
    cabecera = []



    for row in cursor:
        cabecera=row

    sql_query_data = """
    EXEC USP_REPORTE_DISTRITAL '{}','{}','{}'
    """.format(ubigeo,distope,fase)
    cursor.execute(sql_query_data)
    data=[]
    for row in cursor:
        data.append(row)

    sql_query_resumen = """exec USP_RESUMEN_DISTRITAL '{}','{}','{}'""".format(ubigeo,distope,fase)


    cursor.execute(sql_query_resumen)
    resumen = []
    for row in cursor:
        resumen=row

    conn.commit()
    conn.close()


    return [cabecera,data,resumen]


def listar_distrito_operativos(ubigeo,fase):
    conn=conx.Conexion()
    cursor = conn.cursor()
    sql="""select b.ubigeo,b.id from dbo.DISTRITO_OPE b
                where b.ubigeo='{}'  and  b.FASE='{}'  """.format(ubigeo,fase)


    cursor.execute(sql)
    list_dist_ope = []

    for row in cursor:
         list_dist_ope.append(row[1])
    return list_dist_ope
    conn.close()


def exportar_listado_urbano_distrito(ubigeo, fase):
    list_dist_ope = listar_distrito_operativos(ubigeo, fase)
    print list_dist_ope
    for iddistope in list_dist_ope:
        path_pdf = "{}\\{}.pdf".format(path_urbano_croquis_listado, iddistope)
        distope = iddistope[6:]
        informacion = ObtenerReporteDistrital(ubigeo, distope, fase)[:]
        listado.ListadoDistrito(informacion, path_pdf)



fase='CPV2017'
c = conx.Conexion()
cursor = c.cursor()


sql = """
    select distinct b.ubigeo from  dbo.REPORTE_DISTRITAL b
    where b.fase='{fase}' and ubigeo='150604' --and substring(ubigeo,1,2)   in ('01') --and substring(ubigeo,1,2)>13 
    order by b.ubigeo
    """.format(fase=fase)
cursor.execute(sql)

for row in cursor:
    ubigeo=row[0]
    print ubigeo
    exportar_listado_urbano_distrito(ubigeo,fase)
c.close()

