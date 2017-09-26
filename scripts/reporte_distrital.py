# -*- coding: utf-8 -*-
import sys
import listado_urbano as listado
import conection_sql as conx
import math
import pyPdf


#ubigeo='{}'.format(sys.argv[1])
#fase ='{}'.format(sys.argv[3])


path_proyecto_segm = "D:/proyecto-segmentacion-urbana/"
path_ini = "D:/proyecto-segmentacion-urbana/segmentacion"
path_out = "D:"
path_croquis=path_out + "\\croquis"
path_listados=path_out + "\\listados"
path_croquis_listado=path_out + "\\croquis-listado"
path_etiquetas=path_out + "\\etiquetas"
path_out_final="\\\\192.168.201.115\\cpv2017"
path_urbano_croquis = path_out + "\\croquis\\urbano"
path_urbano_listados = path_out + "\\listados\\urbano"
#path_urbano_croquis_listado = path_out_final + "\\croquis-listado\\urbano\\{}".format(fase)
path_urbano_etiquetas = path_out_final+ "\\etiquetas\\urbano"
#path_urbano_croquis_listado = path_out_final + "\\croquis-listado\\urbano"

path_urbano_croquis_listado= path_out_final+"\\reportes-distritales-urbanos-nuevo"
#path_urbano_croquis_listado_2= path_out_final+"\\reportes-distritales-urbanos"



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
    list_dist_ope=listar_distrito_operativos(ubigeo,fase)

    print list_dist_ope

    for iddistope in list_dist_ope:

        #path_pdf = "{}\\{}\\{}\\{}.pdf".format(path_urbano_croquis_listado,fase,ubigeo,iddistope)
        path_pdf = "{}\\{}.pdf".format(path_urbano_croquis_listado, iddistope)
        distope=iddistope[6:]
        informacion = conx.ObtenerReporteDistrital(ubigeo,distope,fase)[:]
        #print informacion,path_pdf
        listado.ListadoDistrito(informacion, path_pdf)


        pdf = pyPdf.PdfFileReader(open(path_pdf, "rb"))
        cant_pag = pdf.getNumPages()
        list_web = path_pdf.split("\\")[3:]
        nom_web = ""

        for i in list_web:
            nom_web = nom_web + '/' + i

        nom_web = nom_web.replace("\\", "/")
        #conx.ActualizarCantidadPaginas(fase,iddistope, cant_pag, path_pdf, nom_web)

#exportar_listado_urbano_distrito('010701', 'CPV2017')

