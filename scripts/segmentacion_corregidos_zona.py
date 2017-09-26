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
import pyPdf
import sys
import subprocess

from datetime import *
from itertools import groupby





ubigeox='{}'.format(sys.argv[1])
zonax='{}'.format(sys.argv[2])
#aeux='000'



arcpy.env.overwriteOutput = True

fase='CPV2017'
#path_ini = "D:/Segmentacion"
path_proyecto_segm = "D:/proyecto-segmentacion-urbana/"

path_ini = "D:/proyecto-segmentacion-urbana/segmentacion-corregido"
path_secc="D:/proyecto-segmentacion-urbana/segmentacion/tb_secciones"
path_clips_frentes="D:/proyecto-segmentacion-urbana/segmentacion/tb_clips_frentes"
path_puntos_corte="D:/proyecto-segmentacion-urbana/segmentacion/tb_puntos_corte"

path_out = "D:"
path_croquis=path_out + "\\croquis"
path_listados=path_out + "\\listados"
path_croquis_listado=path_out + "\\croquis-listado"
path_etiquetas=path_out + "\\etiquetas"

path_out_final="\\\\192.168.201.115\\cpv2017"

path_urbano_croquis = path_out + "\\croquis\\urbano"
path_urbano_listados = path_out + "\\listados\\urbano"
path_urbano_croquis_listado = path_out_final + "\\croquis-listado\\urbano\\{}".format(fase)
path_urbano_etiquetas = path_out_final+ "\\etiquetas\\urbano"
path_proyecto="D:/Dropbox"
path_plantillas_croquis=path_proyecto+"/plantillas-croquis"
path_plantillas_layers=path_proyecto+"/plantillas-layers"
path_imagenes=path_proyecto+"/imagenes"
tb_rutas = path_ini + "/tb_rutas.dbf"
tb_subzonas = path_ini + "/tb_subzonas.dbf"
tb_rutas_lineas = path_ini + "/tb_rutas_lineas.shp"
tb_rutas_lineas_multifamiliar = path_ini + "/tb_rutas_lineas_multifamiliar.shp"
tb_viviendas = path_ini + "/tb_viviendas_inicial.shp"
tb_viviendas_ordenadas = path_ini + "/tb_vivienda.dbf"
tb_viviendas_ordenadas_temp = path_ini + "/tb_vivienda_temp.dbf"

tb_sitios_interes=path_ini + "/tb_sitios_interes.shp"
tb_aeus = path_ini + "/tb_aeus.dbf"
tb_manzanas = path_ini + "/tb_manzanas.shp"
tb_manzanas_ordenadas = path_ini + "/tb_manzanas_ordenadas.shp"
tb_manzanas_final=path_ini + "/tb_manzanas_final.shp"
tb_zonas = path_ini + "/tb_zonas.shp"
tb_zonas_dbf = path_ini + "/tb_zona.dbf"
tb_puntos_inicio = path_ini+"/tb_puntos_inicio.shp"
tb_frentes = path_ini+"/tb_frentes.shp"

tb_frentes_1 = path_ini+"/tb_frentes_1.shp"
tb_frentes_2 = path_ini+"/tb_frentes_2.shp"
tb_frentes_3 = path_ini+"/tb_frentes_3.shp"
tb_frentes_dissolve=path_ini+"/tb_frentes_dissolve.shp"
tb_frentes_puntos=path_ini+"/tb_frentes_puntos.shp"
tb_ejes_viales=path_ini+"/tb_ejes_viales.shp"
tb_mzs_condominios=path_ini+"/tb_mzs_condominios.dbf"
tb_viviendas_cortes = path_ini + "/tb_viviendas_cortes.shp"
tb_puertas_viv_multi=path_ini + "/tb_puertas_viv_multi.shp"
tb_rutas_puntos = path_ini + "/tb_rutas_puntos.shp"
tb_puntos_corte = path_ini + "/tb_puntos_corte.shp"
tb_rutas_puntos_min=path_ini + "/tb_rutas_puntos_min.shp"
tb_puntos_seleccionados_copy=path_ini + "/tb_puntos_seleccionados_copy.shp"
tb_edificios_copy=path_ini + "/tb_edificios_copy.shp"
tb_puntos_opciones=path_ini + "/tb_puntos_opciones.shp"
tb_multifamiliar_poligonos=path_ini+"/multifamiliar_poligonos.shp"
tb_vertice_final_manzana=path_ini+"/tb_vertice_final_manzana.shp"
tb_distrito_ope=path_ini+"/tb_distrito_ope.dbf"
tb_copia_multifamiliar=path_ini+"/tb_copia_multifamiliar.shp"

final_subzona=path_ini + "/final_tb_subzonas.dbf"
final_aeu=path_ini+'/final_tb_aeus.dbf'
final_seccion=path_ini+'/final_tb_secciones.shp'
final_sitios_interes= path_ini + '/final_tb_sitios_interes.shp'

error_1= path_ini + "/error_1_puerta_multifamiliar.shp"
error_2= path_ini + "/error_2_manzanas_sin_vias.shp"
error_3= path_ini + "/error_3_vias_dentro_manzana.shp"
error_4= path_ini + "/error_4_puntos_inicio_error.shp"
error_5= path_ini + "/error_5_viviendas_afuera_mz.shp"
error_6= path_ini + "/error_6_frentes_manzanas_cantidad.dbf"
error_7= path_ini + "/error_7_cant_frentes_dif.dbf"
error_8= path_ini + "/error_8_frentes_manzanas_forma.shp"
error_9= path_ini + "/error_9_enumeracion_viviendas_frentes.shp"
error_10= path_ini + "/error_10_viv_error_nombre_vias.shp"
error_11=path_ini + "/error_11_puertas_hijos_multi_en_frente_mz.shp"
error_12=path_ini + "/error_12_viv_hijos_sin_padre.shp"
error_13= path_ini + "/error_13_doble_puerta_multifamiliar.shp"


tb_viv_no_enlazadas=path_ini+"/tb_viv_no_enlazadas"


multifamiliar_id_padre = path_ini + "/multifamiliar_id_reg"
tb_rutas_preparacion = path_ini + "/tb_rutas_preparacion.shp"
tb_aeus_lineas = path_ini+"/segm_u_aeus_lineas.shp"
tb_aeus_puntos = path_ini+"/segm_u_aeus_puntos.shp"
#tb_secciones = path_ini + "/segm_u_seccion.shp"
tb_secciones = path_ini + "/tb_secciones.shp"
ip_server="172.18.1.93"
VIVIENDAS_AEU_OR_MAX = path_ini+"/VIVIENDAS_AEU_OR_MAX.shp"
VIVIENDAS_MZS_OR_MAX = path_ini+"/VIVIENDAS_MZS_OR_MAX.shp"

ip="us_arcgis_seg_2"
clave="b8an!hUse8P-"
db="CPV_SEGMENTACION"

techo_primera_pasada=16
techo_primera_pasada_gra_ciud=16
techo_primera_pasada_peq_ciud=16
techo_segunda_pasada_gra_ciud=0
techo_segunda_pasada_peq_ciud=0
techo_segunda_pasada=0
techo_manzana=18


arcpy.env.workspace = path_ini + ""
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)




def importar_tablas_corregidas(data,campos):
    arcpy.env.overwriteOutput = True
    if arcpy.Exists("CPV_SEGMENTACION2.sde") == False:
        arcpy.CreateDatabaseConnection_management("Database Connections",
                                                  "CPV_SEGMENTACION2.sde",
                                                  "SQL_SERVER",
                                                  ip_server,
                                                  "DATABASE_AUTH",
                                                  "us_arcgis_seg_2",
                                                  "MBs0p0rt301",
                                                  "#",
                                                  "CPV_SEGMENTACION",
                                                  "#",
                                                  "#",
                                                  "#",
                                                  "#")
    arcpy.env.workspace = "Database Connections/CPV_SEGMENTACION2.sde"
    path_conexion="Database Connections/CPV_SEGMENTACION2.sde"
    where_expression=expresion.Expresion(data, campos)


    temp_ubigeos = ""
    i=0
    for x in data:
        i=i+1
        if (i==1):
            temp_ubigeos="'{}'".format(x[0])
        else:
            temp_ubigeos = "{},'{}'".format(temp_ubigeos,x[0])


    print temp_ubigeos
    sql=expresion.Expresion(data, campos)
    manzanas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_MZS',"SELECT geom,UBIGEO,CODCCPP,ZONA,MANZANA,VIV_MZ,FALSO_COD,MZS_COND,CANT_BLOCK FROM TB_MANZANA WHERE UBIGEO IN ({}) ".format(temp_ubigeos))
    zonas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.VW_ZONA_CENSAL',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_ZONA_CENSAL WHERE {} ".format(sql))
    eje_vial_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.TB_EJE_VIAL',"SELECT * FROM CPV_SEGMENTACION.dbo.TB_EJE_VIAL where UBIGEO IN  ({}) ".format(temp_ubigeos))
    #viviendas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.VW_VIVIENDAS_U',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_VIVIENDAS_U WHERE {} ".format(sql))
    #frentes_mfl = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.TB_FRENTES',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_FRENTES WHERE {} ".format(sql))
    manzanas_mfl = arcpy.MakeFeatureLayer_management(manzanas_Layer,"manzanas_mfl")
    zonas_mfl=arcpy.MakeFeatureLayer_management(zonas_Layer, "zonas_mfl")
    eje_vial_mfl = arcpy.MakeFeatureLayer_management(eje_vial_Layer, "eje_vial_mfl")



    list_mfl=[[manzanas_mfl,tb_manzanas],
              #[frentes_mfl,tb_frentes]

              ]

    i=0
    for x in list_mfl:
        i=i+1
        temp= arcpy.CopyFeatures_management(x[0], 'in_memory/temp_{}'.format(i))
        arcpy.AddField_management(temp, 'MANZANA2', 'TEXT', 50)
        arcpy.CalculateField_management(temp, 'MANZANA2', '!MANZANA!', "PYTHON_9.3")
        arcpy.DeleteField_management(temp, ['MANZANA'])
        arcpy.CopyFeatures_management(temp, x[1])
        arcpy.AddField_management(x[1], 'MANZANA', 'TEXT', 50)
        arcpy.CalculateField_management(x[1], 'MANZANA', '!MANZANA2!', "PYTHON_9.3")
        arcpy.DeleteField_management(temp, ['MANZANA2'])

    arcpy.CopyFeatures_management(zonas_mfl, tb_zonas)
    arcpy.CopyFeatures_management(eje_vial_mfl, tb_ejes_viales)
    arcpy.TableToTable_conversion(path_conexion + '/CPV_SEGMENTACION.dbo.VW_MZS_CONDOMINIOS', path_ini,"tb_mzs_condominios.dbf")
    arcpy.env.workspace = path_ini+""
    arcpy.DeleteField_management(tb_manzanas, ['AEU','IDMANZANA'])
    arcpy.AddField_management(tb_manzanas, "IDMANZANA", "TEXT")
    expression = "(!UBIGEO!)+(!ZONA!)+(!MANZANA!)"
    arcpy.CalculateField_management(tb_manzanas, "IDMANZANA", expression, "PYTHON_9.3")
    arcpy.AddField_management(tb_manzanas, "AEU", "SHORT")
    arcpy.AddField_management(tb_manzanas, "AEU_2", "SHORT")
    arcpy.AddField_management(tb_manzanas, "FLG_MZ", "SHORT")
    arcpy.Dissolve_management(tb_frentes, tb_frentes_dissolve,['UBIGEO', 'ZONA', 'MANZANA', 'FRENTE_ORD'])


    database = "CPV_SEGMENTACION_GDB"
    if arcpy.Exists("{}.sde".format(database)) == False:
        arcpy.CreateDatabaseConnection_management("Database Connections",
                                                  "{}.sde".format(database),
                                                  "SQL_SERVER",
                                                  ip_server,
                                                  "DATABASE_AUTH",
                                                  "sde",
                                                  "$deDEs4Rr0lLo",
                                                  "#",
                                                  database,
                                                  "#",
                                                  "#",
                                                  "#",
                                                  "#")
    arcpy.env.workspace = "Database Connections/{}.sde".format(database)
    path_conexion2 = "Database Connections/{}.sde".format(database)

    list_capas=[
                ["{}.sde.SEGM_AEU".format(database),tb_aeus,2],
                ["{}.sde.SEGM_RUTA".format(database), tb_rutas, 2],

                ["{}.sde.SEGM_SECCION".format(database), tb_secciones, 1],
                ["{}.sde.SEGM_SITIOS_INTERES".format(database), tb_sitios_interes, 1],
                ["{}.sde.SEGM_SUBZONA".format(database), tb_subzonas, 2],
                ] ##capa orin, destino

    for i,capa in enumerate(list_capas):

        if capa[2]==1:
            #print 'aqui'

            #if capa[1]==tb_viviendas_ordenadas_temp:
            #    x = arcpy.MakeQueryLayer_management(path_conexion2, 'capa{}'.format(i),"select * from {} where {}".format(capa[0], sql))
#
            #else:
            x = arcpy.MakeQueryLayer_management(path_conexion2, 'capa{}'.format(i),"select * from {} where {} ".format(capa[0],sql))

        else:
            x = arcpy.MakeQueryTable_management(capa[0], "capa{}".format(i), "USE_KEY_FIELDS", "objectid", "", sql)


        if capa[1] in (tb_rutas,tb_rutas_puntos):
            #####tratamiento del campo manzana para las Ñ #####

            if capa[1] in (tb_rutas) :
                temp = arcpy.CopyRows_management(capa[0], 'in_memory/temp_m2{}'.format(i))
            else:
                temp = arcpy.CopyFeatures_management(capa[0], 'in_memory/temp_m{}'.format(i))


            arcpy.AddField_management(temp, 'MANZANA2', 'TEXT', 50)
            arcpy.CalculateField_management(temp, 'MANZANA2', '!MANZANA!', "PYTHON_9.3")
            arcpy.DeleteField_management(temp, ['MANZANA'])

            #####copiando archivos

            if capa[1] in (tb_rutas):
                print capa[1]
                temp = arcpy.CopyRows_management(temp, capa[1])
            else:
                temp = arcpy.CopyFeatures_management(temp, capa[1])

            arcpy.AddField_management(capa[1], 'MANZANA', 'TEXT', 50)
            arcpy.CalculateField_management(capa[1], 'MANZANA', '!MANZANA2!', "PYTHON_9.3")
            arcpy.DeleteField_management(capa[1], ['MANZANA2'])

            arcpy.AddField_management(capa[1], 'AEU2', 'TEXT', 3)
            arcpy.CalculateField_management(capa[1], 'AEU2', 'str(!AEU!).zfill(3)', "PYTHON_9.3")
            arcpy.DeleteField_management(capa[1], ['AEU'])
            arcpy.AddField_management(capa[1], 'AEU', 'TEXT', 3)
            arcpy.CalculateField_management(capa[1], 'AEU', '!AEU2!', "PYTHON_9.3")
            arcpy.DeleteField_management(capa[1], ['AEU2'])
        else:
            print capa[1]
            if capa[2] > 1:
                arcpy.CopyRows_management(x, capa[1])
            else:
                arcpy.CopyFeatures_management(x, capa[1])



def viviendas(data,campos):
    sql = expresion.Expresion(data, campos)

    conn = conx.Conexion2()
    cursor = conn.cursor()

    sql_query = """select  * from sde.VW_VIVIENDAS_U WHERE {} ORDER BY UBIGEO,ZONA,AEU,FALSO_COD,ID_REG_OR """.format(sql)

    cursor.execute(sql_query)

    list_viv = []
    for row in cursor:
        list_viv.append(row)


    arcpy.CreateTable_management(path_ini, 'tb_vivienda.dbf')


    list_fields = [['UBIGEO', 'TEXT'], ['ZONA', 'TEXT'], ['MANZANA', 'TEXT'], ['FRENTE_ORD', 'SHORT'] ,['ID_REG_OR', 'SHORT'],['AEU', 'TEXT'],
                   ['OR_VIV_AEU', 'SHORT'],['FASE','TEXT'],['P20','TEXT'],['P21','TEXT'],['P22_A','TEXT'],['P23','TEXT'],
                   ['P24','TEXT'],['P25','TEXT'],['P26','TEXT'],['P27_A','TEXT'],['P28','TEXT'],['P32','TEXT'],['FALSO_COD','SHORT']
                   ]
    for field in list_fields:
        arcpy.AddField_management(tb_viviendas_ordenadas, field[0], field[1])

    cursor_insert = arcpy.da.InsertCursor(tb_viviendas_ordenadas,
                                          ['UBIGEO', 'ZONA', 'MANZANA','FRENTE_ORD', 'ID_REG_OR', 'AEU','OR_VIV_AEU','FASE','P20','P21','P22_A','P23','P24','P25','P26','P27_A','P28','P32','FALSO_COD']
                                          )

    for x in list_viv:
        cursor_insert.insertRow(x)

    conn.close()

def obtener_datos_pdf(nom_pdf):

    pdf = pyPdf.PdfFileReader(open(nom_pdf, "rb"))
    cant_pag = pdf.getNumPages()
    list_web = nom_pdf.split("\\")[3:]
    nom_web = ""

    for i in list_web:
        nom_web = nom_web + '/' + i

    nom_web = nom_web.replace("\\", "/")
    print nom_web

    return [cant_pag,nom_web]


def ordenar_manzanas_cod_falso(where_expression):
    manzanas_selecc= arcpy.Select_analysis(tb_manzanas, "in_memory//manzanas_selecc", where_expression)
    manzanas_ordenadas=arcpy.Sort_management(manzanas_selecc, tb_manzanas_ordenadas, ["UBIGEO", "ZONA", "FALSO_COD"])
    expression = "flg_manzana(!VIV_MZ!)"

    arcpy.AddField_management(manzanas_ordenadas, "FLG_MZ", "SHORT")

def obtener_datos_pdf(nom_pdf):

    pdf = pyPdf.PdfFileReader(open(nom_pdf, "rb"))
    cant_pag = pdf.getNumPages()
    list_web = nom_pdf.split("\\")[3:]
    nom_web = ""

    for i in list_web:
        nom_web = nom_web + '/' + i

    nom_web = nom_web.replace("\\", "/")
    print nom_web

    return [cant_pag,nom_web]


def error_export_pdf(path_urbano_croquis, ubigeo, zona,codigo,out_croquis,mxd):
    error=1
    while (error == 1):
        error = pdf_to_png(dir=os.path.join(path_urbano_croquis, ubigeo, zona), archivo="{}.pdf".format(codigo))
        if (error == 1):
            print 'hay error'
            print codigo
            arcpy.mapping.ExportToPDF(mxd, out_croquis, data_frame="PAGE_LAYOUT", resolution=300)
        else:
            break


def pdf_to_png(dir, archivo):

    proceso = subprocess.Popen("convert -density 72 {} {}".format(os.path.join(dir, archivo),
                                                                  os.path.join(dir,
                                                                               '{}.png'.format(archivo.split(".")[0]))),
                               shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    errores = proceso.stderr.read()
    errores_print='{}'.format(errores)

    print errores_print
    if len(errores_print) > 0:
        return 1
    else:
        return 0






def exportar_croquis_urbano_zona_subzona(where_expression):
    arcpy.env.workspace =path_ini+""

    with arcpy.da.UpdateCursor(tb_subzonas,
                               ['UBIGEO', 'ZONA', 'SUBZONA', 'CODCCPP',  'CANT_VIV', 'CANT_PAG', 'COD_CROQ',
                                'RUTA_CROQ', 'RUTA_WEB'], where_expression) as cursor:

        for row in cursor:
            ubigeo=row[0]
            ccdd=ubigeo[0:2]
            ccpp = ubigeo[2:4]
            ccdi = ubigeo[4:6]
            zona=row[1]
            subzona=row[2]
            codccpp = row[3]
            cant_viv = row[4]
            #######################################Creamos los filtros de la zona############################################################
            where_zona = "UBIGEO='{}' AND ZONA='{}'".format(ubigeo,zona)

            if subzona>0:
                where_subzona    = "UBIGEO='{}' AND ZONA='{}' AND SUBZONA={}".format(ubigeo, zona,subzona)
                list_secciones=[[x[0],x[1],x[2]] for x in arcpy.da.SearchCursor(tb_secciones,['UBIGEO','ZONA','SECCION'],where_subzona)]
            else:
                where_subzona = "UBIGEO='{}' AND ZONA='{}'".format(ubigeo, zona)
                list_secciones = [[x[0], x[1], x[2]] for x in
                                  arcpy.da.SearchCursor(tb_secciones, ['UBIGEO', 'ZONA', 'SECCION'], where_zona)]


            where_secciones=expresion.Expresion_2(list_secciones, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["SECCION", "TEXT"]])

            list_aeus=[[x[0],x[1],x[2]] for x in arcpy.da.SearchCursor(tb_aeus,['UBIGEO','ZONA','AEU'],where_secciones)]
            where_aeus=expresion.Expresion_2(list_aeus, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["AEU", "TEXT"]])
            list_manzanas = [[x[0], x[1], x[2]] for x in arcpy.da.SearchCursor(tb_rutas, ['UBIGEO', 'ZONA', 'MANZANA'], where_aeus)]



            where_manzanas = expresion.Expresion_2(list_manzanas, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["MANZANA", "TEXT"]])

            for row4 in arcpy.da.SearchCursor(tb_zonas, ['DEPARTAMEN', 'PROVINCIA', 'DISTRITO', 'NOMCCPP'], where_zona):
                dep = row4[0]
                prov = row4[1]
                dist = row4[2]
                nomccpp = row4[3]
            #########################################Listamos los layers del mxd #####################################
            mxd = arcpy.mapping.MapDocument(path_plantillas_croquis + "/CroquisUrbanoZonaCorregido.mxd")
            df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
            zona_mfl = arcpy.mapping.ListLayers(mxd, "TB_ZONAS")[0]
            zona_mfl.definitionQuery=where_zona
            mzs_mfl = arcpy.mapping.ListLayers(mxd, "TB_MZS_ORD")[0]

            if subzona==0:
                mzs_mfl.definitionQuery =" MANZANA='0' "
            else:
                mzs_mfl.definitionQuery=where_manzanas

            list_manzanas_2 = list(set((x[0], x[1], x[2]) for x in arcpy.da.SearchCursor(tb_rutas, ['UBIGEO', 'ZONA', 'MANZANA'], where_aeus)))

            cant_secc = int(len(list_secciones))
            cant_mzs=int(len(list_manzanas_2))
            cant_aeus = int(len(list_aeus))
            seccion_inicial_temp = list_secciones[0][2]
            seccion_final_temp = list_secciones[-1][2]
            aeu_inicial_temp = list_aeus[0][2]
            aeu_final_temp = list_aeus[-1][2]
            mzs_inicial = list_manzanas[0][2]
            mzs_final = list_manzanas[-1][2]
            aeu_inicial = str(aeu_inicial_temp).zfill(3)
            aeu_final = str(aeu_final_temp).zfill(3)
            seccion_inicial = str(seccion_inicial_temp).zfill(3)
            seccion_final = str(seccion_final_temp).zfill(3)
            codigo = '{}{}{}'.format(ubigeo,zona,subzona)
            zona_etiqueta = expresion.EtiquetaZona(zona)
            frase = u'<BOL>OBSERVACIONES: </BOL>La Zona Nº {} se inicia en la manzana Nº {} y termina en la manzana Nº {}'.format(zona_etiqueta,mzs_inicial,mzs_final)
            #########################################Asignado los valores de las variables de los croquis ################################################
            list_text_el=[["COD_BARRA","*{}*".format(codigo)],["TEXT_COD_BARRA","*{}*".format(codigo)],["CCDD",ccdd],["CCPP",ccpp],["CCDI",ccdi],["CODCCPP",codccpp],
                          ["DEPARTAMENTO",dep],["PROVINCIA",prov],["DISTRITO",dist],["NOMCCPP",nomccpp],["ZONA",zona_etiqueta],["SUBZONA",subzona],["SECCION_INICIAL",seccion_inicial],
                          ["SECCION_FINAL",seccion_final],["AEU_INICIAL",aeu_inicial],["AEU_FINAL",aeu_final],["CANT_VIV",cant_viv],["FRASE",frase]]
            for text_el in list_text_el:
                el=arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", text_el[0])[0]
                el.text=text_el[1]
            #######################################Asignamos la escala del dibujo#################################################
            df.extent = zona_mfl.getSelectedExtent()

            ################################################Obtemos la informacion del listado####################################################
            cabecera=[ccdd,dep,ccpp,prov,ccdi,dist,codccpp,nomccpp,'CIUDAD',zona_etiqueta,subzona, u'DEL {} AL {}'.format(seccion_inicial, seccion_final),u'DEL {} AL {} '.format(aeu_inicial,aeu_final),cant_viv]
            data=[]
            for x in arcpy.da.SearchCursor(tb_aeus,['UBIGEO','ZONA','SECCION','AEU','CANT_VIV'],where_secciones):
                mzs=""
                i=0
                for y in  arcpy.da.SearchCursor(tb_rutas,['MANZANA'],"UBIGEO='{}' AND ZONA='{}' AND AEU='{}'".format(x[0],x[1],x[3])):
                    i=i+1
                    if (i==1):
                        mzs=u"{}".format(y[0])
                    else:
                        mzs = u"{}-{}".format(mzs,y[0])
                data.append(['{}'.format(x[2]).zfill(3),'{}'.format(x[3]).zfill(3),mzs,x[4]  ]    )
            resumen=[cant_secc,cant_mzs,cant_aeus]
            informacion=[cabecera,data,resumen]
            ########################################Exportamos los croquis y listados####################################################
            out_croquis = '{}\\{}\\{}\\{}.pdf'.format(path_urbano_croquis,ubigeo,zona,codigo)
            out_listado = '{}\\{}\\{}\\{}.pdf'.format(path_urbano_listados, ubigeo, zona, codigo)
            out_final= '{}\\{}\\{}\\{}.pdf'.format(path_urbano_croquis_listado, ubigeo, zona, codigo)

            arcpy.mapping.ExportToPDF(mxd, out_croquis, "PAGE_LAYOUT")
            error_export_pdf(path_urbano_croquis, ubigeo, zona, codigo, out_croquis, mxd)
            listado.ListadoZona(informacion, out_listado)
            pdfDoc = arcpy.mapping.PDFDocumentCreate(out_final)
            pdfDoc.appendPages(out_croquis)
            pdfDoc.appendPages(out_listado)
            pdfDoc.saveAndClose()

            [cant_pag,nomb_web]=obtener_datos_pdf(out_final)

            row[5]=cant_pag
            row[6]=codigo
            row[7]=out_final
            row[8]=nomb_web
            cursor.updateRow(row)


            del mxd
            del df
            del pdfDoc



def crear_carpetas_exportacion(data,campos):
    arcpy.env.workspace = path_ini
    paths_ini = [

        path_croquis,
        path_listados,

    ]
    paths = [

        path_urbano_croquis,
        path_urbano_listados,



    ]

    for path in paths_ini:
        if os.path.exists(path) == False:
            os.mkdir(path)
    for path in paths:
        if os.path.exists(path) == False:
            os.mkdir(path)
    for el in data:
        for path in paths:
            if os.path.exists(path + "\\" + str(el[0])) == False:
                os.mkdir(path + "\\" + str(el[0]))

    where_expression = expresion.Expresion(data, campos)

    with arcpy.da.SearchCursor(tb_zonas, ['UBIGEO', 'ZONA'], where_expression) as cursor:
        for row in cursor:
            for path in paths:

                if os.path.exists(path + "\\" + str(row[0]) + "\\" + str(row[1])) == False:
                    os.mkdir(path + "\\" + str(row[0]) + "\\" + str(row[1]))

                else:
                    shutil.rmtree(path + "\\" + str(row[0]) + "\\" + str(row[1]))
                    os.mkdir(path + "\\" + str(row[0]) + "\\" + str(row[1]))


data=[[ubigeox,zonax]]
campos=['UBIGEO','ZONA']




where1 = expresion.Expresion(data=[[ubigeox,zonax]], campos=['UBIGEO','ZONA'])
importar_tablas_corregidas(data=data, campos=campos)
crear_carpetas_exportacion(data=data,campos=campos)
ordenar_manzanas_cod_falso(where_expression=where1)

exportar_croquis_urbano_zona_subzona(where_expression=where1)
