# -*- coding: utf-8 -*-
import math

from __builtin__ import list

import numpy as np
import random
import os
import shutil
import arcpy
import expresiones_consulta_arcpy as expresion
import  datetime
import  conection_sql as conx
import listado_urbano as listado
from datetime import *
import subprocess
import time
#import  EtiquetasUrbano as etiquetas
from itertools import groupby
import pyPdf
import sys
import string

#ubigeox='170101'
#zonax='00800'
#
#fase ='CPV2017'


ubigeox='{}'.format(sys.argv[1])
zonax='{}'.format(sys.argv[2])
fase ='{}'.format(sys.argv[3])
print sys.argv
#fase='CPV2017'

if len(sys.argv)>4:
    fase='{} {}'.format(sys.argv[3],sys.argv[4])

arcpy.env.overwriteOutput = True
print fase
#path_ini = "D:/Segmentacion"
path_proyecto_segm = "D:/proyecto-segmentacion-urbana/"

path_ini = "D:/proyecto-segmentacion-urbana/segmentacion"
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
tb_viviendas_ordenadas = path_ini + "/tb_vivienda.shp"
tb_viviendas_ordenadas_dbf= path_ini + "/tb_vivienda.dbf"
tb_sitios_interes=path_ini + "/tb_sitios_interes.dbf"
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

#techo_elegido_menores_segunda_pasada=16


arcpy.env.workspace = path_ini + ""

arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)

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

def CrearCarpetasSegmentacion():
    #arcpy.env.workspace = path_ini
    list_paths=[path_proyecto_segm,
        path_ini,



        ]

    for el in  list_paths:
        if os.path.exists(el) == False:
            os.mkdir(el)

def Nombre_ejes_viales():
    arcpy.AddField_management(tb_viviendas_ordenadas,'ID_FRENTE','TEXT')
    arcpy.AddField_management(tb_viviendas_ordenadas,'NOM_CAT_AL','TEXT')
    arcpy.AddField_management(tb_viviendas_ordenadas,'NOM_VIA_AL','TEXT')
    arcpy.AddField_management(tb_viviendas_ordenadas,'ERROR_VIA','SHORT')

    with arcpy.da.UpdateCursor(tb_viviendas_ordenadas, ["UBIGEO","ZONA","MANZANA","FRENTE_ORD","ID_FRENTE"]) as cursor:
        for x in cursor:
            x[4]=u'{}{}{}{}'.format(x[0],x[1],x[2],x[3])
            cursor.updateRow(x)


    #arcpy.CalculateField_management(tb_viviendas_ordenadas,'ID_FRENTE','!UBIGEO!+!ZONA!+!MANZANA!+str(!FRENTE_ORD!)','PYTHON_9.3')
    viviendas_mfl = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendas_mfl")
    mzs_line = arcpy.FeatureToLine_management(tb_manzanas_ordenadas, "in_memory/mzs_line")
    viviendas_selecc_frentes_mfl = arcpy.SelectLayerByLocation_management(viviendas_mfl, "INTERSECT", mzs_line)
    viviendas_selecc_frentes = arcpy.CopyFeatures_management(viviendas_selecc_frentes_mfl, "in_memory/viv_selecc_frentes")
    ejes_viales_buffer=arcpy.Buffer_analysis(tb_ejes_viales,"in_memory/ejes_viales_buffer","60 meters","","","LIST",["CAT_VIA","NOMBRE_CAT","NOMBRE_VIA","NOMBRE_ALT","CAT_NOM","UBIGEO"])
    intersect_viv_vias=arcpy.Intersect_analysis([viviendas_selecc_frentes,ejes_viales_buffer],"in_memory/intersect_viv_vias")
    list_id_frentes_validos=list(set([ u"{}{}{}{}".format(x[0],x[1],x[2],x[3]) for x in arcpy.da.SearchCursor(intersect_viv_vias,["UBIGEO","ZONA","MANZANA","FRENTE_ORD"],"P20=NOMBRE_CAT AND NOMBRE_VIA=P21")]))
    print list_id_frentes_validos


    where_expression_list=""
    with arcpy.da.UpdateCursor(tb_viviendas_ordenadas, ["ID_FRENTE","ERROR_VIA"]) as cursor:
        for x in cursor:
            if x[0] not in list_id_frentes_validos:
                x[1]=1

            cursor.updateRow(x)


    viviendas_no_enlazadas=arcpy.Select_analysis(tb_viviendas_ordenadas, error_10, "ERROR_VIA=1")
    viviendas_no_enlazadas_mfl=arcpy.MakeFeatureLayer_management(viviendas_no_enlazadas)


    viviendas_no_enlazadas_select=arcpy.SelectLayerByLocation_management(viviendas_no_enlazadas_mfl,"INTERSECT",mzs_line,'5 METERS',"NEW_SELECTION")
    lineas_viv_no_en=arcpy.PointsToLine_management(viviendas_no_enlazadas_select,path_ini+'/lineas_viv_no_en.shp','p21','ID_FRENTE')


    ejes_viales_buffer=arcpy.Buffer_analysis(tb_ejes_viales,"in_memory/ejes_viales_buffer","40 meters","","","LIST",["CAT_VIA","NOMBRE_CAT","NOMBRE_VIA","NOMBRE_ALT","CAT_NOM","UBIGEO"])
    where_eje="NOMBRE_VIA<>'{}'".format("SN")

    ejes_viales_buffer_select=arcpy.Select_analysis(ejes_viales_buffer,'in_memory/ejes_viales_buffer_select',where_eje)
    ejes_viales_buffer_select_mfl=arcpy.MakeFeatureLayer_management(ejes_viales_buffer_select)
    temp=arcpy.SpatialJoin_analysis(ejes_viales_buffer_select_mfl,lineas_viv_no_en,path_ini+'/temp.shp','JOIN_ONE_TO_MANY','','','CONTAINS')

    arcpy.AddField_management(temp,'AREA','DOUBLE')
    exp = "!SHAPE.AREA@METERS!"
    arcpy.CalculateField_management(temp,'AREA',exp,'PYTHON_9.3')
    temp_sort=arcpy.Sort_management(temp,'in_memory/temp_sort',[["JOIN_FID","ASCENDING"], ["AREA","ASCENDING"] ])
    temp_sort_select=arcpy.Select_analysis(temp_sort,path_ini+'/temp_sort_select.shp',"JOIN_FID<>-1")
    arcpy.DeleteIdentical_management(temp_sort_select,["JOIN_FID"])

    list_correcion_vias=list(set([(x[0],x[1],x[2]) for x in  arcpy.da.SearchCursor(temp_sort_select,["p21","NOMBRE_CAT","NOMBRE_VIA"])]))
    #list_correcion_vias_p21=list(set([x[0] for x in  arcpy.da.SearchCursor(temp_sort_select,["p21"])]))

    arcpy.AddField_management(error_10, 'NOM_CAT_AL', 'TEXT')
    arcpy.AddField_management(error_10, 'NOM_VIA_AL', 'TEXT')
    ##########################################################################################################
    with arcpy.da.UpdateCursor(error_10, ["UBIGEO", "ZONA", "MANZANA", "ID_REG_OR", "p21", "NOM_CAT_AL", "NOM_VIA_AL"]) as cursor:
        for x in cursor:
            for y in list_correcion_vias:
                if (y[0]==x[4]):
                    x[5]=y[1]
                    x[6]=y[2]
                    break
            cursor.updateRow(x)

def ImportarTablasTrabajo(data,campos):
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



    sql=expresion.Expresion(data, campos)
    manzanas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_MZS',"SELECT geom,UBIGEO,CODCCPP,ZONA,MANZANA,VIV_MZ,FALSO_COD,MZS_COND,CANT_BLOCK FROM TB_MANZANA WHERE UBIGEO IN ({}) ".format(temp_ubigeos))
    sitios_interes_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_SITIOS_INT',"SELECT * FROM TB_SITIO_INTERES WHERE UBIGEO IN ({}) AND (CODIGO<91 AND CODIGO<>26) ".format(temp_ubigeos))
    puntos_inicio_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_PUNTO_INICIO', "SELECT * FROM TB_PUNTO_INICIO WHERE {} ".format(sql))
    zonas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.VW_ZONA_CENSAL',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_ZONA_CENSAL WHERE {} ".format(sql))
    eje_vial_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.TB_EJE_VIAL',
                                                     "SELECT * FROM CPV_SEGMENTACION.dbo.TB_EJE_VIAL where UBIGEO IN  ({}) ".format(temp_ubigeos))
    viviendas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.VW_VIVIENDAS_U',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_VIVIENDAS_U WHERE {} ".format(sql))



    frentes_mfl = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.TB_FRENTES',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_FRENTES WHERE {} ".format(sql))



    manzanas_mfl = arcpy.MakeFeatureLayer_management(manzanas_Layer,"manzanas_mfl")
    sitios_interes_mfl=arcpy.MakeFeatureLayer_management(sitios_interes_Layer,"sitios_interes_mfl")
    puntos_inicio_mfl = arcpy.MakeFeatureLayer_management(puntos_inicio_Layer, "puntos_inicio_mfl")
    zonas_mfl=arcpy.MakeFeatureLayer_management(zonas_Layer, "zonas_mfl")
    eje_vial_mfl = arcpy.MakeFeatureLayer_management(eje_vial_Layer, "eje_vial_mfl")
    viviendas_mfl = arcpy.MakeFeatureLayer_management(viviendas_Layer, "viviendas_mfl")


    list_mfl=[[viviendas_mfl, tb_viviendas],[manzanas_mfl,tb_manzanas],[frentes_mfl,tb_frentes]]

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

    arcpy.CopyFeatures_management(puntos_inicio_mfl, tb_puntos_inicio)
    arcpy.CopyFeatures_management(zonas_mfl, tb_zonas)
    arcpy.CopyFeatures_management(eje_vial_mfl, tb_ejes_viales)
    arcpy.CopyFeatures_management(sitios_interes_mfl, tb_sitios_interes)
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

def OrdenarManzanasFalsoCod(where_expression):
    manzanas_selecc= arcpy.Select_analysis(tb_manzanas, "in_memory//manzanas_selecc", where_expression)
    manzanas_ordenadas=arcpy.Sort_management(manzanas_selecc, tb_manzanas_ordenadas, ["UBIGEO", "ZONA", "FALSO_COD"])
    expression = "flg_manzana(!VIV_MZ!)"
    #codeblock = """def flg_manzana(VIV_MZ):\n  if (VIV_MZ>{}):\n    return 1\n  else:\n    return 0""".format(techo_primera_pasada)

    arcpy.AddField_management(manzanas_ordenadas, "FLG_MZ", "SHORT")
    #arcpy.CalculateField_management(manzanas_ordenadas, "FLG_MZ", expression, "PYTHON_9.3", codeblock)

def CrearViviendasOrdenadas():

    arcpy.Sort_management(tb_viviendas, tb_viviendas_ordenadas, ["UBIGEO", "ZONA", "FALSO_COD", "ID_REG_OR"])
    arcpy.AddField_management(tb_viviendas_ordenadas, "AEU", "SHORT")
    arcpy.AddField_management(tb_viviendas_ordenadas, "OR_VIV_AEU", "SHORT")
    arcpy.AddField_management(tb_viviendas_ordenadas, "FLG_CORTE", "SHORT")
    arcpy.AddField_management(tb_viviendas_ordenadas, "FLG_MZ", "SHORT")

def crear_frentes_por_escala():
    arcpy.env.overwriteOutput = True
    list_frentes = [[x[0], x[1], x[2], int(x[3]),int(x[4])] for x in
                    arcpy.da.SearchCursor(tb_frentes, ['UBIGEO', 'ZONA', 'MANZANA', 'FRENTE_ORD','FALSO_COD'])]
    puntos = arcpy.FeatureToPoint_management(tb_frentes, 'in_memory/puntos', "INSIDE")

    list_frentes_capas=[tb_frentes_1,tb_frentes_2,tb_frentes_3]

    metros_ini=1.5
    for i,frente_capa in enumerate(list_frentes_capas):

        puntos_buffer = arcpy.Buffer_analysis(puntos, 'in_memory/buffer_puntos{}'.format(i), '{} meters'.format(metros_ini+i))
        frente_lyr = arcpy.MakeFeatureLayer_management(tb_frentes, 'frente_lyr')

        buffer_lyr = arcpy.MakeFeatureLayer_management(puntos_buffer, 'buffer_lyr{}'.format(i))

        list_cortes = []

        for x in list_frentes:
            frente_lyr = arcpy.SelectLayerByAttribute_management(frente_lyr, "NEW_SELECTION",
                                                             u"UBIGEO='{}' AND ZONA='{}' AND MANZANA='{}' AND FRENTE_ORD={}".format(
                                                                 x[0], x[1], x[2], x[3]))
            buffer_lyr = arcpy.SelectLayerByAttribute_management(buffer_lyr, "NEW_SELECTION",
                                                             u"UBIGEO='{}' AND ZONA='{}' AND MANZANA='{}' AND FRENTE_ORD={}".format(
                                                                 x[0], x[1], x[2], x[3]))
            clip = arcpy.Clip_analysis(frente_lyr, buffer_lyr, '{}\\clip{}{}{}{}.shp'.format(path_clips_frentes,x[0], x[1], x[4], x[3]))
            list_cortes.append(clip)

        arcpy.Merge_management(list_cortes, frente_capa)



def CrearSitiosInteresPorZonas():
    list_zonas =[ [x[0],x[1]] for x in arcpy.da.SearchCursor(tb_zonas,["UBIGEO","ZONA"])]
    tb_zonas_mfl=arcpy.MakeFeatureLayer_management(tb_zonas,'tb_zonas_mfl')

    for z in list_zonas:
        zona_selecc=arcpy.SelectLayerByAttribute_management(tb_zonas_mfl,'NEW_SELECTION',"UBIGEO='{}' AND ZONA='{}'".format(z[0],z[1]))
        sitio_interes_nuevo=arcpy.Clip_analysis(tb_sitios_interes,zona_selecc,'{}/sitios_interes{}{}.shp'.format(path_ini,z[0],z[1]))
        arcpy.AddField_management(sitio_interes_nuevo,'CODIGO2','TEXT')
        arcpy.CalculateField_management(sitio_interes_nuevo,'CODIGO2','!CODIGO!.zfill(2)','PYTHON_9.3')
        arcpy.DeleteField_management(sitio_interes_nuevo, 'CODIGO')
        arcpy.AddField_management(sitio_interes_nuevo, 'CODIGO', 'TEXT')

        arcpy.CalculateField_management(sitio_interes_nuevo, 'CODIGO', '!CODIGO2!','PYTHON_9.3')
        arcpy.DeleteField_management(sitio_interes_nuevo, 'CODIGO2')

        arcpy.AddField_management(sitio_interes_nuevo, 'ZONA', 'TEXT')
        arcpy.CalculateField_management(sitio_interes_nuevo, 'ZONA', '"{}"'.format(zonax), 'PYTHON_9.3')
        arcpy.CopyFeatures_management(sitio_interes_nuevo, final_sitios_interes)


def ConjuntosConexosCantViviendas(ubigeo,zona):
    arcpy.env.workspace = path_ini
    conjunto_conexo_cero_final=[]

    where="UBIGEO='{}' and ZONA='{}' AND VIV_MZ<={}".format(ubigeo,zona,techo_primera_pasada)
    list_manzanas = [(x[0],x[1],x[2],x[3],x[4]) for x in arcpy.da.SearchCursor(tb_manzanas_ordenadas, ["UBIGEO","ZONA","MANZANA","VIV_MZ","FALSO_COD"],where)]
    conjuntos_conexos_ceros=[]
    conjunto_conexo=[]
    cant_viv_acu = 0
    cod_falso_aux=0


    for el in list_manzanas:

        cod_falso = int(el[4])
        if (cod_falso_aux==cod_falso):
            #conjunto_conexo.append(el[0]+el[1]+el[2])
            conjunto_conexo.append(cod_falso)
            cant_viv_acu = cant_viv_acu + int(el[3])
            cod_falso_aux = cod_falso + 1
        else:
            if cant_viv_acu == 0:
                if len(conjunto_conexo)>0:
                    conjuntos_conexos_ceros.append(conjunto_conexo)
            conjunto_conexo=[]
            conjunto_conexo.append(cod_falso)
            cant_viv_acu = int(el[3])
            cod_falso_aux=cod_falso+1



    if len(conjunto_conexo) > 0 and cant_viv_acu==0:
        conjuntos_conexos_ceros.append(conjunto_conexo)

    where = "UBIGEO='{}' and ZONA='{}'".format(ubigeo,zona)
    list_falso_cod=[ x[0] for x in  arcpy.da.SearchCursor(tb_manzanas_ordenadas,["FALSO_COD"],where)]
    #print list_falso_cod
    max_falso_cod=max(list_falso_cod)
    #print max_falso_cod

    if len(conjuntos_conexos_ceros)>0:
        if max_falso_cod in conjuntos_conexos_ceros[-1]:
            conjunto_conexo_cero_final=conjuntos_conexos_ceros[-1][:]

    print conjuntos_conexos_ceros
    return conjunto_conexo_cero_final




def EnumerarAEUEnViviendasDeManzanas(where_expression):
    arcpy.env.overwriteOutput = True

    for row in arcpy.da.SearchCursor(tb_zonas, ["UBIGEO", "ZONA","CANT_VIV","CANT_MZS","ID_ESTRATO"], where_expression):
        cant_viv_zona=int(row[2])
        id_estrato=int(row[4])
        if id_estrato==1:
            techo_primera_pasada=techo_primera_pasada_gra_ciud
        else:
            techo_primera_pasada = techo_primera_pasada_peq_ciud

        print techo_primera_pasada
        if cant_viv_zona>0:
            conjunto_conexo_final_cero=ConjuntosConexosCantViviendas(ubigeo=row[0],zona=row[1])[:]
            where_expression1 = "UBIGEO='{}' and ZONA='{}'".format(str(row[0]),str(row[1]))

            numero_aeu = 1
            cant_vivi_agrupadas = 0
            anterior_manzana = 0
            cant_viv_anterior_manzana = 0
            or_viv_aeu = 1



            with arcpy.da.UpdateCursor(tb_manzanas_ordenadas,
                                       ["UBIGEO", "ZONA", "MANZANA", "FALSO_COD", "VIV_MZ", "MZS_COND", "AEU","FLG_MZ"],
                                       where_expression1) as cursor1:
                for row1 in cursor1:
                    cant_viv = int(row1[4])
                    if (cant_viv > techo_manzana):
                        where_expression_viv = " UBIGEO='{}' AND ZONA='{}' AND FALSO_COD={}".format(row1[0],row1[1],row1[3])
                        row1[7] =1

                        mzs_cond=0 ###ya no importa si la manzana es puro condominio
                        # Aqui se hace referencia a la manzana anterior
                        if anterior_manzana == 1:  # la anterior manzana es una menor o igual a 16 viviendas
                            #if cant_vivi_agrupadas != 0:  ###las manzanas anteriores tienen 0
                            numero_aeu = numero_aeu + 1
                            #else:
                            #    print cant_vivi_agrupadas

                        cant_vivi_agrupadas = 0
                        anterior_manzana = 2  ##si la manzana es con mas de 16 viv entonces 2
                        cant_viv_aeux=cant_viv
                        if (mzs_cond == 0):
                            #division = float(cant_viv) / 16.0

                            division = float(cant_viv) / techo_primera_pasada
                            cant_aeus = math.ceil(division)
                            residuo = cant_viv % cant_aeus
                            viv_aeu = cant_viv / cant_aeus
                            i = 0
                            or_viv_aeu = 1
                            edificacion_anterior = 0
                            numero_aeu_anterior = 0
                            idmanzana_anterior = ""

                            cant_aeus_aux = 0
                            id_reg_or_aux=0
                            with arcpy.da.UpdateCursor(tb_viviendas_ordenadas,
                                                       ["UBIGEO", "ZONA", "MANZANA", "ID_REG_OR", "AEU", "OR_VIV_AEU",
                                                        "P19A",
                                                        "P29", "FLG_MZ", "P23"], where_expression_viv) as cursor2:
                                for row2 in cursor2:

                                    row2[8] = 1
                                    id_reg_or=row2[3]
                                    flag=0

                                    idmanzana = u'{}{}{}'.format(row2[0], row2[1] ,row2[2])
                                    usolocal = int(row2[7])
                                    edificacion = int(row2[6])

                                    if (usolocal in [1, 3]):
                                        row2[4] = numero_aeu
                                        row2[5] = or_viv_aeu
                                        or_viv_aeu = or_viv_aeu + 1
                                        cant_viv_aeux=cant_viv_aeux-1

                                    elif (usolocal == 6):
                                        if (cant_viv_aeux>0):
                                            row2[4] = numero_aeu
                                        else:
                                            row2[4] = numero_aeu_anterior


                                    else:
                                        if or_viv_aeu != 1:
                                            row2[4] = numero_aeu
                                        else:
                                            if idmanzana == idmanzana_anterior:
                                                if edificacion == edificacion_anterior:
                                                    if flag==0:
                                                        row2[4] = numero_aeu_anterior
                                                    else:
                                                        row2[4] = numero_aeu
                                                elif (edificacion != edificacion_anterior and cant_aeus_aux == cant_aeus):
                                                    row2[4] = numero_aeu_anterior
                                                elif (edificacion == 1 and edificacion_anterior != 1):
                                                    row2[4] = numero_aeu_anterior
                                                else:
                                                    row2[4] = numero_aeu
                                            else:
                                                row2[4] = numero_aeu
                                                flag=1


                                    if residuo > 0:
                                        if or_viv_aeu > (viv_aeu + 1):
                                            i = 1
                                            edificacion_anterior = edificacion
                                            numero_aeu_anterior = numero_aeu
                                            idmanzana_anterior = idmanzana
                                            numero_aeu = numero_aeu + 1
                                            residuo = residuo - 1
                                            or_viv_aeu = 1
                                            cant_aeus_aux = cant_aeus_aux + 1

                                    else:
                                        if or_viv_aeu > (viv_aeu):
                                            edificacion_anterior = edificacion
                                            numero_aeu_anterior = numero_aeu
                                            numero_aeu = numero_aeu + 1
                                            idmanzana_anterior = idmanzana
                                            or_viv_aeu = 1
                                            cant_aeus_aux = cant_aeus_aux + 1


                                    id_reg_or_aux=id_reg_or
                                    cursor2.updateRow(row2)


                                del cursor2
                        else:

                            condominio_anterior = 0
                            numero_aeu_anterior = 0

                            if (cant_viv == 0):
                                cant_aeu_condominio = 1
                                viv_aeu_condominio = 0
                                res_viv_condominio = 0
                            else:
                                ##########cant aeu_condominio es la cantidad de aeus por block ########################
                                #cant_aeu_condominio = int(math.ceil(float(cant_viv) / 16.0))
                                cant_aeu_condominio = int(math.ceil(float(cant_viv) / techo_primera_pasada))
                                ##########viv_aeu_block es la cantidad de viviendas por block#####################
                                viv_aeu_condominio = int(cant_viv) / int(cant_aeu_condominio)
                                ##########res_viv_block es el residuo de viviendas por block######################
                                res_viv_condominio = int(cant_viv) % int(cant_aeu_condominio)
                            or_viv_aeu = 1
                            where_expression_viv_cond = " UBIGEO='{}' AND ZONA='{}' AND FALSO_COD={}".format(row1[0],
                                                                                                        row1[1],
                                                                                                        row1[3])

                            with arcpy.da.UpdateCursor(tb_viviendas_ordenadas,
                                                       ["UBIGEO", "ZONA", "MANZANA", "ID_REG_OR", "AEU", "OR_VIV_AEU",
                                                        "P19A",
                                                        "P29", "FLG_MZ", "P23"], where_expression_viv_cond) as cursor2:
                                for row2 in cursor2:
                                    # flg manzana en 1
                                    row2[8] = 1
                                    idmanzana =  u'{}{}{}'.format(row2[0], row2[1] ,row2[2])
                                    usolocal = int(row2[7])
                                    condominio = int(row2[6])
                                    if (usolocal in [1, 3]):
                                        row2[4] = numero_aeu
                                        row2[5] = or_viv_aeu
                                        or_viv_aeu = or_viv_aeu + 1
                                    elif (usolocal == 6):
                                        row2[4] = numero_aeu
                                    else:
                                        if or_viv_aeu != 1:
                                            row2[4] = numero_aeu
                                        else:
                                            if condominio == condominio_anterior:
                                                row2[4] = numero_aeu_anterior
                                            else:
                                                row2[4] = numero_aeu
                                    if res_viv_condominio > 0:
                                        if or_viv_aeu > (viv_aeu_condominio + 1):
                                            i = 1
                                            condominio_anterior = condominio
                                            numero_aeu_anterior = numero_aeu
                                            idmanzana_anterior = idmanzana
                                            numero_aeu = numero_aeu + 1
                                            res_viv_condominio = res_viv_condominio  - 1
                                            or_viv_aeu = 1
                                    else:
                                        if or_viv_aeu > (viv_aeu_condominio):
                                            condominio_anterior = condominio
                                            numero_aeu_anterior = numero_aeu
                                            numero_aeu = numero_aeu + 1
                                            idmanzana_anterior = idmanzana
                                            or_viv_aeu = 1
                                    cursor2.updateRow(row2)
                            del cursor2

                    else:
                        cant_vivi_agrupadas = cant_vivi_agrupadas + cant_viv
                        cod_falso=int(row1[3])
                        row1[7] =0
                        if (anterior_manzana == 2 or anterior_manzana == 0):  # si la manzana anterior es una manzana que tiene mas de 16 viviendas

                            cant_vivi_agrupadas = cant_viv  # cantidad  de viviendas del grupo regrewsa a 0 y se almacena la cantidad de viviendas
                            or_viv_aeu = 1
                            #if len(conjunto_conexo_final_cero)>0:
                            #    if cod_falso in conjunto_conexo_final_cero: ## si la manzana esta en el ultimo conjunto conexo
                            #        numero_aeu = numero_aeu - 1

                        else:
                            if cant_vivi_agrupadas <= techo_primera_pasada:
                                numero_aeu = numero_aeu

                            else:
                                cant_vivi_agrupadas = cant_viv
                                numero_aeu = numero_aeu + 1
                                or_viv_aeu = 1

                        anterior_manzana = 1  ##si la manzana es menor igual a 16 viv entonces la anterior manzana tiene valor 1

                        where_expression_viv = " UBIGEO='{}' AND ZONA='{}' AND FALSO_COD={}".format(row1[0], row1[1],
                                                                                                    row1[3])



                        with arcpy.da.UpdateCursor(tb_viviendas_ordenadas,
                                                   ["UBIGEO", "ZONA", "MANZANA", "ID_REG_OR", "AEU", "OR_VIV_AEU", "P19A",
                                                    "P29", "FLG_MZ", "P23"], where_expression_viv) as cursor2:
                            for row2 in cursor2:
                                row2[8] = 0
                                row2[4] = numero_aeu
                                usolocal = int(row2[7])
                                if (usolocal in [1, 3]):
                                    row2[5] = or_viv_aeu
                                    or_viv_aeu = or_viv_aeu + 1
                                cursor2.updateRow(row2)
                        del cursor2

                    row1[6] = int(numero_aeu)

                    cursor1.updateRow(row1)
        else:
            cant_manzanas =int(row[3])
            if (cant_manzanas > 10):
                cant_aeus = int(math.ceil(float(cant_manzanas) / 10))
                cant_mzs_por_aeu= cant_manzanas / cant_aeus
                resto = cant_manzanas % cant_aeus
            else:
                cant_aeus = 1
                cant_mzs_por_aeu = cant_manzanas
                resto = 0

            aeu = 1
            cant_mzs_aux = 0
            with arcpy.da.UpdateCursor(tb_manzanas_ordenadas, ['UBIGEO', 'ZONA','MANZANA','AEU'], "UBIGEO='{}' AND ZONA ='{}'".format(row[0],row[1]) ) as cursor:
                for x in cursor:
                    x[3]=aeu
                    cant_mzs_aux=cant_mzs_aux+1

                    if (resto > 0):
                        if ((cant_mzs_por_aeu + 1) == cant_mzs_aux):
                            resto = resto - 1
                            cant_mzs_aux = 0
                            aeu = aeu + 1
                    else:
                        if (cant_mzs_por_aeu == cant_mzs_aux):
                            cant_mzs_aux = 0
                            aeu = aeu + 1
                    cursor.updateRow(x)

            dict_aeus_mzs= dict((u'{}{}{}'.format(x[0],x[1],x[2]),int(x[3])  ) for x in arcpy.da.SearchCursor(tb_manzanas_ordenadas,['UBIGEO','ZONA','FALSO_COD','AEU']) )

            with arcpy.da.UpdateCursor(tb_viviendas_ordenadas, ['UBIGEO', 'ZONA','FALSO_COD','AEU'], "UBIGEO='{}' AND ZONA ='{}'".format(row[0],row[1]) ) as cursor:
                for x in cursor:
                    idmanzana=u"{}{}{}".format(x[0],x[1],x[2])
                    aeu=dict_aeus_mzs[idmanzana]
                    x[3]=aeu
                    cursor.updateRow(x)


def agrupar_aeus_viv_cero():


    #arcpy.AddField_management(tb_rutas, "TECHO_S_P", "SHORT")
    resumen_zona = arcpy.Statistics_analysis(tb_aeus, 'in_memory/zonas_aeu', [["AEU", "MAX"]],
                                             ["UBIGEO", "ZONA"])
    zonas = [(x[0], x[1], x[2]) for x in arcpy.da.SearchCursor(resumen_zona, ["UBIGEO", "ZONA", "MAX_AEU"])]

    for zona in zonas:
        where_zona = " UBIGEO='{}' AND ZONA='{}'  ".format(zona[0], zona[1])

        id_estrato = [x[0] for x in arcpy.da.SearchCursor(tb_zonas, ['ID_ESTRATO'], where_zona)][0]

        cant_aeus_zona = int(zona[2])
        #if id_estrato == 1:
            #techo_segunda_pasada = techo_segunda_pasada_gra_ciud
        techo_primera_pasada=18
        #else:
            #techo_segunda_pasada = techo_segunda_pasada_peq_ciud
        #    techo_primera_pasada = techo_primera_pasada_peq_ciud

        where_aeus_elegidos = " UBIGEO='{}' AND ZONA='{}' AND CANT_VIV=0 ".format(zona[0], zona[1])
        where_aeus = " UBIGEO='{}' AND ZONA='{}' ".format(zona[0], zona[1])

        ############################diccionario de datos#############################


        dic_aeus_elegidos = dict((x[2], [x[3]]) for x in
                                 arcpy.da.SearchCursor(tb_aeus, ["UBIGEO", "ZONA", "AEU", "CANT_VIV"],
                                                       where_aeus_elegidos))


        print dic_aeus_elegidos

        aeus_eleg_l = dic_aeus_elegidos.keys()

        aeus_eleg = sorted(aeus_eleg_l)[:]

        dic_aeus = dict((x[2], [x[3]]) for x in
                        arcpy.da.SearchCursor(tb_aeus, ["UBIGEO", "ZONA", "AEU", "CANT_VIV"], where_aeus))

        aeus = dic_aeus.keys()

        aeus_temp = aeus[:]
        aeus_opciones = aeus[:]

        if len(aeus) > 1:
            if int(len(aeus_eleg) > 0):
                while int(len(aeus_eleg) > 0):
                    aeu_eleg = aeus_eleg[0]
                    aeus_eleg.remove(aeu_eleg)
                    datos_aeu_eleg = dic_aeus_elegidos[aeu_eleg]
                    aeu = int(aeu_eleg)
                    cant_viv = int(datos_aeu_eleg[0])

                    if (aeu > 1 and aeu < cant_aeus_zona):
                        aeu_op_anterior = aeu - 1
                        aeu_op_posterior = aeu + 1

                        temp_aeu_anterior = dic_aeus[aeu_op_anterior]
                        temp_aeu_posterior = dic_aeus[aeu_op_posterior]

                        cant_viv_anterior = int(temp_aeu_anterior[0]) + cant_viv
                        cant_viv_posterior = int(temp_aeu_posterior[0]) + cant_viv

                        if ((cant_viv_anterior <= techo_primera_pasada) and (
                            cant_viv_posterior <= techo_primera_pasada)):
                            if (cant_viv_anterior <= cant_viv_posterior):
                                if aeu_op_anterior in aeus_opciones:
                                    aeu_selec = aeu_op_anterior
                                    aeus_opciones.remove(aeu_selec)
                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)

                                elif aeu_op_posterior in aeus_opciones:
                                    aeu_selec = aeu_op_posterior
                                    aeus_opciones.remove(aeu_selec)
                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)
                                else:
                                    aeu_selec = -1
                            else:
                                if aeu_op_posterior in aeus_opciones:
                                    aeu_selec = aeu_op_posterior
                                    aeus_opciones.remove(aeu_selec)
                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)
                                elif aeu_op_anterior in aeus_opciones:
                                    aeu_selec = aeu_op_anterior
                                    aeus_opciones.remove(aeu_selec)
                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)
                                else:
                                    aeu_selec = -1
                        elif ((cant_viv_anterior <= techo_primera_pasada)):
                            if aeu_op_anterior in aeus_opciones:
                                aeu_selec = aeu_op_anterior
                                aeus_opciones.remove(aeu_selec)
                                if aeu_selec in aeus_eleg:
                                    aeus_eleg.remove(aeu_selec)
                            else:
                                aeu_selec = -1

                        elif ((cant_viv_posterior <= techo_primera_pasada)):
                            if aeu_op_posterior in aeus_opciones:
                                aeu_selec = aeu_op_posterior
                                aeus_opciones.remove(aeu_selec)
                                if aeu_selec in aeus_eleg:
                                    aeus_eleg.remove(aeu_selec)
                            else:
                                aeu_selec = -1
                        else:
                            aeu_selec = -1

                        if aeu_selec != -1:
                            aeus_temp[(aeus_temp.index(aeu))] = aeu_selec

                    elif (aeu == 1):
                        aeu_op_posterior = aeu + 1
                        temp_aeu_posterior = dic_aeus[aeu_op_posterior]
                        cant_viv_posterior = int(temp_aeu_posterior[0]) + cant_viv
                        if (cant_viv_posterior <= techo_primera_pasada):
                            if aeu_op_posterior in aeus_opciones:
                                aeu_selec = aeu_op_posterior
                                aeus_opciones.remove(aeu_selec)
                                aeus_temp[(aeus_temp.index(aeu))] = aeu_selec

                                if aeu_selec in aeus_eleg:
                                    aeus_eleg.remove(aeu_selec)


                    else:
                        aeu_op_anterior = aeu - 1
                        temp_aeu_anterior = dic_aeus[aeu_op_anterior]
                        cant_viv_anterior = int(temp_aeu_anterior[0]) + cant_viv
                        if (cant_viv_anterior <= techo_primera_pasada):
                            if aeu_op_anterior in aeus_opciones:
                                aeu_selec = aeu_op_anterior
                                aeus_opciones.remove(aeu_selec)
                                aeus_temp[(aeus_temp.index(aeu))] = aeu_selec
                                if aeu_selec in aeus_eleg:
                                    aeus_eleg.remove(aeu_selec)


        print aeus_temp

        aeus_renumerados = []

        i = 0
        temp_anterior = 0

        for x in aeus_temp:
            if temp_anterior != x:
                i = i + 1
            aeus_renumerados.append(i)
            temp_anterior = x

        with arcpy.da.UpdateCursor(tb_rutas, ["AEU"], where_aeus) as cursor:
            for x in cursor:
                indice = int(x[0]) - 1
                x[0] = aeus_renumerados[indice]
                cursor.updateRow(x)

        with arcpy.da.UpdateCursor(tb_viviendas_ordenadas, ["AEU"], where_aeus) as cursor:
            for x in cursor:
                indice = int(x[0]) - 1
                x[0] = aeus_renumerados[indice]
                cursor.updateRow(x)


        #####################reordenamiento de viviendas#####################################

        #where_viv=" UBIGEO='{}' AND ZONA='{}'  AND OR_VIV_AEU<>0 ".format(zona[0], zona[1])
#
        #list_viviendas_ordenadas = [[x[0], x[1], x[2], x[3], x[4], x[5]] for x in
        #                               arcpy.da.SearchCursor(tb_viviendas_ordenadas,
        #                                                     ["UBIGEO", "ZONA", "MANZANA", "ID_REG_OR", "AEU",
        #                                                      "OR_VIV_AEU"], where_viv)]
        #for aeu_renumerado in aeus_renumerados:
        #    orden = 1
        #    for el in list_viviendas_ordenadas:
        #        if (el[4]==aeu_renumerado):
        #            el[5]=orden
        #            orden=orden+1
#
        #dic_viviendas_ordenadas = dict((u'{}{}{}{}'.format(x[0], x[1], x[2], x[3]),  x[5]) for x in list_viviendas_ordenadas)
#
        #with arcpy.da.UpdateCursor(tb_viviendas_ordenadas, ["UBIGEO","ZONA","MANZANA","ID_REG_OR","OR_VIV_AEU"], where_viv) as cursor:
        #     for x in cursor:
        #         id=u'{}{}{}{}'.format(x[0],x[1],x[2],x[3])
        #         id_reg_or=dic_viviendas_ordenadas[id]
        #         x[4]=id_reg_or
        #         cursor.updateRow(x)

    arcpy.Statistics_analysis(tb_rutas, tb_aeus, [["CANT_VIV", "SUM"]],
                                          ["UBIGEO", "CODCCPP", "ZONA", "AEU"])


    arcpy.AddField_management(tb_aeus, "CANT_VIV", "SHORT")
    arcpy.AddField_management(tb_aeus, "CANT_PAG", "SHORT")

    arcpy.CalculateField_management(tb_aeus, "CANT_VIV",
                                "[SUM_CANT_V]", "VB", "")
    arcpy.DeleteField_management(tb_aeus, ["SUM_CANT_V"])


def CrearRutas(where_expression):
    arcpy.env.overwriteOutput = True
    tb_mzs_menores_16 = "in_memory//tb_mzs_menores_16"
    where = " FLG_MZ=1 AND P29<>6"
    arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendas_ordenadas_3", where)
    tb_rutas_1=arcpy.Statistics_analysis("viviendas_ordenadas_3","in_memory//tb_rutas_1", [["OR_VIV_AEU", "MAX"]],
                              ["UBIGEO", "CODCCPP", "ZONA", "MANZANA", "AEU", "FALSO_COD"])
    arcpy.AddField_management(tb_rutas_1, "CANT_VIV", "SHORT")
    expression = "!MAX_OR_VIV_AEU!"
    arcpy.CalculateField_management(tb_rutas_1, "CANT_VIV", expression, "PYTHON_9.3")
    arcpy.DeleteField_management(tb_rutas_1, ['FREQUENCY', 'MAX_OR_VIV_AEU'])

    ####creacion de  la vista de manzanas menores a 16
    print techo_primera_pasada
    where2 = " FLG_MZ=0"
    arcpy.MakeFeatureLayer_management(tb_manzanas_ordenadas, "mzs_menores_16", where2)

    arcpy.Statistics_analysis("mzs_menores_16", tb_mzs_menores_16, [["MANZANA", "COUNT"]],
                              ["UBIGEO", "CODCCPP", "ZONA", "MANZANA", "AEU", "FALSO_COD", "VIV_MZ"])
    arcpy.AddField_management(tb_mzs_menores_16, "CANT_VIV", "SHORT")
    expression = "!VIV_MZ!"
    arcpy.CalculateField_management(tb_mzs_menores_16, "CANT_VIV", expression, "PYTHON_9.3")
    arcpy.DeleteField_management(tb_mzs_menores_16, ['FID_', 'FREQUENCY', 'COUNT_MANZANA', 'VIV_MZ'])
    tb_rutas_2=arcpy.Merge_management([tb_rutas_1, tb_mzs_menores_16], "in_memory/tb_rutas_2")
    arcpy.Sort_management(tb_rutas_2, tb_rutas, ["UBIGEO", "CODCCPP", "ZONA", "FALSO_COD", "AEU"])
    arcpy.Delete_management(tb_rutas_1)
    arcpy.Delete_management(tb_rutas_2)

def CrearPuntosDeCorte2():
    arcpy.env.overwriteOutput = True
    VIVIENDAS_AEU_OR_MAX_Stadistics = "in_memory/VIVIENDAS_AEU_OR_MAX_Stadistics"
    VIVIENDAS_MZS_OR_MAX_Stadistics = "in_memory/VIVIENDAS_MZS_OR_MAX_Stadistics"
    where_expression_l = "FLG_MZ=1"

    arcpy.AddField_management(tb_viviendas_ordenadas, "ID_VIV", "TEXT")
    arcpy.CalculateField_management(tb_viviendas_ordenadas, "ID_VIV", "!UBIGEO!+!ZONA!+!MANZANA!+str(!ID_REG_OR!)",
                                    "PYTHON_9.3")

    arcpy.AddField_management(tb_viviendas_ordenadas, "ID_AEU", "TEXT")
    arcpy.CalculateField_management(tb_viviendas_ordenadas, "ID_AEU", "!UBIGEO!+!ZONA!+!MANZANA!+str(!AEU!)",
                                    "PYTHON_9.3")
    arcpy.AddField_management(tb_viviendas_ordenadas, "FLAG_CORTE", "SHORT")

    viviendas_mfl0 = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendasMFL0")



    manzanas_lineas = arcpy.FeatureToLine_management(tb_manzanas, "in_memory/tb_manzanas_lineas", "", "ATTRIBUTES")

    manzanas_lineas_mfl = arcpy.MakeFeatureLayer_management(manzanas_lineas, "manzanaslineasMFL")

    ###intersectar con las manzanas

    vivSeleccionadas = arcpy.SelectLayerByLocation_management(viviendas_mfl0, "INTERSECT", manzanas_lineas_mfl, "#","NEW_SELECTION")

    viviendas_mfl = arcpy.MakeFeatureLayer_management(vivSeleccionadas, "viviendasMFL",where_expression_l)

    #where_expression_l = "FLG_MZ=1 AND P29<>6"
    #arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendas_ordenadas", where_expression_l)

    ########################calculando el ID de viviendas###########################################
    arcpy.AddField_management(viviendas_mfl, "ID_VIV", "TEXT")
    arcpy.CalculateField_management(viviendas_mfl, "ID_VIV", "!UBIGEO!+!ZONA!+!MANZANA!+str(!ID_REG_OR!)",
                                    "PYTHON_9.3")

    arcpy.AddField_management(viviendas_mfl, "ID_AEU", "TEXT")
    arcpy.CalculateField_management(viviendas_mfl, "ID_AEU", "!UBIGEO!+!ZONA!+!MANZANA!+str(!AEU!)",
                                    "PYTHON_9.3")


    ##############Calculando los puntos maximos de cada AEU##########################################
    viviendas_aeu_or_max=arcpy.Statistics_analysis(viviendas_mfl, VIVIENDAS_AEU_OR_MAX_Stadistics, [["ID_REG_OR", "MAX"]],
                              ["UBIGEO", "ZONA", "MANZANA", "AEU"])
    viviendas_mzs_or_max=arcpy.Statistics_analysis(viviendas_mfl, VIVIENDAS_MZS_OR_MAX_Stadistics, [["ID_REG_OR", "MAX"]],
                              ["UBIGEO", "ZONA", "MANZANA"])


    aeus_max_manzana=arcpy.Statistics_analysis(tb_viviendas_ordenadas, "in_memory/aeus_max_manzana", [["AEU", "MAX"]],
                              ["UBIGEO", "ZONA", "MANZANA"])


    arcpy.AddField_management(viviendas_aeu_or_max, "ID_VIV", "TEXT")

    arcpy.CalculateField_management(viviendas_aeu_or_max, "ID_VIV",
                                    "!UBIGEO!+!ZONA!+!MANZANA!+str(int(!MAX_ID_REG_OR!))",
                                    "PYTHON_9.3")


    arcpy.AddField_management(viviendas_mzs_or_max, "ID_VIV", "TEXT")


    arcpy.CalculateField_management(viviendas_mzs_or_max, "ID_VIV",
                                    "!UBIGEO!+!ZONA!+!MANZANA!+str(int(!MAX_ID_REG_OR!))",
                                    "PYTHON_9.3")



    arcpy.AddField_management(aeus_max_manzana, "ID_AEU", "TEXT")


    arcpy.CalculateField_management(aeus_max_manzana, "ID_AEU",
                                    "!UBIGEO!+!ZONA!+!MANZANA!+str(int(!MAX_AEU!))",
                                    "PYTHON_9.3")

    list_id_viviendas_aeu_or_max = [x[0] for x  in arcpy.da.SearchCursor(viviendas_aeu_or_max,['ID_VIV'])]
    list_id_viviendas_mzs_or_max = [x[0] for x in arcpy.da.SearchCursor(viviendas_mzs_or_max, ['ID_VIV'])]
    list_id_aeus_max_manzana = [x[0] for x in arcpy.da.SearchCursor(aeus_max_manzana, ['ID_AEU'])]

    print list_id_viviendas_aeu_or_max
    print list_id_viviendas_mzs_or_max
    print list_id_aeus_max_manzana

    with arcpy.da.UpdateCursor(tb_viviendas_ordenadas, ["ID_VIV","ID_AEU","FLAG_CORTE"]) as cursor:
        for row in cursor:

            if (row[0] in list_id_viviendas_aeu_or_max):
                if (row[0] not in list_id_viviendas_mzs_or_max):
                    row[2] = 1
                else:
                    if (row[1] not in list_id_aeus_max_manzana):
                        row[2] = 1

            cursor.updateRow(row)

    arcpy.Select_analysis(tb_viviendas_ordenadas,tb_puntos_corte,"FLAG_CORTE=1")

def RenumerarViviendasMzsMenores16(where_expression):
    arcpy.env.overwriteOutput = True
    AEU = "in_memory/AEU"
    AEU_SELECC = "in_memory/AEU_SELECC"
    arcpy.Statistics_analysis(tb_rutas, AEU, [["MANZANA", "COUNT"]],
                              ["UBIGEO", "CODCCPP", "ZONA", "AEU"])
    where = " COUNT_MANZANA>1"
    arcpy.MakeTableView_management(AEU, AEU_SELECC, where)

    for row1 in arcpy.da.SearchCursor(AEU_SELECC, ["UBIGEO", "ZONA", "AEU"]):
        where_expression_viv = " UBIGEO=\'" + str(row1[0]) + "\'  AND  ZONA=\'" + str(
            row1[1]) + "\' AND AEU=" + str(row1[2])
        print where_expression_viv
        or_viv_aeu = 1
        with arcpy.da.UpdateCursor(tb_viviendas_ordenadas,
                                   ["UBIGEO", "ZONA", "MANZANA", "ID_REG_OR", "AEU", "OR_VIV_AEU",
                                    "P29"], where_expression_viv) as cursor2:
            for row2 in cursor2:

                usolocal = int(row2[6])



                if (usolocal in [1, 3]):
                    row2[5] = or_viv_aeu
                    or_viv_aeu = or_viv_aeu + 1

                cursor2.updateRow(row2)

def CrearRutasPuntos():
    arcpy.env.overwriteOutput = True

    spatial_reference = arcpy.Describe(tb_viviendas_ordenadas).spatialReference

    where = "P29=6"
    where3 = "P29<>6"
    where2 = "P29=1 OR P29=3"

    arcpy.Select_analysis(tb_viviendas_ordenadas, tb_puertas_viv_multi, where)
    tb_viv_multi=arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "tb_viv_multi",where3)
    tb_viv_multi2 =arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas,"tb_viv_multi2",where2)

    if arcpy.Exists(tb_rutas_puntos):
        arcpy.Delete_management(tb_rutas_puntos)

    if arcpy.Exists(tb_rutas_puntos) == False:
        arcpy.CreateFeatureclass_management(path_ini,
                                            "tb_rutas_puntos.shp",
                                            "POINT",
                                            "",
                                            "",
                                            "",
                                            spatial_reference)

    list_field = ["SHAPE@", "UBIGEO", "CODCCPP", "ZONA", "MANZANA", "AEU","ID_REG_OR","MAX_OR_VIV","TIPO"]
    list_addfield = [["UBIGEO", "TEXT"], ["CODCCPP", "TEXT"], ["ZONA", "TEXT"], ["MANZANA", "TEXT"], ["AEU", "SHORT"],["ID_REG_OR","SHORT"],
                     ["MAX_OR_VIV", "SHORT"],["TIPO","SHORT"]]

    for el in list_addfield:
        arcpy.AddField_management(tb_rutas_puntos, el[0], el[1])


    puertas_mfl=arcpy.MakeFeatureLayer_management(tb_puertas_viv_multi,"puertas_viviendas_multifamiliar")
    mzs_aeu_mtv=arcpy.MakeTableView_management(tb_rutas,"mzs_aeu_mtv")

    multifamiliar_id_reg=arcpy.Statistics_analysis(tb_viv_multi2, multifamiliar_id_padre,[["ID_REG_OR","MAX"],["OR_VIV_AEU","MAX"]],["UBIGEO", "CODCCPP", "ZONA", "MANZANA","AEU", "FRENTE_ORD","ID_REG_PAD"])

    aeus_max_por_frente = arcpy.Statistics_analysis(tb_viviendas_ordenadas, "in_memory/aeus_max_manzana",
                                                    [["AEU", "MAX"]],
                                                    ["UBIGEO", "ZONA", "MANZANA", "FRENTE_ORD"])

    list_aeus_max_por_frente = [u'{}{}{}{}{}'.format(x[0], x[1], x[2], x[3], int(x[4])) for x in
                            arcpy.da.SearchCursor(aeus_max_por_frente,
                                                  ["UBIGEO", "ZONA", "MANZANA", "FRENTE_ORD", "MAX_AEU"])]



    puertas_mfl0 = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "puertas_mfl0")
    manzanas_lineas = arcpy.FeatureToLine_management(tb_manzanas, "in_memory/tb_manzanas_lineas", "", "ATTRIBUTES")
    manzanas_lineas_mfl = arcpy.MakeFeatureLayer_management(manzanas_lineas, "manzanas_lineas_mfl")
    puertas_selecc = arcpy.SelectLayerByLocation_management(puertas_mfl0, "INTERSECT", manzanas_lineas_mfl, "#","NEW_SELECTION")
    puertas_selecc_mfl = arcpy.MakeFeatureLayer_management(puertas_selecc, "puertas_selecc_mfl")

    id_reg_or_max_aeu=arcpy.Statistics_analysis(puertas_selecc_mfl,'in_memory/id_reg_or_max_aeu',[["ID_REG_OR","MAX"]],["UBIGEO", "CODCCPP", "ZONA", "AEU"])

    dict_id_reg_or_max_aeu= dict(  ('{}{}{}'.format(x[0],x[1],x[2]),int(x[3])) for x in arcpy.da.SearchCursor(id_reg_or_max_aeu,['UBIGEO','ZONA','AEU','MAX_ID_REG_OR']))

    keys_id_reg_or_max_aeu=dict_id_reg_or_max_aeu.keys()

    with arcpy.da.InsertCursor(tb_rutas_puntos, list_field) as cursor_insert:
        for row in  arcpy.da.SearchCursor(multifamiliar_id_reg, ["UBIGEO", "CODCCPP", "ZONA", "MANZANA","AEU", "ID_REG_PAD","MAX_ID_REG_OR","MAX_OR_VIV_AEU","FRENTE_ORD"],"ID_REG_PAD>0 AND MAX_OR_VIV_AEU>0"):
            puertas_mfl_selec=arcpy.SelectLayerByAttribute_management(puertas_mfl,'NEW_SELECTION', u" UBIGEO='{}' AND ZONA='{}' AND MANZANA='{}' AND ID_REG={} AND FRENTE_ORD={} ".format(row[0],row[2],row[3],int(row[5]),int(row[8]) ))
            mzs_aeu_temp=arcpy.SelectLayerByAttribute_management(mzs_aeu_mtv,'NEW_SELECTION', u" UBIGEO='{}' AND ZONA='{}' AND MANZANA='{}' AND AEU={}".format(row[0],row[2],row[3],int(row[4])))
            cant_viv=0
            corte=0
            for row3 in arcpy.da.SearchCursor(mzs_aeu_temp, ["CANT_VIV"]):
                cant_viv= int(row3[0])
            for row2 in arcpy.da.SearchCursor(puertas_mfl_selec, ["SHAPE@XY","ID_REG",'FRENTE_ORD']):
                point = arcpy.Point(row2[0][0], row2[0][1])
                id_aeu_frente=u'{}{}{}{}{}'.format(row[0],row[2],row[3],row2[2],int(row[4]))
                y=u'{}{}{}'.format(row[0],row[2],int(row[4]))

                if cant_viv==int(row[7]):
                    if y in keys_id_reg_or_max_aeu:
                        id_reg_or_max_aeu=int(dict_id_reg_or_max_aeu[y])
                    else:
                        id_reg_or_max_aeu =0


                    if ( (id_reg_or_max_aeu<int(row[6])) and (id_aeu_frente not in list_aeus_max_por_frente)):
                        corte = 1

                rowArray = [point, row[0], row[1], row[2], row[3], int(row[4]),int(row[6]),int(row[7]),corte]
                cursor_insert.insertRow(rowArray)

    sort_fields = [["UBIGEO", "ASCENDING"], ["CODCCPP", "ASCENDING"], ["ZONA", "ASCENDING"],
                               ["MANZANA", "ASCENDING"],
                               ["AEU", "ASCENDING"], ["ID_REG_OR", "ASCENDING"]]

    tb_rutas_cortes=arcpy.Select_analysis(tb_rutas_puntos,"in_memory/tb_rutas_cortes"," TIPO=1")
    tb_rutas_min=arcpy.Sort_management(tb_rutas_cortes, tb_rutas_puntos_min, sort_fields)
    arcpy.DeleteIdentical_management(tb_rutas_min, ["Shape"])

def CrearPuntosDeCorte():
    arcpy.env.overwriteOutput = True
    arcpy.AddField_management(tb_viviendas_ordenadas, "ID_VIV", "TEXT")
    arcpy.AddField_management(tb_viviendas_ordenadas, "LLAVE_AEU", "TEXT")
    arcpy.CalculateField_management(tb_viviendas_ordenadas, "ID_VIV", "!UBIGEO!+!ZONA!+!MANZANA!+str(!ID_REG_OR!)", "PYTHON_9.3")
    arcpy.CalculateField_management(tb_viviendas_ordenadas, "LLAVE_AEU", "!UBIGEO!+!ZONA!+str(!AEU!)", "PYTHON_9.3")
    viviendas_mfl0 = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendasMFL0","FLG_MZ=1")
    mz_line=arcpy.FeatureToLine_management(tb_manzanas,'in_memory/mzs_line')

    #frentes_mfl = arcpy.MakeFeatureLayer_management(tb_frentes, "frentesMFL")
    mzs_line_mfl = arcpy.MakeFeatureLayer_management(mz_line, "mzs_line_mfl")


    vivSeleccionadas = arcpy.SelectLayerByLocation_management(viviendas_mfl0, "INTERSECT", mzs_line_mfl, "0.1 METERS",
                                                              "NEW_SELECTION")


    cant_viv_selecc=len(list(x[0]  for x in arcpy.da.SearchCursor(vivSeleccionadas, ["ID_VIV"])))



    viviendas_frente_mfl = arcpy.MakeFeatureLayer_management(vivSeleccionadas, "viviendasMFL")

    VivMax = []
    aeus_max_por_frente = arcpy.Statistics_analysis(tb_viviendas_ordenadas, "in_memory/aeus_max_manzana", [["AEU", "MAX"]],
                                                 ["UBIGEO", "ZONA", "MANZANA","FRENTE_ORD"])


    viviendas_id_max_por_frente = arcpy.Statistics_analysis(viviendas_frente_mfl, "in_memory/viviendas_id_max_por_frente",
                                                    [["ID_REG_OR", "MAX"]],
                                                    ["UBIGEO", "ZONA", "MANZANA", "FRENTE_ORD"])

    list_aeus_max_por_frente= dict(( u'{}{}{}{}'.format(x[0],x[1],x[2],x[3]),int(x[4]))  for x in arcpy.da.SearchCursor(aeus_max_por_frente, ["UBIGEO","ZONA" ,"MANZANA", "FRENTE_ORD","MAX_AEU"]))
    list_viviendas_id_max_por_frente=[u'{}{}{}{}'.format(x[0],x[1],x[2],int(x[4]))   for x in arcpy.da.SearchCursor(viviendas_id_max_por_frente, ["UBIGEO","ZONA" ,"MANZANA", "FRENTE_ORD","MAX_ID_REG_OR"])]


    #for el in  list_aeus_max_por_frente:
    #    #if el[:14]=='15012502500020':
    #    print el

    VivMax=list_viviendas_id_max_por_frente
    VivMax.sort()
    VerticesFinales = []
    '''
    Se itera por AEU y se determina su ultima vivienda, si esta vivienda se encuentra en la lista generada en el proceos anterior, quiere
    decir que dicha vivienda es la ultima vivienda del frente, por tanto el limite maximo del AEU se transporta hasta el vertice de la
    manzana. Adicionalmente a ello se agregan los puntos de cortes de aquellos AEU que su limite se encuentran en el punto intermedio de la
    arista de la manzana.
    '''
    #print list_aeus_max_por_frente
    #print VivMax
    #print viviendas_frente_mfl
    #viviendas_mfl0


    punto_nro=1
    intersect_nro=1
    for x in list(set([x[0] for x in arcpy.da.SearchCursor(viviendas_frente_mfl, ["LLAVE_AEU"])])):
        print x
        listVivAeu_frente = [[m[0], m[1], m[2], m[3], m[4], m[5],m[6],m[7]] for m in arcpy.da.SearchCursor(viviendas_frente_mfl,
                                                                                          ["ID_VIV", "UBIGEO","ZONA", "MANZANA",
                                                                                           "FRENTE_ORD", "ID_REG_OR",
                                                                                           "SHAPE@","AEU"],
                                                                                          "LLAVE_AEU = '{}'".format(x))]
        listVivAeu_frente.sort(key=lambda l: l[5], reverse=True)  #######SE OBTIENE  EL LISTADO DE VIVIENDAS SELECCIONADAS EN EL FRENTE POR AEU
        VivMaxAeu_frente = listVivAeu_frente[0]                   #######SE OBTIENE  EL ID_REG_OR MAXIMO POR CADA AEU(POSIBLE PUNTO DE CORTE)
        #############SE NUMERA LA CANTIDAD DE FRENTES DE LA MANZANA DONDE ESTA EL AEU
        print VivMaxAeu_frente[1],VivMaxAeu_frente[2], VivMaxAeu_frente[3]
        NumeroFrentes = max([m[0] for m in arcpy.da.SearchCursor(tb_frentes, ["FRENTE_ORD"],u"UBIGEO='{}' AND ZONA = '{}' AND MANZANA = '{}'".format(VivMaxAeu_frente[1],VivMaxAeu_frente[2], VivMaxAeu_frente[3]))])
        #################SE OBTIENE EL IDENTIFICADOR DEL FRENTE DONDE SE ENCUENTRA EL ULTIMO REGISTRO DE PUERTA DEL AEU ##################
        id_frente=u'{}{}{}{}'.format(VivMaxAeu_frente[1],VivMaxAeu_frente[2],VivMaxAeu_frente[3],VivMaxAeu_frente[4])
        aeu=int(VivMaxAeu_frente[7])
        #####################SE COMPRUEBA SI EL ULTIMO REGISTRO DE PUERTA EN EL FRENTE DEL AEU SE ENCUENTRA EL UNA LISTA DE REGISTRO DE PUERTA MAX DEL FRENTE
        #####################SI ESTA ES PORQUE ES EL ULTIMO REGISTRO DEL FRENTE Y ES POSIBLE QUE EL PUNTO DE CORTE SE ENCUENTRE EN LA INTERSECCION DE FRENTES

        if VivMaxAeu_frente[0] in VivMax:

            ##################SI EL AEU DE LA PUERTA MAXIMA DEL FRENTE ES EL AEU MAXIMO DEL FRENTE
            if int(list_aeus_max_por_frente[id_frente])==aeu:
                for n in arcpy.da.SearchCursor(tb_frentes, ["UBIGEO","ZONA", "MANZANA", "FRENTE_ORD", "SHAPE@"],
                                               u" UBIGEO='{}' AND ZONA = '{}' AND MANZANA = '{}' AND FRENTE_ORD = {}".format(
                                                   VivMaxAeu_frente[1],
                                                   VivMaxAeu_frente[2],
                                                   VivMaxAeu_frente[3],
                                                   VivMaxAeu_frente[4])):
                    ## Cuando la vivienda no se encuentra en ultimo frente de la manzana
                    if int(n[3]) != NumeroFrentes:
                        for p in arcpy.da.SearchCursor(tb_frentes, ["UBIGEO","ZONA", "MANZANA", "FRENTE_ORD", "SHAPE@"],
                                                       u"UBIGEO='{}' AND ZONA = '{}' AND MANZANA = '{}' AND FRENTE_ORD = {}".format(
                                                           VivMaxAeu_frente[1],VivMaxAeu_frente[2], VivMaxAeu_frente[3], VivMaxAeu_frente[4] + 1)):
                            point0 = arcpy.Intersect_analysis([n[4], p[4]], 'in_memory\\intersect',
                                                              "#",
                                                              "0.1 meters", "POINT")



                            arcpy.AddField_management(point0, "LLAVE_AEU", "TEXT")
                            #arcpy.AddField_management(point0, "VIV_MAX", "TEXT")
                            arcpy.AddField_management(point0, "MANZANA","TEXT")
                            arcpy.AddField_management(point0, "ID_REG_OR", "SHORT")


                            with arcpy.da.UpdateCursor(point0, ["LLAVE_AEU","MANZANA","ID_REG_OR"]) as cursorUC:
                                for row in cursorUC:
                                    row[0] = x
                                    row[1] = VivMaxAeu_frente[3]   ####insertamos manzana
                                    len_id_manzana=int(len(VivMaxAeu_frente[3])) +11
                                    row[2] = int(VivMaxAeu_frente[0][len_id_manzana:])
                                    cursorUC.updateRow(row)
                            del cursorUC


                            #point = arcpy.FeatureToPoint_management(point0,'in_memory\\viv{}.shp'.format(VivMaxAeu_frente[0]))
                            point = arcpy.FeatureToPoint_management(point0, 'in_memory\\viv{}'.format(punto_nro))
                            punto_nro+=1
                            VerticesFinales.append(point)
                    ## Cuando la vivienda se encuentra en ultimo frente de la manzana
                    else:
                        for p in arcpy.da.SearchCursor(tb_frentes, ["UBIGEO","ZONA", "MANZANA", "FRENTE_ORD", "SHAPE@"],
                                                       u" UBIGEO='{}' AND ZONA = '{}' AND MANZANA = '{}' AND FRENTE_ORD = 1".format(
                                                           VivMaxAeu_frente[1], VivMaxAeu_frente[2],VivMaxAeu_frente[3])):
                            point0 = arcpy.Intersect_analysis([n[4], p[4]], 'in_memory\intersect',
                                                              "#",
                                                              "0.1 meters", "POINT")
                            arcpy.AddField_management(point0, "LLAVE_AEU", "TEXT")
                            #arcpy.AddField_management(point0, "VIV_MAX", "TEXT")
                            arcpy.AddField_management(point0, "MANZANA","TEXT")
                            arcpy.AddField_management(point0, "ID_REG_OR", "SHORT")

                            #with arcpy.da.UpdateCursor(point0, ["LLAVE_AEU","VIV_MAX"]) as cursorUC:
                            #    for row in cursorUC:
                            #        row[0] = x
                            #        cursorUC.updateRow(row)

                            with arcpy.da.UpdateCursor(point0, ["LLAVE_AEU","MANZANA","ID_REG_OR"]) as cursorUC:
                                for row in cursorUC:
                                    row[0] = x
                                    row[1] = VivMaxAeu_frente[3]   ####insertamos manzana
                                    len_id_manzana=int(len(VivMaxAeu_frente[3])) +11
                                    row[2] = int(VivMaxAeu_frente[0][len_id_manzana:])

                                    cursorUC.updateRow(row)

                            del cursorUC
                            #point = arcpy.FeatureToPoint_management(point0,'in_memory\\viv{}.shp'.format(path_puntos_corte))

                            point = arcpy.FeatureToPoint_management(point0, 'in_memory\\viv{}'.format(punto_nro))
                            punto_nro+=1
                            VerticesFinales.append(point)
            else:
                for n in arcpy.da.SearchCursor(tb_frentes, ["UBIGEO", "ZONA", "MANZANA", "FRENTE_ORD", "SHAPE@"],
                                               u" UBIGEO='{}' AND ZONA = '{}' AND MANZANA = '{}' AND FRENTE_ORD = {}".format(
                                                   VivMaxAeu_frente[1],
                                                   VivMaxAeu_frente[2],
                                                   VivMaxAeu_frente[3],
                                                   VivMaxAeu_frente[4])):
                    if int(n[3]) != NumeroFrentes:
                        for p in arcpy.da.SearchCursor(tb_frentes, ["UBIGEO", "ZONA", "MANZANA", "FRENTE_ORD", "SHAPE@"],
                                                       u"UBIGEO='{}' AND ZONA = '{}' AND MANZANA = '{}' AND FRENTE_ORD = {}".format(
                                                           VivMaxAeu_frente[1], VivMaxAeu_frente[2], VivMaxAeu_frente[3],
                                                                   VivMaxAeu_frente[4] + 1)):
                            point0 = arcpy.Intersect_analysis([n[4], p[4]], 'in_memory\intersect',
                                                              "#",
                                                              "0.1 meters", "POINT")
                            arcpy.AddField_management(point0, "LLAVE_AEU", "TEXT")
                            #arcpy.AddField_management(point0, "VIV_MAX", "TEXT")
                            arcpy.AddField_management(point0, "MANZANA","TEXT")
                            arcpy.AddField_management(point0, "ID_REG_OR", "SHORT")



                            with arcpy.da.UpdateCursor(point0, ["LLAVE_AEU","MANZANA","ID_REG_OR"]) as cursorUC:
                                for row in cursorUC:
                                    row[0] =u'{}{}{}'.format(p[0],p[1], (list_aeus_max_por_frente[id_frente]))
                                    row[1] = VivMaxAeu_frente[3]   ####insertamos manzana
                                    len_id_manzana=int(len(VivMaxAeu_frente[3])) +11
                                    row[2]= int(VivMaxAeu_frente[0][len_id_manzana:])                              ####insertamos id_reg_or
                                    print "Actualizacion de punto vertice de manzana 1", row[0]
                                    cursorUC.updateRow(row)
                            #del cursorUC
                            #point = arcpy.FeatureToPoint_management(point0,'in_memory\\viv{}.shp'.format(VivMaxAeu_frente[0]))


                            point = arcpy.FeatureToPoint_management(point0, 'in_memory\\viv{}'.format(punto_nro))
                            punto_nro+=1
                            VerticesFinales.append(point)
                            ## Cuando la vivienda se encuentra en ultimo frente de la manzana

                    else:
                        for p in arcpy.da.SearchCursor(tb_frentes, ["UBIGEO", "ZONA", "MANZANA", "FRENTE_ORD", "SHAPE@"],
                                                       u" UBIGEO='{}' AND ZONA = '{}' AND MANZANA = '{}' AND FRENTE_ORD = 1".format(
                                                           VivMaxAeu_frente[1], VivMaxAeu_frente[2], VivMaxAeu_frente[3])):
                            point0 = arcpy.Intersect_analysis([n[4], p[4]], 'in_memory\\intersect',
                                                              "#",
                                                              "0.1 meters", "POINT")
                            arcpy.AddField_management(point0, "LLAVE_AEU", "TEXT")
                            #arcpy.AddField_management(point0, "VIV_MAX", "TEXT")
                            arcpy.AddField_management(point0, "MANZANA", "TEXT")
                            arcpy.AddField_management(point0, "ID_REG_OR", "SHORT")

                            with arcpy.da.UpdateCursor(point0, ["LLAVE_AEU","MANZANA","ID_REG_OR"]) as cursorUC:
                                for row in cursorUC:

                                    row[0] = u'{}{}{}'.format(p[0], p[1], (list_aeus_max_por_frente[id_frente]))
                                    row[1] = VivMaxAeu_frente[3]   ####insertamos manzana
                                    len_id_manzana=int(len(VivMaxAeu_frente[3])) +11
                                    row[2]= int(VivMaxAeu_frente[0][len_id_manzana:])
                                    print "Actualizacion de punto vertice de manzana 2",row[0]
                                    cursorUC.updateRow(row)

                            #point = arcpy.FeatureToPoint_management(point0,'in_memory\\viv{}.shp'.format(VivMaxAeu_frente[0]))

                            point = arcpy.FeatureToPoint_management(point0,'in_memory\\viv{}'.format(punto_nro))
                            punto_nro+=1
                            VerticesFinales.append(point)



                    #point = arcpy.FeatureToPoint_management(point0, 'in_memory' + '\\' + 'viv{}'.format(punto_nro))

                    #point = arcpy.CopyFeatures_management(VivMaxAeu_frente[6],'in_memory\\viv{}2.shp'.format(VivMaxAeu_frente[0]))
                    point = arcpy.CopyFeatures_management(VivMaxAeu_frente[6],'in_memory\\viv{}'.format(punto_nro))
                    punto_nro+=1

                    arcpy.AddField_management(point, "LLAVE_AEU", "TEXT")
                    #arcpy.AddField_management(point, "VIV_MAX", "TEXT")
                    arcpy.AddField_management(point, "MANZANA", "TEXT")
                    arcpy.AddField_management(point, "ID_REG_OR", "SHORT")

                    with arcpy.da.UpdateCursor(point, ["LLAVE_AEU","MANZANA","ID_REG_OR"]) as cursorUC:
                        for row in cursorUC:
                            row[0] = u'{}{}'.format(x[:11],aeu)
                            #row[1] = VivMaxAeu_frente[0]

                            row[1] = VivMaxAeu_frente[3]  ####insertamos manzana
                            len_id_manzana = int(len(VivMaxAeu_frente[3])) + 11
                            row[2] = int(VivMaxAeu_frente[0][len_id_manzana:])

                            print  "Actualizancion de punto final de aeu" ,row[0], " vivienda maxima" ,row[1]
                            cursorUC.updateRow(row)
                    VerticesFinales.append(point)


        else:



            #point = arcpy.CopyFeatures_management(VivMaxAeu_frente[6], "in_memory\\viv{}".format(VivMaxAeu_frente[0]))
            #point = arcpy.CopyFeatures_management(VivMaxAeu_frente[6], "in_memory\\viv{}.shp".format(VivMaxAeu_frente[0]))

            point = arcpy.CopyFeatures_management(VivMaxAeu_frente[6], 'in_memory\\viv{}'.format(punto_nro))
            #point = arcpy.FeatureToPoint_management(point, 'in_memory' + '\\' + 'viv{}'.format(punto_nro))
            punto_nro += 1
            arcpy.AddField_management(point, "LLAVE_AEU", "TEXT")
            arcpy.AddField_management(point, "MANZANA", "TEXT")
            arcpy.AddField_management(point, "ID_REG_OR", "SHORT")
            #arcpy.AddField_management(point, "VIV_MAX", "TEXT")

            #with arcpy.da.UpdateCursor(point, ["LLAVE_AEU","VIV_MAX"]) as cursorUC:
            with arcpy.da.UpdateCursor(point, ["LLAVE_AEU", "MANZANA","ID_REG_OR"]) as cursorUC:
                for row in cursorUC:
                    row[0] = x
                    row[1] = VivMaxAeu_frente[3]  ####insertamos manzana
                    len_id_manzana = int(len(VivMaxAeu_frente[3])) + 11
                    row[2] = int(VivMaxAeu_frente[0][len_id_manzana:])
                    #row[1] = VivMaxAeu_frente[0]
                    cursorUC.updateRow(row)
            del cursorUC
            VerticesFinales.append(point)



    #merge = arcpy.Merge_management(VerticesFinales, tb_puntos_corte)
    ruta_pre=os.path.join(path_proyecto_segm,'tb_pre_puntos_corte.shp')
    print  VerticesFinales
    pre_puntos_corte = arcpy.Merge_management(VerticesFinales, ruta_pre)



    #merge=arcpy.Sort_management(pre_puntos_corte,out_dataset=tb_puntos_corte,sort_field=['LLAVE_AEU','VIV_MAX'])
    merge = arcpy.Sort_management(pre_puntos_corte, out_dataset=tb_puntos_corte, sort_field=[['LLAVE_AEU','ASCENDING'],['MANZANA','ASCENDING'], ['ID_REG_OR','DESCENDING']])

    arcpy.DeleteIdentical_management(merge, ['LLAVE_AEU','MANZANA'])

    listaCamposMerge = [x.name for x in arcpy.ListFields(merge)]
    CamposNecesarios = ['FID', 'Shape', 'LLAVE_AEU','MANZANA','ID_REG_OR']
    for x in CamposNecesarios:
        listaCamposMerge.remove(x)

    if len(listaCamposMerge)>0:
        arcpy.DeleteField_management(merge, listaCamposMerge)

    arcpy.AddField_management(merge,"AEU","SHORT")
    arcpy.CalculateField_management(merge,"AEU","!LLAVE_AEU![11:]","PYTHON_9.3")

    if cant_viv_selecc == 0:
        with arcpy.da.UpdateCursor(tb_puntos_corte, ["FID"]) as cursor:
            for row in cursor:
                cursor.deleteRow()



def RelacionarVerticeFinalInicioConAEUMax():
    arcpy.env.overwriteOutput = True
    tb_viviendas_ordenadas = path_ini + "/AEU/EnumerarAEUViviendas/TB_VIVIENDAS_ORDENADAS.shp"
    tb_viviendas_mzs_max_aeu = path_ini + "/AEU/EnumerarAEUViviendas/TB_VIVIENDAS_MZS_MAX_AEU"
    TB_VERTICES_FINAL = path_ini + "/AEU/CrearRepresentacionAEU/TB_VERTICES_FINAL.shp"
    TB_VERTICES_FINAL_Layer = "TB_VERTICES_FINAL_Layer"
    TB_VERTICES_FINAL_AEU = path_ini + "/AEU/CrearRepresentacionAEU/TB_VERTICES_FINAL_AEU.shp"

    arcpy.MakeFeatureLayer_management(TB_VERTICES_FINAL, TB_VERTICES_FINAL_Layer)
    tb_viviendas_mzs_max_aeu=arcpy.Statistics_analysis(tb_viviendas_ordenadas, "in_memory/tb_viviendas_mzs_max_aeu", [["AEU", "MAX"],["ID_REG_OR","MAX"]],["UBIGEO", "ZONA", "MANZANA"])

    arcpy.AddField_management(TB_VERTICES_FINAL_Layer, "IDMANZANA", "TEXT")
    arcpy.CalculateField_management(TB_VERTICES_FINAL_Layer, "IDMANZANA", "!UBIGEO!+!ZONA!+!MANZANA!", "PYTHON_9.3")

    arcpy.AddField_management(tb_viviendas_mzs_max_aeu, "IDMANZANA", "TEXT")
    arcpy.CalculateField_management(tb_viviendas_mzs_max_aeu, "IDMANZANA", "!UBIGEO!+!ZONA!+!MANZANA!", "PYTHON_9.3")

    arcpy.AddJoin_management(TB_VERTICES_FINAL_Layer, "IDMANZANA", tb_viviendas_mzs_max_aeu, "IDMANZANA")

    arcpy.CopyFeatures_management(TB_VERTICES_FINAL_Layer, TB_VERTICES_FINAL_AEU)

    add_field = [["UBIGEO", "TEXT"], ["CODCCPP", "TEXT"], ["ZONA", "TEXT"], ["MANZANA", "TEXT"], ["AEU", "SHORT"],["ID_REG_OR","SHORT"]]

    for el in add_field:
        arcpy.AddField_management(TB_VERTICES_FINAL_AEU, el[0], el[1])

    calculate_field = [["UBIGEO", "!TB_VERTICE!"], ["ZONA", "!TB_VERTI_1!"], ["ZONA", "!TB_VERTI_2!"],
                       ["MANZANA", "!TB_VERTI_3!"], ["AEU", "!tb_vivie_6!"],["ID_REG_OR", "!tb_vivie_7!"]]

    for el in calculate_field:
        arcpy.CalculateField_management(TB_VERTICES_FINAL_AEU, el[0], el[1], "PYTHON_9.3")

    list_deletefield = ["TB_VERTICE", "tb_viviend"]
    for el in range(1, 7):
        list_deletefield.append("TB_VERTI_" + str(el))
    for el in range(1, 8):
        list_deletefield.append("tb_vivie_" + str(el))

    arcpy.DeleteField_management(TB_VERTICES_FINAL_AEU, list_deletefield)

def CrearRutasPreparacion(manzanas):
    puntos_inicio_buffer=arcpy.Buffer_analysis(tb_puntos_inicio, "in_memory/buffer_inicio", "0.3 Meters")
    lineas=arcpy.FeatureToLine_management(manzanas, "in_memory/tb_mzs_line", "", "ATTRIBUTES")
    erase_lineas = arcpy.Erase_analysis(lineas, puntos_inicio_buffer, 'in_memory/erase_lineas')
    split=arcpy.SplitLine_management(erase_lineas,"in_memory/split")
    dissolve=arcpy.Dissolve_management(split, "in_memory/dissolve", "UBIGEO;CODCCPP;ZONA;MANZANA;FLG_MZ", "", "MULTI_PART",
                              "DISSOLVE_LINES")
    '''OBTENIENDO LOS VERTICES FINALES DE CADA MANZANA'''
    arcpy.FeatureVerticesToPoints_management(dissolve,tb_vertice_final_manzana , "END")
    arcpy.AddField_management(tb_vertice_final_manzana,"AEU","SHORT")
    buffer = arcpy.Buffer_analysis(tb_puntos_corte, 'in_memory/buffer', '0.5 Meters')
    erase = arcpy.Erase_analysis(dissolve, buffer, 'in_memory/erase')
    if manzanas==tb_manzanas_final:
        buffer_lyr=arcpy.MakeFeatureLayer_management(buffer,'buffer_lyr')
        buffer_no_intersect=arcpy.SelectLayerByLocation_management(buffer_lyr,"INTERSECT",dissolve,'','NEW_SELECTION','INVERT')
        buffer_2=arcpy.Buffer_analysis(buffer_no_intersect,'in_memory/buffer_2','0.5 Meters')
        erase_2 = arcpy.Erase_analysis(erase, buffer_2, 'in_memory/erase_2')
        rutas_preparacion = arcpy.MultipartToSinglepart_management(erase_2, tb_rutas_preparacion)
    else:
        rutas_preparacion = arcpy.MultipartToSinglepart_management(erase, tb_rutas_preparacion)


    listaCampos = [x.name for x in arcpy.ListFields(rutas_preparacion)]

    CamposNecesarios = ['FID', 'Shape', 'UBIGEO','CODCCPP','ZONA','MANZANA']

    for x in CamposNecesarios:
        listaCampos.remove(x)

    arcpy.DeleteField_management(rutas_preparacion, listaCampos)

    '''PEGANDO LOS AEU  A LOS VERTICES FINALES'''

    stadistics_viviendas_mzs_max_aeu=arcpy.Statistics_analysis(tb_viviendas_ordenadas, "in_memory/stadistics_viviendas_mzs_max_aeu",[["AEU", "MAX"], ["ID_REG_OR", "MAX"]], ["UBIGEO", "ZONA", "MANZANA"])

    list_id_viv_mzs_max = [x[0] + x[1] + x[2] for x in arcpy.da.SearchCursor(stadistics_viviendas_mzs_max_aeu,["UBIGEO", "ZONA", "MANZANA"])]
    list_st_viv_mzs_max = [ (x[0]+x[1]+x[2],x[3])   for x in arcpy.da.SearchCursor(stadistics_viviendas_mzs_max_aeu, ["UBIGEO","ZONA","MANZANA","MAX_AEU"])]


    with arcpy.da.UpdateCursor(tb_vertice_final_manzana , ["UBIGEO", "ZONA", "MANZANA","AEU"]) as cursor:
        for c in cursor:
            id_manzana=(c[0]+c[1]+c[2])
            indice=0
            aeu=0
            if id_manzana in list_id_viv_mzs_max:
                indice=list_id_viv_mzs_max.index(id_manzana)
                aeu=list_st_viv_mzs_max[indice][1]

            c[3]=aeu

            cursor.updateRow(c)

def pendiente(x1, y1, x2, y2):
    return (y1 - y2) / (x1 - x2)

def CrearLineasMultifamiliar():
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(32718)


    mzs_line=arcpy.FeatureToLine_management(tb_manzanas_ordenadas,"in_memory/mzs_line")
    rutas_puntos_mfl=arcpy.MakeFeatureLayer_management(tb_rutas_puntos,"tb_rutas_puntos")
    rutas_puntos_mfl_selecc=arcpy.SelectLayerByLocation_management(rutas_puntos_mfl,"INTERSECT",mzs_line,'0.2 METERS',"NEW_SELECTION")




    fc_Edificios_copy = arcpy.CopyFeatures_management(rutas_puntos_mfl_selecc,tb_edificios_copy)
                                                      #path_ini + "\AEU\CrearRepresentacionAEU\edificios_copy.shp")
    fc_Edificios_mfl = arcpy.MakeFeatureLayer_management(fc_Edificios_copy, "edificios_mfl")

    fc_Edificios_mfl_buffer = arcpy.Buffer_analysis(fc_Edificios_mfl, 'in_memory/fc_Edificios_mfl_buffer', "0.5 METERS")
    fc_Edificios_mfl_buffer_line = arcpy.FeatureToLine_management(fc_Edificios_mfl_buffer,
                                                                  'in_memory/fc_Edificios_mfl_buffer_line')

    fc_manzanas_copy = arcpy.CopyFeatures_management(tb_manzanas_ordenadas, "in_memory/manzanas_copy")
    fc_manzanas_mfl = arcpy.MakeFeatureLayer_management(fc_manzanas_copy, "manzanas_mfl")

    fc_manzanas_line = arcpy.FeatureToLine_management(fc_manzanas_copy, 'in_memory/fc_manzanas_line')

    fc_intersects = arcpy.Intersect_analysis([fc_manzanas_line, fc_Edificios_mfl_buffer_line],
                                             'in_memory/fc_intersects', "ALL", None, "POINT")

    puntos_opciones = arcpy.CreateFeatureclass_management(path_ini,"puntos_opciones.shp", 'POINT', '#', '#', '#',
                                                          arcpy.SpatialReference(32718))

    arcpy.AddField_management(puntos_opciones, 'ORIG_ID', 'TEXT')
    arcpy.AddField_management(puntos_opciones, 'TIPO', 'SHORT')

    # fc_inicio_fin=arcpy.FeatureToPoint_management(fc_intersects,path_inicial+'in_memory/fc_inicio_fin')


    cursor = arcpy.da.InsertCursor(puntos_opciones, ['SHAPE', 'ORIG_ID', 'TIPO'])

    for m in arcpy.da.SearchCursor(fc_Edificios_mfl,["FID", "UBIGEO", "ZONA", "AEU", "ID_REG_OR", "SHAPE@X","SHAPE@Y", "MANZANA","TIPO"]):
        vertices_mfl = arcpy.MakeFeatureLayer_management(fc_intersects,
                                                         "V{}".format(str(m[0])),
                                                         " ORIG_FID = {}".format(m[0]))

        # print m[0],m[1],m[2],m[3],m[4],m[5],m[6]
        id =u"{}{}{}{}{}{}".format( m[1],m[2],m[7].zfill(4) , str(m[3]).zfill(3) ,m[8],m[0])
        #print id
        inicio_fin = []

        print m[1],m[2],m[3],m[4]
        for n in arcpy.da.SearchCursor(vertices_mfl, ["SHAPE@"]):
            for el in n[0]:
                inicio_fin.append(el.X)
                inicio_fin.append(el.Y)
                print el.X , el.Y

        inicio_fin.append(m[0])
        p = pendiente(inicio_fin[0], inicio_fin[1], inicio_fin[2], inicio_fin[3])
        x = m[5]
        y = m[6]

        d = 1

        p_inversa = 1 / p * -1

        y1 = y + d * p_inversa / math.sqrt(math.pow(p_inversa, 2) + 1)
        x1 = x + d / math.sqrt(math.pow(p_inversa, 2) + 1)

        y2 = y - d * p_inversa / math.sqrt(math.pow(p_inversa, 2) + 1)
        x2 = x - d / math.sqrt(math.pow(p_inversa, 2) + 1)

        punto1 = arcpy.Point(x1, y1)
        punto2 = arcpy.Point(x2, y2)
        cursor.insertRow((punto1, id, m[8]))
        cursor.insertRow((punto2, id, m[8]))

    del cursor


    arcpy.AddField_management(fc_Edificios_copy, 'ORIG_ID', 'TEXT')

    arcpy.CalculateField_management(fc_Edificios_copy, 'ORIG_ID',
                                    '!UBIGEO!+!ZONA!+(!MANZANA!).zfill(4)+str(!AEU!).zfill(3)+str(!TIPO!)+str(!FID!)',
                                    'PYTHON_9.3')
    puntos_opciones_mfl = arcpy.MakeFeatureLayer_management(puntos_opciones, "puntos_opciones_mfl")
    puntos_seleccionados_mfl = arcpy.SelectLayerByLocation_management(puntos_opciones_mfl, "INTERSECT",
                                                                      fc_manzanas_copy)
    puntos_seleccionados_copy = arcpy.CopyFeatures_management(puntos_seleccionados_mfl,tb_puntos_seleccionados_copy)

    arcpy.Append_management(puntos_seleccionados_copy, fc_Edificios_copy, "NO_TEST")

    puntos_selecc_para_lineas_mfl = arcpy.Select_analysis(fc_Edificios_copy,
                                                          "in_memory/puntos_selecc_para_lineas",
                                                          #path_ini + '\AEU\CrearRepresentacionAEU\puntos_selecc_para_lineas.shp',
                                                          " TIPO=0 OR TIPO=1")




    puntos_selecc_para_poligonos_mfl = arcpy.Select_analysis(fc_Edificios_copy,"in_memory/puntos_selecc_para_poligonos","TIPO=2")

    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)

    multifamiliar_lineas = arcpy.PointsToLine_management(puntos_selecc_para_lineas_mfl,tb_rutas_lineas_multifamiliar,"ORIG_ID")

    multifamiliar_lineas_temp=CambiarLongitudLineaMultifamiliar(multifamiliar_lineas,4.5)

    multifamiliar_lineas =arcpy.CopyFeatures_management(multifamiliar_lineas_temp,tb_rutas_lineas_multifamiliar)

    print 'creando lineas para los poligonos'
    multifamiliar_lineas_para_poligonos=arcpy.PointsToLine_management(puntos_selecc_para_poligonos_mfl,'in_memory/multifamiliar_lineas_para_poligonos',"ORIG_ID")


    multifamiliar_lineas_para_poligonos_temp = CambiarLongitudLineaMultifamiliar(multifamiliar_lineas_para_poligonos, 4.5)




    multifamiliar_poligonos= arcpy.Buffer_analysis(multifamiliar_lineas_para_poligonos_temp,
                                                           "in_memory/multifamiliar_poligonos",
                                                           "0.8 METERS",'','FLAT')




    #multifamiliar_poligonos_buffer = arcpy.Buffer_analysis(multifamiliar_poligonos,
    #                                                "in_memory/multifamiliar_poligonos_buffer",
    #                                                "0.2 METERS")

    ################limpiamos todos los poligonos que cruzan la geomatria de la manzana
    multifamiliar_lineas_para_poligonos_temp=validar_poligonos_multifamiliar(multifamiliar_poligonos, mzs_line,multifamiliar_lineas_para_poligonos_temp)
    print 'validar poligonos multifamiliar'

    multifamiliar_poligonos = arcpy.Buffer_analysis(multifamiliar_lineas_para_poligonos_temp,
                                                    "in_memory/multifamiliar_poligonos",
                                                    "0.8 METERS", '', 'FLAT')


    multifamiliar_poligonos_buffer = arcpy.Buffer_analysis(multifamiliar_poligonos,
                                                       "in_memory/multifamiliar_poligonos_buffer",
                                                       "0.2 METERS")





    #######################creando agujeros multifamiliares en las manzanas#################################

    fc_manzanas_final = arcpy.Erase_analysis(fc_manzanas_copy, multifamiliar_poligonos_buffer,tb_manzanas_final)

    list_campos=arcpy.ListFields(multifamiliar_poligonos_buffer)


    addFields = [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["MANZANA", "TEXT"], ["AEU", "SHORT"], ["TIPO", "SHORT"]]

    for el in addFields:
        arcpy.AddField_management(multifamiliar_lineas, el[0], el[1])

    calculateFields = [["UBIGEO", "!ORIG_ID![0:6]"],["ZONA", "!ORIG_ID![6:11]"], ["MANZANA", "!ORIG_ID![11:15]"],
                       ["AEU","int(!ORIG_ID![15:18])"], ["TIPO", "!ORIG_ID![18:19]"]]


    for el in calculateFields:
        arcpy.CalculateField_management(multifamiliar_lineas, el[0], el[1], "PYTHON_9.3")

    with arcpy.da.UpdateCursor(multifamiliar_lineas, [ "MANZANA"]) as cursor:
        for c in cursor:
            manzana = c[0]
            manzana_final=""
            x=manzana.strip(string.digits)
            if len(x)==0:
                manzana_final=manzana[1:]
            else:
                manzana_final=manzana

            c[0]=manzana_final

            cursor.updateRow(c)


##################aqui se validan si los poligonos de multifamiliar pasan el limite de manzana
def validar_poligonos_multifamiliar(multifamiliar_poligonos, mzs_line,mutifamiliar_lineas_para_poligonos):
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(32718)
    mzs_line_temp=arcpy.CopyFeatures_management(mzs_line,'in_memory/mzs_line_temp')
    multifamiliar_poligonos_temp=arcpy.CopyFeatures_management(multifamiliar_poligonos,'in_memory/multifamiliar_poligonos_temp')
    resultado_temp=arcpy.CopyFeatures_management(mutifamiliar_lineas_para_poligonos,'in_memory/resultado_temp')

    temp_intersect=arcpy.Intersect_analysis([mzs_line_temp,multifamiliar_poligonos_temp],"in_memory/{}".format('temp_intersect'), "ALL", "", "INPUT")

    #for el in arcpy.ListFields(temp_intersect):
    #    print el.name


    #for el in arcpy.da.SearchCursor(temp_intersect,'*'):
    #    print el


    x_temp=arcpy.Statistics_analysis(temp_intersect,'in_memory/x_temp', [["FID", "COUNT"]], ["ORIG_FID"])
    d = 1.0

    list_invalidos=[]


    for el in  arcpy.da.SearchCursor(x_temp,['ORIG_FID','COUNT_FID'],"count_fid>1"):

        #print el[0],el[1]

        list_invalidos.append(el[0])
    #print list_invalidos


    #for el in arcpy.ListFields(resultado_temp):
    #    print el.name
#
#
    #for el in arcpy.da.SearchCursor(resultado_temp,'*'):
    #    print el


    with arcpy.da.UpdateCursor(resultado_temp, ['OID','SHAPE@']) as cursor:
        for x in cursor:
            if x[0] in list_invalidos:
                lineas = x[1]
                puntos = []
                for linea in lineas:
                    for pnt in linea:
                        puntos.append((pnt.X, pnt.Y))
                dx = puntos[1][0] - puntos[0][0]
                dy = puntos[1][1] - puntos[0][1]
                h = math.hypot(dx, dy)

                x1 = (puntos[1][0] - puntos[0][0]) * d / h + puntos[0][0]
                y1 = (puntos[1][1] - puntos[0][1]) * d / h + puntos[0][1]
                x[1] = arcpy.Polyline(arcpy.Array([arcpy.Point(puntos[0][0], puntos[0][1]), arcpy.Point(x1, y1)]))
                cursor.updateRow(x)

    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
    resultado_temp_copy=arcpy.CopyFeatures_management(resultado_temp,'in_memory/resultado_temp_copy')

    return resultado_temp_copy





def ExtenderRutasLineas():
    arcpy.AddField_management(tb_rutas_lineas,"tipo_rutas","short" )
    arcpy.AddField_management(tb_rutas_lineas_multifamiliar, "tipo_rutas", "short")

    arcpy.CalculateField_management(tb_rutas_lineas, "tipo_rutas", "1","PYTHON_9.3")
    arcpy.CalculateField_management(tb_rutas_lineas_multifamiliar, "tipo_rutas", "2","PYTHON_9.3")
    tb_rutas_merge=arcpy.Merge_management([tb_rutas_lineas,tb_rutas_lineas_multifamiliar],"in_memory/tb_rutas_merge")
    arcpy.ExtendLine_edit(tb_rutas_merge,"1 METERS")
    arcpy.Select_analysis(tb_rutas_merge,tb_rutas_lineas,"tipo_rutas=1")

    arcpy.AddField_management(tb_rutas_lineas, "AEU_FINAL", "short")
    arcpy.AddField_management(tb_rutas_lineas_multifamiliar, "AEU_FINAL", "short")
    arcpy.CalculateField_management(tb_rutas_lineas, "AEU_FINAL", "!AEU!","PYTHON_9.3")
    arcpy.CalculateField_management(tb_rutas_lineas_multifamiliar, "AEU_FINAL", "!AEU!","PYTHON_9.3")

def RelacionarRutasLineasConAEU_ANT():
    arcpy.env.overwriteOutput = True
    TB_VIVIENDAS_CORTES_shp = path_ini + "/AEU/CrearRepresentacionAEU/TB_VIVIENDAS_CORTES.shp"
    TB_RUTAS_PUNTOS = path_ini + "/AEU/CrearRepresentacionAEU/TB_RUTAS_PUNTOS.shp"
    TB_RUTAS_PUNTOS_MIN = path_ini + "/AEU/CrearRepresentacionAEU/TB_RUTAS_PUNTOS_MIN.shp"
    TB_RUTAS_PUNTOS_MIN_SELECT = path_ini + "/AEU/CrearRepresentacionAEU/TB_RUTAS_PUNTOS_MIN_SELECT.shp"
    TB_INTERSECT_RUTAS_1 = path_ini + "/AEU/CrearRepresentacionAEU/TB_INTERSECT_RUTAS_1.shp"
    TB_INTERSECT_RUTAS_2 = path_ini + "/AEU/CrearRepresentacionAEU/TB_INTERSECT_RUTAS_2.shp"
    TB_INTERSECT_RUTAS_3 = path_ini + "/AEU/CrearRepresentacionAEU/TB_INTERSECT_RUTAS_3.shp"
    MZS_AEU = path_ini + "/AEU/EnumerarAEUViviendas/MZS_AEU.dbf"
    TB_INTERSECT_RUTAS = path_ini + "/AEU/CrearRepresentacionAEU/TB_INTERSECT_RUTAS.shp"
    TB_STADISTICS_INTERSECT_RUTAS = path_ini + "/AEU/CrearRepresentacionAEU/TB_STADISTICS_INTERSECT_RUTAS"
    TB_RUTAS_LINEAS = path_ini + "/AEU/CrearRepresentacionAEU/TB_RUTAS_LINEAS.shp"

    TB_RUTAS_PREPARACION = path_ini + "\\AEU\\CrearRepresentacionAEU\\TB_RUTAS_PREPARACION.shp"
    TB_RUTAS_PREPARACION_Layer = "TB_RUTAS_PREPARACION_Layer"
    VIVIENDAS_MZS_OR_MAX = path_ini + "/AEU/EnumerarAEUViviendas/VIVIENDAS_MZS_OR_MAX.shp"

    TB_VERTICES_FINAL_AEU = path_ini + "/AEU/CrearRepresentacionAEU/TB_VERTICES_FINAL_AEU.shp"
    TB_RUTAS_LINEAS_TEMP = path_ini + "/AEU/CrearRepresentacionAEU/TB_RUTAS_LINEAS_TEMP.shp"
    TB_RUTAS_LINEAS_DISSOLVE = path_ini + "/AEU/CrearRepresentacionAEU/TB_RUTAS_LINEAS_DISSOLVE.shp"
    TB_RUTAS_LINEAS_DISSOLVE_Layer = "TB_RUTAS_LINEAS_DISSOLVE_Layer"
    TB_RUTAS_LINEAS_TEMP_2 = path_ini + "/AEU/CrearRepresentacionAEU/TB_RUTAS_LINEAS_TEMP_2.shp"

    arcpy.MakeFeatureLayer_management(TB_RUTAS_PREPARACION, TB_RUTAS_PREPARACION_Layer)

    arcpy.Intersect_analysis(
        [TB_RUTAS_PREPARACION, TB_VIVIENDAS_CORTES_shp],
        TB_INTERSECT_RUTAS_1, "ALL", "0.70 Meters", "INPUT")


    list_deletefield = ["FID_VIVIEN", "FLG_CORTE", "FLG_MZ_1", "ORIG_FID", "FID_TB_VIV", "ID", "UBIGEO_1", "CODCCPP_1",
                        "ZONA_1", "MANZANA_1", "FALSO_COD", "NOMCCPP", "DEPARTAMEN", "PROVINCIA", "DISTRITO", "AREA",
                        "FRENTE_ORD", "P19A", "P29", "P29M", "p29_1", "ID_P23", "P23", "OR_VIV_AEU"]

    arcpy.DeleteField_management(TB_INTERSECT_RUTAS_1, list_deletefield)

    arcpy.Intersect_analysis(
        [TB_RUTAS_PREPARACION, TB_RUTAS_PUNTOS_MIN],
        TB_INTERSECT_RUTAS_2, "ALL", "0.70 Meters", "INPUT")
    list_deletefield = ["ORIG_FID", "FID_TB_AEU", "ID", "UBIGEO_1", "CODCCPP_1", "ZONA_1", "MANZANA_1", "CANT_VIV"]

    arcpy.DeleteField_management(TB_INTERSECT_RUTAS_2, list_deletefield)

    arcpy.Intersect_analysis(
        [TB_RUTAS_PREPARACION, TB_VERTICES_FINAL_AEU],
        TB_INTERSECT_RUTAS_3, "ALL", "0.20 Meters", "INPUT")

    list_deletefield = ["FID_TB_VER", "ORIG_FID", "FID_TB_AEU", "ID", "UBIGEO_1", "CODCCPP_1", "ZONA_1", "MANZANA_1",
                        "CANT_VIV"]

    arcpy.DeleteField_management(TB_INTERSECT_RUTAS_3, list_deletefield)

    arcpy.Merge_management([TB_INTERSECT_RUTAS_1, TB_INTERSECT_RUTAS_2, TB_INTERSECT_RUTAS_3], TB_INTERSECT_RUTAS)

    arcpy.Statistics_analysis(TB_INTERSECT_RUTAS, TB_STADISTICS_INTERSECT_RUTAS, [["AEU", "MAX"], ["ID_REG_OR", "MAX"]],
                              ["FID_TB_RUT"])

    arcpy.AddJoin_management(TB_RUTAS_PREPARACION_Layer, "FID", TB_STADISTICS_INTERSECT_RUTAS, "FID_TB_RUT")

    arcpy.CopyFeatures_management(TB_RUTAS_PREPARACION_Layer, TB_RUTAS_LINEAS_TEMP)

    add_field = [["UBIGEO", "TEXT"], ["CODCCPP", "TEXT"], ["ZONA", "TEXT"], ["MANZANA", "TEXT"], ["FLG_MZ", "SHORT"],
                 ["AEU", "SHORT"], ["ID_REG_OR", "SHORT"],["TIPO","SHORT"]]

    for el in add_field:
        arcpy.AddField_management(TB_RUTAS_LINEAS_TEMP, el[0], el[1])

    calculate_field = [["UBIGEO", "!TB_RUTAS_P!"], ["CODCCPP", "!TB_RUTAS_1!"], ["ZONA", "!TB_RUTAS_2!"],
                       ["MANZANA", "!TB_RUTAS_3!"], ["FLG_MZ", "!TB_RUTAS_4!"], ["AEU", "!tb_stadi_4!"],
                       ["ID_REG_OR", "!tb_stadi_5!"]]

    for el in calculate_field:
        arcpy.CalculateField_management(TB_RUTAS_LINEAS_TEMP, el[0], el[1], "PYTHON_9.3")

    list_deletefield = ["TB_RUTAS_P", "TB_RUTAS_1", "TB_RUTAS_2", "TB_RUTAS_3", "TB_RUTAS_4", "TB_RUTAS_5",
                        "tb_stadist", "tb_stadi_1", "tb_stadi_2", "tb_stadi_3", "tb_stadi_4", "tb_stadi_5"]

    arcpy.DeleteField_management(TB_RUTAS_LINEAS_TEMP, list_deletefield)

    sort_fields = [["UBIGEO", "ASCENDING"], ["CODCCPP", "ASCENDING"], ["ZONA", "ASCENDING"], ["MANZANA", "ASCENDING"],
                   ["AEU", "ASCENDING"], ["ID_REG_OR", "ASCENDING"]]

    #arcpy.Sort_management(TB_RUTAS_LINEAS_TEMP, TB_RUTAS_LINEAS_TEMP_2, sort_fields)
    arcpy.Sort_management(TB_RUTAS_LINEAS_TEMP, TB_RUTAS_LINEAS, sort_fields)

    arcpy.CalculateField_management(TB_RUTAS_LINEAS,"TIPO","3")


    '''
    arcpy.Dissolve_management(TB_RUTAS_LINEAS_TEMP_2, TB_RUTAS_LINEAS_DISSOLVE,
                              ["UBIGEO", "CODCCPP", "ZONA", "MANZANA", "FLG_MZ", "AEU"], "", "MULTI_PART",
                              "DISSOLVE_LINES")

    arcpy.MakeFeatureLayer_management(TB_RUTAS_LINEAS_DISSOLVE, TB_RUTAS_LINEAS_DISSOLVE_Layer)
    arcpy.AddField_management(TB_RUTAS_LINEAS_DISSOLVE_Layer, "ID_RUTA", "TEXT")
    arcpy.CalculateField_management(TB_RUTAS_LINEAS_DISSOLVE_Layer, "ID_RUTA",
                                    "!UBIGEO!+!CODCCPP!+!ZONA!+!MANZANA!+str(!AEU!)", "PYTHON_9.3")


    arcpy.AddField_management(MZS_AEU, "ID_RUTA", "TEXT")
    arcpy.CalculateField_management(MZS_AEU, "ID_RUTA", "!UBIGEO!+!CODCCPP!+!ZONA!+!MANZANA!+str(!AEU!)",
                                    "PYTHON_9.3")

    arcpy.AddJoin_management(TB_RUTAS_LINEAS_DISSOLVE_Layer, "ID_RUTA", MZS_AEU, "ID_RUTA")

    arcpy.CopyFeatures_management(TB_RUTAS_LINEAS_DISSOLVE_Layer, TB_RUTAS_LINEAS)

    add_field = [["UBIGEO", "TEXT"], ["CODCCPP", "TEXT"], ["ZONA", "TEXT"], ["MANZANA", "TEXT"], ["FLG_MZ", "SHORT"],
                 ["AEU", "SHORT"], ["CANT_VIV", "SHORT"],["ID_REG_OR", "SHORT"]]

    for el in add_field:
        arcpy.AddField_management(TB_RUTAS_LINEAS, el[0], el[1])

    calculate_field = [["UBIGEO", "!TB_RUTAS_L!"], ["CODCCPP", "!TB_RUTAS_1!"], ["ZONA", "!TB_RUTAS_2!"],
                       ["MANZANA", "!TB_RUTAS_3!"], ["FLG_MZ", "!TB_RUTAS_4!"], ["AEU", "!TB_RUTAS_5!"],["ID_REG_OR", "!TB_RUTAS_6!"],
                       ["CANT_VIV", "!MZS_AEU_CA!"]]

    for el in calculate_field:
        arcpy.CalculateField_management(TB_RUTAS_LINEAS, el[0], el[1], "PYTHON_9.3")

    list_deletefield = ["TB_RUTAS_P", "TB_RUTAS_1", "TB_RUTAS_2", "TB_RUTAS_3", "TB_RUTAS_4", "TB_RUTAS_5",
                        "TB_RUTAS_6", "MZS_AEU_OI", "MZS_AEU_FI", "MZS_AEU_UB", "MZS_AEU_CO", "MZS_AEU_ZO",
                        "MZS_AEU_MA", "MZS_AEU_CA", "MZS_AEU_ID"]
    arcpy.DeleteField_management(TB_RUTAS_LINEAS, list_deletefield)
    '''

#def RelacionarRutasLineasConAEU():
#    arcpy.env.overwriteOutput = True
#
#    viviendas_mfl0 = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendasMFL0")
#    manzanas_mfl = arcpy.MakeFeatureLayer_management(tb_manzanas, "manzanasMFL")
#    vivSeleccionadas = arcpy.SelectLayerByLocation_management(viviendas_mfl0, "INTERSECT", manzanas_mfl, "#","NEW_SELECTION")
#    viviendas_mfl = arcpy.MakeFeatureLayer_management(vivSeleccionadas, "viviendasMFL")
#    primeros_frentes_mfl=arcpy.MakeFeatureLayer_management(tb_frentes, "primerosfrentesMFL","FRENTE_ORD=1")
#
#    puntos_corte_mfl = arcpy.MakeFeatureLayer_management(tb_puntos_corte, "puntoscorteMFL")
#    # AÑADIR VALOR DE AEU A AQUELLOS SEGMENTOS CON VALOR NULO
#
#    '''
#    En el proceso de agreado del valor de AEU a los frentes, algunos segmentos del frente no se intersectan con tb_viviendas_ordenadas, por tanto presentan
#    valores Nulos, en este caso es necesario asignar la informacion del AEU al que pertenecen, para esto se realiza una seleccion por localizacion
#    solo de aquellos segmentos, intersectados con la informacion de puntos de corte final de los AEU, cada segmento tendra por lo menos dos
#    puntos cercanos, de los cuales se selecciona el de mayor valor de AEU.
#    '''
#
#    viviendas0 = arcpy.CopyFeatures_management(tb_viviendas_ordenadas, 'in_memory' + '\\' + 'viviendas0')
#    arcpy.AddField_management(viviendas0,"IDFRENTE","TEXT")
#    arcpy.CalculateField_management(viviendas0,"IDFRENTE","!UBIGEO!+!ZONA!+!MANZANA!+str(!FRENTE_ORD!)","PYTHON_9.3")
#    ListaFields = [ x.name for x in arcpy.ListFields(viviendas0)]
#
#
#
#    for n in ['FID','Shape','AEU','IDFRENTE']:
#        ListaFields.remove(n)
#
#    arcpy.DeleteField_management(viviendas0, ListaFields)
#    arcpy.Append_management(tb_vertice_final_manzana,viviendas0, "NO_TEST")
#
#    join = arcpy.SpatialJoin_analysis(tb_rutas_preparacion, viviendas0, tb_rutas_lineas)
#
#
#
#
#    with arcpy.da.UpdateCursor(join , ["SHAPE@", "IDFRENTE", "AEU","FID"], "AEU =0") as cursorUC:
#        for x in cursorUC:
#            print "IDFRENTE", x[1],x[2],x[3]
#
#            ruta_nulo = arcpy.MakeFeatureLayer_management(join, "ruta_nulo", "FID = {}".format(x[3]))
#            mergeSelected = arcpy.SelectLayerByLocation_management(puntos_corte_mfl, "INTERSECT",ruta_nulo , "0.7 METERS", "NEW_SELECTION")
#            merge_temp=arcpy.CopyFeatures_management(mergeSelected,'in_memory/merge_temp')
#
#            LimitesCercanos = []
#
#
#            #for n in arcpy.da.SearchCursor(mergeSelected, ["AEU"]):
#            #    LimitesCercanos.append(n[0])
#
#            for n in arcpy.da.SearchCursor(merge_temp, ["AEU"]):
#                LimitesCercanos.append(n[0])
#
#            print "limites cercanos ", LimitesCercanos
#
#            #primerfrente_intersect_ruta = arcpy.SelectLayerByLocation_management(primeros_frentes_mfl, "INTERSECT",
#            #                                                                     ruta_nulo, "", "NEW_SELECTION")
#
#            #cant_intersect = int(arcpy.GetCount_management(primerfrente_intersect_ruta).getOutput(0))
#
#            #print "cant intersect ", cant_intersect
#
#            if len(LimitesCercanos)==0:
#                seleccionado=0
#            else:
#                #if cant_intersect>0:
#                #    seleccionado = min(LimitesCercanos)
#                #else:
#                seleccionado = max(LimitesCercanos)
#            x[2] = seleccionado
#            cursorUC.updateRow(x)
#
#    #del cursorUC
#    arcpy.AddField_management(tb_rutas_lineas, "TIPO", "SHORT")
#    arcpy.CalculateField_management(tb_rutas_lineas,"TIPO","3")
#
#    ListaFields = [x.name for x in arcpy.ListFields(tb_rutas_lineas)]
#    for n in ['FID','Shape','UBIGEO','CODCCPP','ZONA','MANZANA','AEU','TIPO']:
#        ListaFields.remove(n)
#
#    arcpy.DeleteField_management(tb_rutas_lineas, ListaFields)
#

def RelacionarRutasLineasConAEU():
    arcpy.env.overwriteOutput = True

    viviendas_mfl0 = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendasMFL0")
    manzanas_mfl = arcpy.MakeFeatureLayer_management(tb_manzanas, "manzanasMFL")
    vivSeleccionadas = arcpy.SelectLayerByLocation_management(viviendas_mfl0, "INTERSECT", manzanas_mfl, "#","NEW_SELECTION")
    viviendas_mfl = arcpy.MakeFeatureLayer_management(vivSeleccionadas, "viviendasMFL")

    tb_puntos_corte_buffer = arcpy.Buffer_analysis(tb_puntos_corte, 'in_memory/tb_p_c_buffer', '150 METERS')

    tb_centroides = arcpy.FeatureToPoint_management(tb_puntos_corte_buffer, 'in_memory/centroides', "INSIDE")

    puntos_corte_mfl = arcpy.MakeFeatureLayer_management(tb_centroides, "puntoscorteMFL")
    puntos_ini_mfl=arcpy.MakeFeatureLayer_management(tb_puntos_inicio, "puntosinicioMFL")
    #puntos_corte_mfl = arcpy.MakeFeatureLayer_management(tb_puntos_corte, "puntoscorteMFL")
    # AÑADIR VALOR DE AEU A AQUELLOS SEGMENTOS CON VALOR NULO

    '''
    En el proceso de agreado del valor de AEU a los frentes, algunos segmentos del frente no se intersectan con tb_viviendas_ordenadas, por tanto presentan
    valores Nulos, en este caso es necesario asignar la informacion del AEU al que pertenecen, para esto se realiza una seleccion por localizacion
    solo de aquellos segmentos, intersectados con la informacion de puntos de corte final de los AEU, cada segmento tendra por lo menos dos
    puntos cercanos, de los cuales se selecciona el de mayor valor de AEU.
    '''

    viviendas0 = arcpy.CopyFeatures_management(tb_viviendas_ordenadas, 'in_memory' + '\\' + 'viviendas0')
    arcpy.AddField_management(viviendas0,"IDFRENTE","TEXT")
    arcpy.CalculateField_management(viviendas0,"IDFRENTE","!UBIGEO!+!ZONA!+!MANZANA!+str(!FRENTE_ORD!)","PYTHON_9.3")
    ListaFields = [ x.name for x in arcpy.ListFields(viviendas0)]

    for n in ['FID','Shape','AEU','IDFRENTE']:
        ListaFields.remove(n)

    arcpy.DeleteField_management(viviendas0, ListaFields)
    arcpy.Append_management(tb_vertice_final_manzana,viviendas0, "NO_TEST")

    join = arcpy.SpatialJoin_analysis(tb_rutas_preparacion, viviendas0, tb_rutas_lineas)

    with arcpy.da.UpdateCursor(join , ["SHAPE@", "IDFRENTE", "AEU","FID"], "AEU =0") as cursorUC:
        for x in cursorUC:
            #print x[1],x[2],x[3]
            ruta_nulo = arcpy.MakeFeatureLayer_management(join, "ruta_nulo", "FID = {}".format(x[3]))
            mergeSelected = arcpy.SelectLayerByLocation_management(puntos_corte_mfl, "INTERSECT",ruta_nulo , "0.7 METERS", "NEW_SELECTION")

            puntos_ini_select = arcpy.SelectLayerByLocation_management(puntos_ini_mfl, "INTERSECT", ruta_nulo,"1 METERS", "NEW_SELECTION")
            LimitesCercanos = []
            cant_puntos_ini=int(arcpy.GetCount_management(puntos_ini_select).getOutput(0))

            for n in arcpy.da.SearchCursor(mergeSelected, ["AEU"]):
                LimitesCercanos.append(n[0])

            print LimitesCercanos
            if len(LimitesCercanos)==0:
                seleccionado=0
            else:
                if cant_puntos_ini>0:
                    seleccionado = min(LimitesCercanos)
                else:
                    seleccionado = max(LimitesCercanos)
            x[2] = seleccionado
            cursorUC.updateRow(x)

    del cursorUC
    arcpy.AddField_management(tb_rutas_lineas, "TIPO", "SHORT")
    arcpy.CalculateField_management(tb_rutas_lineas,"TIPO","3")

    ListaFields = [x.name for x in arcpy.ListFields(tb_rutas_lineas)]
    for n in ['FID','Shape','UBIGEO','CODCCPP','ZONA','MANZANA','AEU','TIPO']:
        ListaFields.remove(n)

    arcpy.DeleteField_management(tb_rutas_lineas, ListaFields)





def ClasificarPuntosIntermedios():

    buffer_rutas_lineas=arcpy.Buffer_analysis(tb_rutas_lineas, 'in_memory/buffer_rutas_lineas', '0.1 meters', "FULL", "FLAT")

    intersec_rutas_lineas = arcpy.Intersect_analysis([buffer_rutas_lineas, tb_rutas_puntos],"in_memory/intersec_rutas_lineas")
    intersec_selec=arcpy.Select_analysis(intersec_rutas_lineas,"in_memory/intersec_selec"," AEU=AEU_1")

    list_id_selec=[x[0] for x in arcpy.da.SearchCursor(intersec_selec, ["FID_TB_RUTAS_PUNTOS"])]

    with arcpy.da.UpdateCursor(tb_rutas_puntos, ["FID","TIPO"]) as cursor:
        for row in cursor:
            if row[0] in list_id_selec:
                row[1] = 2
                cursor.updateRow(row)

def Contar(elemento,lista):
    cant=0
    for el in lista:
        if elemento==el:
            cant=cant+1
    return cant

def ClasificarLineasRutas():
    arcpy.AddField_management(tb_rutas_lineas,"ID_RUTA","TEXT")
    arcpy.CalculateField_management(tb_rutas_lineas,"ID_RUTA","!UBIGEO!+!ZONA!+str(!AEU!).zfill(3)+!MANZANA!.zfill(4)","PYTHON_9.3")
    arcpy.AddField_management(tb_rutas_lineas_multifamiliar,"ID_RUTA","TEXT")
    arcpy.CalculateField_management(tb_rutas_lineas_multifamiliar,"ID_RUTA","!UBIGEO!+!ZONA!+str(!AEU!).zfill(3)+!MANZANA!.zfill(4)","PYTHON_9.3")
    ids_rutas_multifamiliar=[x[0] for x in arcpy.da.SearchCursor(tb_rutas_lineas_multifamiliar, ["ID_RUTA"])]
    ids_rutas_multifamiliar_tipos=[ [x[0],x[1]] for x in arcpy.da.SearchCursor(tb_rutas_lineas_multifamiliar, ["ID_RUTA","TIPO"])]
    arcpy.AddField_management(tb_rutas_lineas,"TIPO","short")

    tipo=0

    with arcpy.da.UpdateCursor(tb_rutas_lineas, ["ID_RUTA", "TIPO"]) as cursor:
        for row in cursor:
            #print row[0]
            cant_repetidos = Contar(row[0],ids_rutas_multifamiliar)
            #cant_viv=row[2]
            #print cant_repetidos
            if cant_repetidos == 0 :   ######tipo 3 es que no tiene ningun multifamiliar a los costados es decir
                row[1] = 3

            elif cant_repetidos == 1:
                for el in ids_rutas_multifamiliar_tipos:
                    if el[0]==row[0]:
                        tipo=el[1]
                if tipo==1:
                    row[1] = 0

                else:
                    row[1] = 1

            elif cant_repetidos == 2:
                row[1] = 2

            cursor.updateRow(row)

def CrearAEUS():
    arcpy.Statistics_analysis(tb_rutas, tb_aeus, [["CANT_VIV", "SUM"]], ["UBIGEO", "CODCCPP", "ZONA",  "AEU"])

    arcpy.AddField_management(tb_aeus, "CANT_VIV", "SHORT")
    arcpy.CalculateField_management(tb_aeus, "CANT_VIV","[SUM_CANT_V]", "VB", "")

def SegundaPasada():
    arcpy.AddField_management(tb_rutas, "TECHO_S_P", "SHORT")
    resumen_zona = arcpy.Statistics_analysis(tb_aeus, 'in_memory/zonas_aeu', [["AEU", "MAX"]],
                                             ["UBIGEO", "ZONA"])
    zonas = [(x[0], x[1], x[2]) for x in arcpy.da.SearchCursor(resumen_zona, ["UBIGEO", "ZONA", "MAX_AEU"])]

    for zona in zonas:
        where_zona = " UBIGEO='{}' AND ZONA='{}' ".format(zona[0], zona[1])

        id_estrato = [x[0] for x in arcpy.da.SearchCursor(tb_zonas, ['ID_ESTRATO'], where_zona)][0]

        cant_aeus_zona = int(zona[2])
        if id_estrato == 1:
            techo_segunda_pasada = techo_segunda_pasada_gra_ciud
        else:
            techo_segunda_pasada = techo_segunda_pasada_peq_ciud

        where_aeus_elegidos = " UBIGEO='{}' AND ZONA='{}' AND CANT_VIV<=9 ".format(zona[0], zona[1])
        where_aeus = " UBIGEO='{}' AND ZONA='{}' ".format(zona[0], zona[1])

        ############################diccionario de datos#############################


        dic_aeus_elegidos = dict((x[2], [x[3]]) for x in
                                 arcpy.da.SearchCursor(tb_aeus, ["UBIGEO", "ZONA", "AEU", "CANT_VIV"],
                                                       where_aeus_elegidos))
        aeus_eleg_l = dic_aeus_elegidos.keys()

        aeus_eleg = sorted(aeus_eleg_l)[:]

        dic_aeus = dict((x[2], [x[3]]) for x in
                        arcpy.da.SearchCursor(tb_aeus, ["UBIGEO", "ZONA", "AEU", "CANT_VIV"], where_aeus))

        aeus = dic_aeus.keys()

        aeus_temp = aeus[:]
        aeus_opciones = aeus[:]

        if len(aeus) > 1:
            if int(len(aeus_eleg) > 0):
                while int(len(aeus_eleg) > 0):
                    aeu_eleg = aeus_eleg[0]
                    aeus_eleg.remove(aeu_eleg)
                    datos_aeu_eleg = dic_aeus_elegidos[aeu_eleg]
                    aeu = int(aeu_eleg)
                    cant_viv = int(datos_aeu_eleg[0])

                    if (aeu > 1 and aeu < cant_aeus_zona):
                        aeu_op_anterior = aeu - 1
                        aeu_op_posterior = aeu + 1

                        temp_aeu_anterior = dic_aeus[aeu_op_anterior]
                        temp_aeu_posterior = dic_aeus[aeu_op_posterior]

                        cant_viv_anterior = int(temp_aeu_anterior[0]) + cant_viv
                        cant_viv_posterior = int(temp_aeu_posterior[0]) + cant_viv

                        if ((cant_viv_anterior <= techo_segunda_pasada) and (
                            cant_viv_posterior <= techo_segunda_pasada)):
                            if (cant_viv_anterior <= cant_viv_posterior):
                                if aeu_op_anterior in aeus_opciones:
                                    aeu_selec = aeu_op_anterior
                                    aeus_opciones.remove(aeu_selec)
                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)

                                elif aeu_op_posterior in aeus_opciones:
                                    aeu_selec = aeu_op_posterior
                                    aeus_opciones.remove(aeu_selec)
                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)
                                else:
                                    aeu_selec = -1
                            else:
                                if aeu_op_posterior in aeus_opciones:
                                    aeu_selec = aeu_op_posterior
                                    aeus_opciones.remove(aeu_selec)
                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)
                                elif aeu_op_anterior in aeus_opciones:
                                    aeu_selec = aeu_op_anterior
                                    aeus_opciones.remove(aeu_selec)
                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)
                                else:
                                    aeu_selec = -1
                        elif ((cant_viv_anterior <= techo_segunda_pasada)):
                            if aeu_op_anterior in aeus_opciones:
                                aeu_selec = aeu_op_anterior
                                aeus_opciones.remove(aeu_selec)
                                if aeu_selec in aeus_eleg:
                                    aeus_eleg.remove(aeu_selec)
                            else:
                                aeu_selec = -1

                        elif ((cant_viv_posterior <= techo_segunda_pasada)):
                            if aeu_op_posterior in aeus_opciones:
                                aeu_selec = aeu_op_posterior
                                aeus_opciones.remove(aeu_selec)
                                if aeu_selec in aeus_eleg:
                                    aeus_eleg.remove(aeu_selec)
                            else:
                                aeu_selec = -1
                        else:
                            aeu_selec = -1

                        if aeu_selec != -1:
                            aeus_temp[(aeus_temp.index(aeu))] = aeu_selec

                    elif (aeu == 1):
                        aeu_op_posterior = aeu + 1
                        temp_aeu_posterior = dic_aeus[aeu_op_posterior]
                        cant_viv_posterior = int(temp_aeu_posterior[0]) + cant_viv
                        if (cant_viv_posterior <= techo_segunda_pasada):
                            if aeu_op_posterior in aeus_opciones:
                                aeu_selec = aeu_op_posterior
                                aeus_opciones.remove(aeu_selec)
                                aeus_temp[(aeus_temp.index(aeu))] = aeu_selec

                                if aeu_selec in aeus_eleg:
                                    aeus_eleg.remove(aeu_selec)


                    else:
                        aeu_op_anterior = aeu - 1
                        temp_aeu_anterior = dic_aeus[aeu_op_anterior]
                        cant_viv_anterior = int(temp_aeu_anterior[0]) + cant_viv
                        if (cant_viv_anterior <= techo_segunda_pasada):
                            if aeu_op_anterior in aeus_opciones:
                                aeu_selec = aeu_op_anterior
                                aeus_opciones.remove(aeu_selec)
                                aeus_temp[(aeus_temp.index(aeu))] = aeu_selec
                                if aeu_selec in aeus_eleg:
                                    aeus_eleg.remove(aeu_selec)


        aeus_renumerados = []

        i = 0
        temp_anterior = 0

        for x in aeus_temp:
            if temp_anterior != x:
                i = i + 1
            aeus_renumerados.append(i)
            temp_anterior = x

        with arcpy.da.UpdateCursor(tb_rutas, ["AEU"], where_aeus) as cursor:
            for x in cursor:
                indice = int(x[0]) - 1
                x[0] = aeus_renumerados[indice]
                cursor.updateRow(x)

        with arcpy.da.UpdateCursor(tb_viviendas_ordenadas, ["AEU"], where_aeus) as cursor:
            for x in cursor:
                indice = int(x[0]) - 1
                x[0] = aeus_renumerados[indice]
                cursor.updateRow(x)
        where_viv=" UBIGEO='{}' AND ZONA='{}'  AND OR_VIV_AEU<>0 ".format(zona[0], zona[1])

        list_viviendas_ordenadas = [[x[0], x[1], x[2], x[3], x[4], x[5]] for x in
                                       arcpy.da.SearchCursor(tb_viviendas_ordenadas,
                                                             ["UBIGEO", "ZONA", "MANZANA", "ID_REG_OR", "AEU",
                                                              "OR_VIV_AEU"], where_viv)]
        for aeu_renumerado in aeus_renumerados:
            orden = 1
            for el in list_viviendas_ordenadas:
                if (el[4]==aeu_renumerado):
                    el[5]=orden
                    orden=orden+1

        dic_viviendas_ordenadas = dict((u'{}{}{}{}'.format(x[0], x[1], x[2], x[3]),  x[5]) for x in list_viviendas_ordenadas)

        with arcpy.da.UpdateCursor(tb_viviendas_ordenadas, ["UBIGEO","ZONA","MANZANA","ID_REG_OR","OR_VIV_AEU"], where_viv) as cursor:
             for x in cursor:
                 id=u'{}{}{}{}'.format(x[0],x[1],x[2],x[3])
                 id_reg_or=dic_viviendas_ordenadas[id]
                 x[4]=id_reg_or
                 cursor.updateRow(x)

def CrearAEUSSegundaPasada():
    arcpy.Statistics_analysis(tb_rutas, tb_aeus, [["CANT_VIV", "SUM"]],
                              ["UBIGEO", "CODCCPP","ZONA", "AEU"])
    arcpy.AddField_management(tb_aeus, "CANT_VIV", "SHORT")
    arcpy.AddField_management(tb_aeus, "CANT_PAG", "SHORT")

    arcpy.CalculateField_management(tb_aeus, "CANT_VIV",
                                    "[SUM_CANT_V]", "VB", "")
    arcpy.DeleteField_management(tb_aeus, ["SUM_CANT_V"])

def SeleccionAEUParaCalidad():
    arcpy.env.workspace = path_ini
    zonas=list(set((x[0],x[1])  for x in  arcpy.da.SearchCursor(tb_aeus,['UBIGEO','ZONA'])))
    arcpy.AddField_management(tb_aeus,'FLAG_CALID','SHORT')
    arcpy.CalculateField_management(tb_aeus,'FLAG_CALID','0')
    list_aeus =[[x[0],x[1],x[2]]  for x in  arcpy.da.SearchCursor(tb_aeus,['UBIGEO','ZONA','AEU'])]
    aeus_selecc=[]
    for zona in zonas:
        aeus_zona=[]

        for aeu in list_aeus:
            if (aeu[0]==zona[0] and aeu[1]==zona[1]):
                aeus_zona.append(aeu)
        cant_aeus=len(aeus_zona)
        aeus_zona_temp=aeus_zona[:]

        if (cant_aeus>0 and cant_aeus<=2):
            for aeu_selecc in aeus_zona:
                aeus_selecc.append(u'{}{}{}'.format(aeu_selecc[0], aeu_selecc[1],aeu_selecc[2]) )
        else:
            tamanio_muestra=0
            if (cant_aeus>2 and cant_aeus<=25):
                tamanio_muestra=2
            elif (cant_aeus > 25 and cant_aeus <= 150):
                tamanio_muestra = 8
            elif (cant_aeus > 150 and cant_aeus <= 280):
                tamanio_muestra = 13
            elif (cant_aeus > 280 and cant_aeus <= 500):
                tamanio_muestra = 20
            else:
                tamanio_muestra = 32

            for i in range(tamanio_muestra):
                aeu_selecc = random.choice(aeus_zona_temp)
                aeus_zona_temp.remove(aeu_selecc)
                aeus_selecc.append(u'{}{}{}'.format(aeu_selecc[0], aeu_selecc[1], aeu_selecc[2]))
    with arcpy.da.UpdateCursor(tb_aeus,["UBIGEO", "ZONA", "AEU", "FLAG_CALID"]) as cursor:
        for row in cursor:
            id_aeu='{}{}{}'.format(row[0],row[1],row[2])

            if id_aeu in aeus_selecc:
                row[3]=1
            cursor.updateRow(row)

def EnumerarSecciones():
    arcpy.env.workspace = path_ini + ""
    arcpy.AddField_management(tb_aeus, "SECCION", "SHORT")
    list_zonas=[[x[0],x[1]]  for x in arcpy.da.SearchCursor(tb_zonas,['UBIGEO','ZONA'])]
    for zona in list_zonas:
        where_zona=" UBIGEO='{}' AND ZONA='{}' ".format(zona[0],zona[1])
        list_aeus=[[x[0],x[1],x[2]] for x in arcpy.da.SearchCursor(tb_aeus,["UBIGEO", "ZONA", "AEU"],where_zona)]
        cant_aeus = int(len(list_aeus))
        cant_secciones = 1

        if cant_aeus>6:
            cant_secciones = int(math.ceil(float(cant_aeus) / 6))
            cant_aeus_por_seccion = cant_aeus/cant_secciones
            resto = cant_aeus % cant_secciones
        else:
            cant_secciones=1
            cant_aeus_por_seccion=cant_aeus
            resto=0

        seccion = 1
        cant_aeus_aux = 0

        with arcpy.da.UpdateCursor(tb_aeus, ['UBIGEO', 'ZONA', 'AEU','SECCION'], where_zona) as cursor:
            for x in cursor:
                x[3]=int(seccion)
                cant_aeus_aux=cant_aeus_aux+1
                if (resto>0):
                    if ((cant_aeus_por_seccion+1)==cant_aeus_aux):
                        resto=resto-1
                        cant_aeus_aux=0
                        seccion=seccion+1
                else:
                    if (cant_aeus_por_seccion  == cant_aeus_aux):
                        cant_aeus_aux = 0
                        seccion =seccion+1
                cursor.updateRow(x)

def CrearSecciones():
    arcpy.env.overwriteOutput = True

    arcpy.Dissolve_management(tb_rutas_lineas, tb_aeus_lineas, ["UBIGEO", "CODCCPP","ZONA", "AEU"])
    arcpy.Dissolve_management(tb_rutas_puntos, tb_aeus_puntos, ["UBIGEO","CODCCPP","ZONA", "AEU"])

    if arcpy.Exists(tb_secciones):
        arcpy.Delete_management(tb_secciones)

    temp_secciones=arcpy.Statistics_analysis(tb_aeus, 'in_memory/temp_secciones', [["CANT_VIV", "SUM"]], ["UBIGEO", "CODCCPP", "ZONA", "SECCION"])
    arcpy.MakeTableView_management(tb_aeus, "aeus")
    arcpy.MakeFeatureLayer_management(tb_aeus_lineas, "aeus_lineas")
    arcpy.MakeFeatureLayer_management(tb_aeus_lineas, "aeus_lineas_restantes")
    arcpy.MakeFeatureLayer_management(tb_aeus_puntos, "aeus_puntos")
    arcpy.MakeTableView_management(tb_rutas, "mzs_aeu_temp")
    arcpy.MakeFeatureLayer_management(tb_rutas_lineas, "rutas_lineas_temp")
    viv1=arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendas_ordenadas_temp")
    viv2 = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendas_ordenadas_temp2")
    j = 0

    for row1 in arcpy.da.SearchCursor(temp_secciones, ["UBIGEO","ZONA", "SECCION", "SUM_CANT_VIV", "CODCCPP"]):
        where_seccion = " UBIGEO='{}' and ZONA='{}' AND SECCION={}".format(row1[0],row1[1],row1[2])
        ids_aeus = [[x[0],x[1],x[2]] for x in arcpy.da.SearchCursor(tb_aeus, ["UBIGEO","ZONA","AEU"],where_seccion)]
        ids_aeus_ord = sorted(ids_aeus, key=lambda tup: tup[2])
        id_aeu_min=ids_aeus_ord[0]
        id_aeu_max=ids_aeus_ord[-1]

        where_viv_min=" UBIGEO='{}' AND ZONA='{}' AND AEU={}".format(id_aeu_min[0],id_aeu_min[1],id_aeu_min[2])
        where_viv_max = " UBIGEO='{}' AND ZONA='{}' AND AEU={}".format(id_aeu_max[0], id_aeu_max[1], id_aeu_max[2])

        where_aeu=expresion.Expresion_2(ids_aeus, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["AEU", "SHORT"]])

        #print where_viv_min
        #print where_viv_max

        viviendas_aeu_min=arcpy.SelectLayerByAttribute_management(viv1, "NEW_SELECTION", where_viv_min)
        viviendas_aeu_max= arcpy.SelectLayerByAttribute_management(viv2, "NEW_SELECTION",where_viv_max)

        list_viv_aeu_min=[ [v[0],v[1],v[2],v[3],v[4],v[5],v[6],v[7],v[8],v[9],v[10],v[11],v[12],v[13]] for v in arcpy.da.SearchCursor(viviendas_aeu_min, ['MANZANA', 'P29', 'P20', 'P21', 'P22_A', 'P23', 'P25', 'P26', 'P27_A', 'P32','P19A', 'FRENTE_ORD', 'P29_A', 'P29_P'])]

        #print list_viv_aeu_min

        list_viv_aeu_max = [[v[0], v[1], v[2], v[3], v[4], v[5], v[6],v[7], v[8], v[9], v[10], v[11], v[12], v[13]] for v in arcpy.da.SearchCursor(viviendas_aeu_max,['MANZANA', 'P29', 'P20', 'P21', 'P22_A', 'P23', 'P25', 'P26','P27_A', 'P32', 'P19A', 'FRENTE_ORD', 'P29_A', 'P29_P'])]
        #print list_viv_aeu_max
        #primera_puerta=list_viv_aeu_min[0]
        #ultima_puerta = list_viv_aeu_max[-1]

        #obs = u"La seccion N°{} comprende desde: {}{}{}{}{}{}{}{} hasta: {}{}{}{}{}{}{}{}.".format \
        #    (str(row1[2]).zfill(3),
        #     u"La  MZ N°{}".format(primera_puerta[0]),
        #     u", {} {}".format(primera_puerta[2], primera_puerta[3]),
        #     u", N° de puerta {}".format(primera_puerta[4]) if (primera_puerta[4] != " " and  primera_puerta[4] != " ") else "",
        #     u", N° de block {}".format(primera_puerta[5]) if (primera_puerta[5] != 0 and  primera_puerta[5] != " ") else "",
#
        #     u", Lote N°{}".format(primera_puerta[6]) if (primera_puerta[6] != 0 and primera_puerta[6] != " ") else "",
        #     u", Piso N°{}".format(primera_puerta[7]) if (primera_puerta[7] != 0  and primera_puerta[7] != " ") else "",
        #     u", Interior N°{}".format(primera_puerta[8]) if (primera_puerta[8] != 0  and primera_puerta[8] != " ") else "",
        #     u", {}".format(primera_puerta[9]),
        #     u"La  MZ N°{}".format(ultima_puerta[0]),
        #     u", {} {}".format(ultima_puerta[2], ultima_puerta[3]),
        #     u", N° de puerta {}".format(ultima_puerta[4]) if (ultima_puerta[4] != 0 and ultima_puerta[4] != " ") else "",
        #     u", N° de block {}".format(ultima_puerta[5]) if (ultima_puerta[5] != 0 and ultima_puerta[5] != " ")  else "",
        #     u", Lote N°{}".format(ultima_puerta[6]) if (ultima_puerta[6] != 0 and ultima_puerta[6] != " ") else "",
        #     u", Piso N°{}".format(ultima_puerta[7]) if (ultima_puerta[7] != 0 and ultima_puerta[7] != " ") else "",
        #     u", Interior N°{}".format(ultima_puerta[8]) if (ultima_puerta[8] != 0 and ultima_puerta[8] != " " )else "",
        #     u", {}".format(ultima_puerta[9])
        #     )
        rutas_lineas = arcpy.SelectLayerByAttribute_management("rutas_lineas_temp", "NEW_SELECTION", where_aeu)
        if (int(arcpy.GetCount_management(rutas_lineas).getOutput(0)) > 0):
            out_vertices = arcpy.FeatureVerticesToPoints_management(rutas_lineas, "in_memory/vertices_rutas", "ALL")
            out_seccion = arcpy.PointsToLine_management(out_vertices, "in_memory/lineas_secciones", "MANZANA","AEU")
            out_feature_1 = "in_memory/Buffer" + str(row1[0]) + str(row1[1]) + str(row1[2])
            out_feature_2 = "in_memory/FeatureToLine" + str(row1[0]) + str(row1[1]) + str(row1[2])
            out_feature_3 = "in_memory/FeatureToPolygon" + str(row1[0]) + str(row1[1]) + str(row1[2])
            out_feature = path_ini + "/tb_secciones/" + str(row1[0]) + str(row1[1]) + str(row1[2]) + ".shp"
            arcpy.Buffer_analysis(out_seccion, out_feature_1, '5 METERS', 'FULL', 'FLAT', 'LIST')
            arcpy.FeatureToLine_management(out_feature_1, out_feature_2)
            arcpy.FeatureToPolygon_management(out_feature_2, out_feature_3)
            arcpy.Dissolve_management(out_feature_3, out_feature)
            arcpy.AddField_management(out_feature, "UBIGEO", "TEXT")
            arcpy.AddField_management(out_feature, "CODCCPP", "TEXT")
            arcpy.AddField_management(out_feature, "ZONA", "TEXT")
            arcpy.AddField_management(out_feature, "SECCION", "SHORT")
            arcpy.AddField_management(out_feature, "CANT_VIV", "SHORT")
            arcpy.AddField_management(out_feature, "OBS", "TEXT", "#", "#", "254")
            calculate_expression1 = "\'" + str(row1[0]) + "\'"
            calculate_expression2 = "\'" + str(row1[1]) + "\'"
            calculate_expression3 = str(row1[2])
            calculate_expression4 = str(row1[3])
            calculate_expression5 = "\'" + str(row1[4]) + "\'"
            arcpy.CalculateField_management(out_feature, "UBIGEO", calculate_expression1, "PYTHON_9.3")
            arcpy.CalculateField_management(out_feature, "CODCCPP", calculate_expression5, "PYTHON_9.3")
            arcpy.CalculateField_management(out_feature, "ZONA", calculate_expression2, "PYTHON_9.3")
            arcpy.CalculateField_management(out_feature, "SECCION", calculate_expression3, "PYTHON_9.3")
            arcpy.CalculateField_management(out_feature, "CANT_VIV", calculate_expression4, "PYTHON_9.3")
            #arcpy.CalculateField_management(out_feature, "OBS", u"'{}'".format(obs), "PYTHON_9.3")

            if (j == 0):
                arcpy.CopyFeatures_management(out_feature, tb_secciones)
            else:
                arcpy.Append_management(out_feature, tb_secciones, "NO_TEST")
            j = j + 1
            arcpy.Delete_management(out_feature_1)
            arcpy.Delete_management(out_feature_2)
            arcpy.Delete_management(out_feature_3)
            arcpy.Delete_management(out_vertices)
            arcpy.Delete_management(out_seccion)

        else:

            # out_feature = "in_memory/Buffer" + str(row1[0]) + str(row1[1]) + str(row1[2])
            out_feature = path_ini + "/tb_secciones/" + str(row1[0]) + str(
                row1[1]) + str(row1[2]) + ".shp"

            arcpy.SelectLayerByAttribute_management("aeus_puntos", "NEW_SELECTION", where_aeu)
            arcpy.Buffer_analysis("aeus_puntos", out_feature, '5 METERS', 'FULL', 'ROUND', 'LIST',
                                  ['UBIGEO', 'CODCCPP', 'ZONA'])

            arcpy.AddField_management(out_feature, "SECCION", "SHORT")
            arcpy.AddField_management(out_feature, "CANT_VIV", "SHORT")
            arcpy.AddField_management(out_feature, "OBS", "TEXT")
            calculate_expression3 = str(row1[2])
            calculate_expression4 = str(row1[3])
            arcpy.CalculateField_management(out_feature, "SECCION",calculate_expression3 , "PYTHON_9.3")
            arcpy.CalculateField_management(out_feature, "CANT_VIV", calculate_expression4, "PYTHON_9.3")
            #arcpy.CalculateField_management(out_feature, "OBS", u"'{}'".format(obs), "PYTHON_9.3")


            if (j == 0):
                arcpy.CopyFeatures_management(out_feature, tb_secciones)
            else:
                arcpy.Append_management(out_feature, tb_secciones, "NO_TEST")
            j = j + 1

            arcpy.Delete_management(out_feature)
    arcpy.AddField_management(tb_secciones,'CANT_PAG','SHORT')

def SeleccionLegajoParaCalidad():
    arcpy.env.workspace = path_ini
    zonas=list(set((x[0],x[1])  for x in  arcpy.da.SearchCursor(tb_secciones,['UBIGEO','ZONA'])))
    arcpy.AddField_management(tb_secciones,'FLAG_CALID','SHORT')
    arcpy.CalculateField_management(tb_secciones,'FLAG_CALID','0')
    list_secciones =[[x[0],x[1],x[2]]  for x in  arcpy.da.SearchCursor(tb_secciones,['UBIGEO','ZONA','SECCION'])]
    seccs_selecc=[]

    for zona in zonas:
        secc_zona=[]

        for seccion in list_secciones:
            if (seccion[0]==zona[0] and seccion[1]==zona[1]):
                secc_zona.append(seccion)
        cant_secciones=len(secc_zona)
        secciones_zona_temp=secc_zona[:]

        if (cant_secciones>0 and cant_secciones<=2):
            for secc_selecc in secc_zona:
                seccs_selecc.append('{}{}{}'.format(secc_selecc[0], secc_selecc[1],secc_selecc[2]) )
        else:
            tamanio_muestra=0
            if (cant_secciones>2 and cant_secciones<=25):
                tamanio_muestra=2
            elif (cant_secciones > 25 and cant_secciones <= 150):
                tamanio_muestra = 8
            elif (cant_secciones > 150 and cant_secciones <= 280):
                tamanio_muestra = 13
            elif (cant_secciones > 280 and cant_secciones <= 500):
                tamanio_muestra = 20
            else:
                tamanio_muestra = 32

            for i in range(tamanio_muestra):
                secc_selecc = random.choice(secciones_zona_temp)
                secciones_zona_temp.remove(secc_selecc)
                seccs_selecc.append('{}{}{}'.format(secc_selecc[0], secc_selecc[1], secc_selecc[2]))

    with arcpy.da.UpdateCursor(tb_secciones,["UBIGEO", "ZONA", "SECCION", "FLAG_CALID"]) as cursor:
        for row in cursor:
            id_secc='{}{}{}'.format(row[0],row[1],row[2])

            if id_secc in seccs_selecc:
                row[3]=1
            cursor.updateRow(row)

def CrearSubZonas():


    zonas= [[x[0],x[1],x[2]] for x in arcpy.da.SearchCursor(tb_zonas,['UBIGEO','ZONA','CODCCPP'])]
    arcpy.AddField_management(tb_secciones,"SUBZONA","SHORT")
    for zona in zonas:
        where_zona="UBIGEO='{}' AND ZONA='{}'".format(zona[0],zona[1])
        list_secciones = [[x[0], x[1], x[2]] for x in arcpy.da.SearchCursor(tb_secciones, ['UBIGEO', 'ZONA', 'SECCION'],where_zona)]
        cant_secciones=int(len(list_secciones))
        cant_subzonas=1
        if (cant_secciones>17):
            cant_subzonas=int(math.ceil( float(cant_secciones)/17))
            cant_secc_por_zona=cant_secciones/cant_subzonas
            resto=cant_secciones%cant_subzonas
        else:
            cant_subzonas=1
            cant_secc_por_zona=cant_secciones
            resto=0


        if (cant_secciones>17):
            subzona=1
        else:
            subzona = 0
        cant_sec_aux=0


        #print cant_subzonas,cant_secc_por_zona,resto

        with arcpy.da.UpdateCursor(tb_secciones, ['UBIGEO', 'ZONA', 'SECCION','SUBZONA'], where_zona) as cursor:
            for x in cursor:
                x[3]=subzona
                cant_sec_aux=cant_sec_aux+1

                if (resto>0):
                    if ((cant_secc_por_zona+1)==cant_sec_aux):
                        resto=resto-1
                        cant_sec_aux=0
                        subzona=subzona+1
                else:
                    if (cant_secc_por_zona  == cant_sec_aux):
                        cant_sec_aux = 0
                        subzona =subzona+1
                cursor.updateRow(x)

    arcpy.Statistics_analysis(tb_secciones,tb_subzonas,[['CANT_VIV','SUM']],['UBIGEO','CODCCPP','ZONA','SUBZONA'])
    arcpy.AddField_management(tb_subzonas, "CANT_VIV", "SHORT")
    arcpy.CalculateField_management(tb_subzonas,'CANT_VIV','!SUM_CANT_V!','PYTHON_9.3')
    arcpy.DeleteField_management(tb_subzonas,['SUM_CANT_V'])
    arcpy.AddField_management(tb_subzonas, 'CANT_PAG','SHORT')

    for zona in zonas:
        where_zona="UBIGEO='{}' AND ZONA='{}'".format(zona[0],zona[1])
        list_subzonas = [x[0] for x in arcpy.da.SearchCursor(tb_subzonas, ['CANT_VIV'],where_zona)]



        if (len(list_subzonas)>1):
            total_viv = 0
            for cant in list_subzonas:
                total_viv = total_viv + cant
            with arcpy.da.InsertCursor(tb_subzonas, ['UBIGEO','CODCCPP','ZONA','SUBZONA','CANT_VIV']) as cursor:
                row=(zona[0],zona[2],zona[1],0,total_viv)
                cursor.insertRow(row)

def actualizar_campos():
    #tb_secciones

    arcpy.AddField_management(tb_secciones, 'SECCION2', 'TEXT')
    arcpy.AddField_management(tb_secciones, 'SECCION2', 'TEXT')


def preparar_registros():
    list_capas = [tb_viviendas_ordenadas,tb_rutas,tb_aeus,tb_secciones, tb_subzonas,  tb_rutas_lineas]
    if arcpy.Exists(tb_rutas_lineas_multifamiliar):
        list_capas.append(tb_rutas_lineas_multifamiliar)


    list_campos_modificables=['AEU','SECCION']
    print fase
    for el in list_capas:
        dir=os.path.split(el)
        dir_copia=os.path.join(dir[0],"final_{}".format(dir[1]))
        print dir_copia
        arcpy.Copy_management(el, dir_copia)
        #print arcpy.ListFields(dir_copia)
        for campo in arcpy.ListFields(dir_copia):
            campo_old=campo.name
            if campo_old in list_campos_modificables:
                campo_nuevo="{}2".format(campo_old)
                arcpy.AddField_management(dir_copia,campo_nuevo,'TEXT')
                calculate="str(!{}!).zfill(3)".format(campo_old)
                arcpy.CalculateField_management(dir_copia,campo_nuevo,calculate,"PYTHON_9.3")
                arcpy.DeleteField_management(dir_copia,[campo_old])
                arcpy.AddField_management(dir_copia, campo_old,'TEXT')
                arcpy.CalculateField_management(dir_copia, campo_old, "!{}!".format(campo_nuevo), "PYTHON_9.3")
                arcpy.DeleteField_management(dir_copia,[campo_nuevo])


        if el==tb_viviendas_ordenadas:
            fields=arcpy.ListFields(dir_copia)
            list_campos_validos = ['FID', 'Shape', 'UBIGEO', 'CODCCPP', 'ZONA', 'MANZANA', 'ID_REG_OR', 'FRENTE_ORD','AEU','ID_VIV','OR_VIV_AEU','P29','CANT_POB']
            delete_fields = []
            for el in fields:
                if el.name not in list_campos_validos:
                    delete_fields.append(el.name)

            arcpy.DeleteField_management(dir_copia, delete_fields)
        arcpy.AddField_management(dir_copia, 'FASE', 'TEXT')
        arcpy.CalculateField_management(dir_copia, 'FASE','"{}"'.format(fase),"PYTHON")

    list_capas=[final_aeu,final_seccion,final_subzona]
    for el in list_capas:
        arcpy.AddField_management(el,'CANT_PAG','SHORT')
        arcpy.AddField_management(el, 'COD_CROQ', 'TEXT')
        arcpy.AddField_management(el, 'RUTA_CROQ', 'TEXT')
        arcpy.AddField_management(el, 'RUTA_WEB', 'TEXT')



def insertar_registros(data):
    print datetime.today()
    #arcpy.env.workspace = "Database Connections/PruebaSegmentacion.sde"
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)

    db='CPV_SEGMENTACION_GDB'
    if arcpy.Exists("{}.sde".format(db)) == False:
        arcpy.CreateDatabaseConnection_management("Database Connections",
                                                  "{}.sde".format(db),
                                                  "SQL_SERVER",
                                                  ip_server,
                                                  "DATABASE_AUTH",
                                                  "sde",
                                                  "$deDEs4Rr0lLo",
                                                  "#",
                                                  "{}".format(db),
                                                  "#",
                                                  "#",
                                                  "#",
                                                  "#")
    arcpy.env.workspace = "Database Connections/{}.sde".format(db)
    path_conexion2 = "Database Connections/{}.sde".format(db)

    #segm_ruta=path_conexion2 + "/GEODB_CPV_SEGM.SDE.SEGM_RUTA"
    #segm_aeu=path_conexion2 + "/GEODB_CPV_SEGM.SDE.SEGM_AEU"
    #segm_seccion=path_conexion2 + "/GEODB_CPV_SEGM.SDE.URBANO/GEODB_CPV_SEGM.SDE.SEGM_SECCION"
    #segm_subzona = path_conexion2 + "/GEODB_CPV_SEGM.SDE.SEGM_SUBZONA"
    #segm_rutas_lineas=path_conexion2 + "/GEODB_CPV_SEGM.SDE.SEG_RESUL_U/GEODB_CPV_SEGM.SDE.SEGM_RUTAS_LINEAS"
    #segm_rutas_lineas_multi = path_conexion2 + "/GEODB_CPV_SEGM.SDE.SEG_RESUL_U/GEODB_CPV_SEGM.SDE.SEGM_RUTAS_LINEAS_MULTIFAMILIAR"
    #segm_rutas_puntos = path_conexion2 + "/GEODB_CPV_SEGM.SDE.SEG_RESUL_U/GEODB_CPV_SEGM.SDE.SEGM_RUTAS_PUNTOS"
    #segm_frentes_1 = path_conexion2 + "/GEODB_CPV_SEGM.SDE.URBANO/GEODB_CPV_SEGM.SDE.SEGM_FRENTES_1"
    #segm_frentes_2 = path_conexion2 + "/GEODB_CPV_SEGM.SDE.URBANO/GEODB_CPV_SEGM.SDE.SEGM_FRENTES_2"
    #segm_frentes_3 = path_conexion2 + "/GEODB_CPV_SEGM.SDE.URBANO/GEODB_CPV_SEGM.SDE.SEGM_FRENTES_3"
    #segm_sitios_interes = path_conexion2 + "/GEODB_CPV_SEGM.SDE.URBANO/GEODB_CPV_SEGM.SDE.SEGM_SITIOS_INTERES"



    segm_ruta=path_conexion2 + "/{db}.SDE.SEGM_RUTA".format(db=db)
    segm_aeu=path_conexion2 + "/{db}.SDE.SEGM_AEU".format(db=db)
    segm_subzona = path_conexion2 + "/{db}.SDE.SEGM_SUBZONA".format(db=db)
    segm_seccion=path_conexion2 + "/{db}.SDE.URBANO/{db}.SDE.SEGM_SECCION".format(db=db)
    segm_rutas_lineas=path_conexion2 + "/{db}.SDE.URBANO/{db}.SDE.SEGM_RUTAS_LINEAS".format(db=db)
    segm_rutas_lineas_multi = path_conexion2 + "/{db}.SDE.URBANO/{db}.SDE.SEGM_RUTAS_LINEAS_MULTIFAMILIAR".format(db=db)
    segm_rutas_puntos = path_conexion2 + "/{db}.SDE.URBANO/{db}.SDE.SEGM_RUTAS_PUNTOS".format(db=db)
    segm_frentes_1 = path_conexion2 + "/{db}.SDE.URBANO/{db}.SDE.SEGM_FRENTES_1".format(db=db)
    segm_frentes_2 = path_conexion2 + "/{db}.SDE.URBANO/{db}.SDE.SEGM_FRENTES_2".format(db=db)
    segm_frentes_3 = path_conexion2 + "/{db}.SDE.URBANO/{db}.SDE.SEGM_FRENTES_3".format(db=db)
    segm_sitios_interes = path_conexion2 + "/{db}.SDE.URBANO/{db}.SDE.SEGM_SITIOS_INTERES".format(db=db)
    segm_vivienda_u = path_conexion2 + "/{db}.SDE.SEGM_VIVIENDA_U".format(db=db)
    list_capas_ini = [tb_viviendas_ordenadas_dbf, tb_rutas, tb_aeus, tb_secciones, tb_subzonas, tb_rutas_lineas,tb_rutas_lineas_multifamiliar]
    list_capas_fin = [segm_vivienda_u, segm_ruta, segm_aeu, segm_seccion, segm_subzona, segm_rutas_lineas,segm_rutas_lineas_multi]
    list_capas = []


    for i, el in enumerate(list_capas_ini):
        dir = os.path.split(el)
        dir_copia = os.path.join(dir[0], "final_{}".format(dir[1]))
        print dir_copia

        formato = dir[1].split(".")[1]

        if el == tb_rutas_lineas_multifamiliar:
            if arcpy.Exists(tb_rutas_lineas_multifamiliar):
                list_capas.append([dir_copia, segm_rutas_lineas_multi, 1])


        else:

            if formato == 'shp':
                list_capas.append([dir_copia, list_capas_fin[i], 1])
            else:
                list_capas.append([dir_copia, list_capas_fin[i], 2])

    conn = conx.Conexion2()
    cursor = conn.cursor()

    for el in data:
        ubigeo = el[0]
        zona = el[1]
        sql_query = """
                DELETE {db}.SDE.SEGM_RUTA where ubigeo='{ubigeo}' and zona='{zona}'
                DELETE {db}.SDE.SEGM_AEU where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_VIVIENDA_U  where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_SECCION where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_SUBZONA  where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_RUTAS_LINEAS  where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_RUTAS_LINEAS_MULTIFAMILIAR  where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_FRENTES_1  where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_FRENTES_2  where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_FRENTES_3  where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_SITIOS_INTERES  where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_RUTAS_PUNTOS  where ubigeo='{ubigeo}' and zona='{zona}'
                """.format(ubigeo=ubigeo, zona=zona,db=db)
        print sql_query
        cursor.execute(sql_query)
        conn.commit()
    conn.close()

    list_capas.append([tb_frentes_1, segm_frentes_1, 1])
    list_capas.append([tb_frentes_2, segm_frentes_2, 1])
    list_capas.append([tb_frentes_3, segm_frentes_3, 1])
    list_capas.append([final_sitios_interes, segm_sitios_interes, 1])
    list_capas.append([tb_rutas_puntos,segm_rutas_puntos,1])

    i = 0
    for el in list_capas:
        i = i + 1
        print el[0],el[1]
        if (int(el[2])>1):
            a = arcpy.MakeTableView_management(el[0], "a{}".format(i), )


        else:
            a = arcpy.MakeFeatureLayer_management(el[0], "b{}".format(i))
            cant_el=int(arcpy.GetCount_management(a).getOutput(0))
            print 'cantidad de elementos: ', cant_el

        arcpy.Append_management(a, el[1], "NO_TEST")



        #for i in range(30):
#
        #    if arcpy.TestSchemaLock(el[1]):
        #        arcpy.Append_management(a, el[1], "NO_TEST")
        #        break
#
        #    else:
#
        #        if i>=30:
        #            arcpy.Append_management(a, el[1], "NO_TEST")
        #        else:
        #            time.sleep(30)
        #            print datetime.today()
#

            #arcpy.Append_management(a, el[1], "NO_TEST")


        #print el[0],el[1]
        #i = i + 1
        #if (int(el[2])>1):
        #    a = arcpy.MakeTableView_management(el[1], "a{}".format(i), "{} ".format( where_list,fase) )
        #    b = arcpy.MakeTableView_management(el[0], "b{}".format(i), "{} and \"FASE\"='{}'".format( where_list,fase) )
        #else:
        #    a = arcpy.MakeFeatureLayer_management(el[1], "a{}".format(i),"{} ".format( where_list,fase) )
        #    b = arcpy.MakeFeatureLayer_management(el[0], "b{}".format(i), "{} and \"FASE\"='{}'".format( where_list,fase) )
#
        #if (int(arcpy.GetCount_management(a).getOutput(0)) > 0):
        #    arcpy.DeleteRows_management(a)
        #arcpy.Append_management(b, el[1], "NO_TEST")

def insertar_registros_2(data):
    ip_server = '192.168.202.84'
    usuario = 'sde'
    password = 'wruvA7a*tat*'
    database = 'CPV_MONITOREO_GIS'

    arcpy.env.workspace = "Database Connections/"
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)

    if arcpy.Exists("{}.sde".format(database)) == False:
        arcpy.CreateDatabaseConnection_management("Database Connections",
                                                  "{}.sde".format(database),
                                                  "SQL_SERVER",
                                                  ip_server,
                                                  "DATABASE_AUTH",
                                                  usuario,
                                                  password,
                                                  "#",
                                                  database,
                                                  "#",
                                                  "#",
                                                  "#",
                                                  "#")
    arcpy.env.workspace = "Database Connections/{}.sde".format(database)
    path_conexion_db = "Database Connections/{}.sde".format(database)
    nom_dataset="{}.sde.{}".format(database,'CPV2017')
    path_dataset_db = os.path.join(path_conexion_db,nom_dataset)

    tb_viv=os.path.join(path_conexion_db,'{}.SDE.TAB_SEGM_U_VIV'.format(database))
    tb_secc=os.path.join(path_dataset_db,'{}.SDE.CARTO_U_SECCION'.format(database))


    list_capas_ini = [ tb_secciones,tb_viviendas_ordenadas_dbf]
    list_capas_fin = [ tb_secc,tb_viv]

    list_capas = []

    for i, el in enumerate(list_capas_ini):
        dir = os.path.split(el)
        dir_copia = os.path.join(dir[0], "final_{}".format(dir[1]))
        print dir_copia

        formato = dir[1].split(".")[1]

        if el == tb_rutas_lineas_multifamiliar:
            if arcpy.Exists(tb_rutas_lineas_multifamiliar):
                list_capas.append([dir_copia, segm_rutas_lineas_multi, 1])


        else:

            if formato == 'shp':
                list_capas.append([dir_copia, list_capas_fin[i], 1])
            else:
                list_capas.append([dir_copia, list_capas_fin[i], 2])


    i = 0



    conn = conx.Conexion3()
    cursor = conn.cursor()
    for el in data:
        ubigeo=el[0]
        zona=el[1]
        sql_query = """
            DELETE SDE.CARTO_U_SECCION where ubigeo='{ubigeo}' and zona='{zona}' and fase='{fase}'
            DELETE  SDE.TAB_SEGM_U_VIV where ubigeo='{ubigeo}' and zona='{zona}' and fase='{fase}'
            """.format(ubigeo=ubigeo, zona=zona,fase=fase)
        cursor.execute(sql_query)
        conn.commit()
    conn.close()




    for el in list_capas:

        i = i + 1

        print el[0]
        if (int(el[2])>1):
            a = arcpy.MakeTableView_management(el[0], "a{}".format(i), )



        else:
            a = arcpy.MakeFeatureLayer_management(el[0], "a{}".format(i))
        arcpy.Append_management(a, el[1], "NO_TEST")

def CrearCarpetasExportarCroquis(data,campos):
    arcpy.env.workspace = path_ini
    paths_ini = [

        path_croquis,
        path_listados,
        path_croquis_listado,
        path_etiquetas,
    ]
    paths = [

        path_urbano_croquis,
        path_urbano_listados,
        path_urbano_croquis_listado,
        path_urbano_etiquetas,
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

def EnumerarDistritoOperativo(distritos):
    list_dist = distritos

    arcpy.AddField_management(tb_zonas,'DISTOPE','SHORT')


    for distrito in distritos:
        list_zonas= [ [x[0],x[1],x[2]] for x in arcpy.SearchCursor(tb_zonas,['UBIGEO','ZONA','DISTRITO']," UBIGEO='{}' ".format(distrito)) ]
        nom_dist=list_zonas[2]

        cant_zonas=len(list_zonas)

        if cant_zonas>18:
            cant_dist_ope = int(math.ceil(float(cant_zonas) / 18))
            cant_zonas_por_dist_ope = cant_zonas/cant_dist_ope
            resto = cant_zonas % cant_dist_ope

        else:
            cant_dist_ope=1
            cant_zonas_por_dist_ope=cant_zonas
            resto=0

        dist_ope = 1
        cant_zonas_por_dist_aux = 0

        with arcpy.da.UpdateCursor(tb_zonas, ['UBIGEO', 'ZONA', 'DISTOPE'], "UBIGEO= '{}'".format(distrito)) as cursor:
            for x in cursor:
                x[2]=int(dist_ope)
                cant_zonas_por_dist_aux=cant_zonas_por_dist_aux+1
                if (resto>0):
                    if ((cant_zonas_por_dist_ope+1)==cant_zonas_por_dist_aux):
                        resto=resto-1
                        cant_zonas_por_dist_aux=0
                        dist_ope=dist_ope+1
                else:
                    if (cant_zonas_por_dist_ope==cant_zonas_por_dist_aux):
                        cant_zonas_por_dist_aux = 0
                        dist_ope = dist_ope + 1
                cursor.updateRow(x)



#
    #    dist_ope = 1
    #    cant_aeus_aux = 0
#
    #    with arcpy.da.UpdateCursor(tb_aeus, ['UBIGEO', 'ZONA', 'AEU','SECCION'], where_zona) as cursor:
    #        for x in cursor:
    #            x[3]=int(seccion)
    #            cant_aeus_aux=cant_aeus_aux+1
    #            if (resto>0):
    #                if ((cant_aeus_por_seccion+1)==cant_aeus_aux):
    #                    resto=resto-1
    #                    cant_aeus_aux=0
    #                    seccion=seccion+1
    #            else:
    #                if (cant_aeus_por_seccion  == cant_aeus_aux):
    #                    cant_aeus_aux = 0
    #                    seccion =seccion+1
    #            cursor.updateRow(x)


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
    #errores_print = errores.decode(sys.getdefaultencoding())

    print errores_print
    if len(errores_print) > 0:
        return 1
    else:
        return 0

def ExportarCroquisUrbanoAEU(where_expression=''):
    arcpy.env.workspace = path_ini + ""
    with arcpy.da.UpdateCursor(final_aeu, ["UBIGEO", "ZONA","CODCCPP" ,"SECCION", "AEU", "CANT_VIV",'CANT_PAG','COD_CROQ' ,'RUTA_CROQ',
                                           'RUTA_WEB'],where_expression) as cursor:


        for row in cursor:



            ubigeo = row[0]
            ccdd = ubigeo[0:2]
            ccpp = ubigeo[2:4]
            ccdi = ubigeo[4:6]
            zona = row[1]
            codccpp = row[2]
            zona_etiqueta = expresion.EtiquetaZona(zona)
            seccion = '{}'.format(row[3]).zfill(3)
            aeu = '{}'.format(row[4]).zfill(3)
            cant_viv = row[5]
            #######################################Creamos los filtros del AEU############################################################
            where_aeu = "UBIGEO ='{}' AND ZONA='{}' AND AEU={}".format(ubigeo, zona, aeu)
            where_seccion = "UBIGEO='{}' AND ZONA='{}' AND SECCION={}".format(ubigeo, zona, seccion)
            where_zona = "UBIGEO='{}' AND ZONA='{}'".format(ubigeo, zona)
            list_manzanas = [[x[0], x[1], x[2]] for x in
                             arcpy.da.SearchCursor(tb_rutas, ["UBIGEO", "ZONA", "MANZANA"], where_aeu)]
            where_manzanas = expresion.Expresion(list_manzanas, ["UBIGEO", "ZONA", "MANZANA"])
            for x in arcpy.da.SearchCursor(tb_zonas, ['DEPARTAMEN', 'PROVINCIA', 'DISTRITO', 'NOMCCPP'], where_zona):
                dep = x[0]
                prov = x[1]
                dist = x[2]
                nomccpp = x[3]
            for x in arcpy.da.SearchCursor(tb_secciones, ["SUBZONA"], where_seccion):
                subzona = x[0]
            codigo = "{}{}{}{}{}".format(ubigeo, zona, subzona, seccion, aeu)
            #########################################Listamos los layes del mxd #####################################
            mxd = arcpy.mapping.MapDocument(path_plantillas_croquis + "/CroquisUrbanoAEU.mxd")
            df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]

            arcpy.MakeFeatureLayer_management('{}/sitios_interes{}{}.shp'.format(path_ini, ubigeo, zona), "sitios_interes")

            lyrFile1 = arcpy.mapping.Layer("sitios_interes")
            arcpy.ApplySymbologyFromLayer_management(lyrFile1, path_plantillas_layers + "/sitios_interes.lyr")
            arcpy.mapping.AddLayer(df, lyrFile1)

            rutas_lineas_mfl = arcpy.mapping.ListLayers(mxd, "TB_RUTAS_LINEAS")[0]
            rutas_lineas_mfl.definitionQuery = where_aeu
            manzanas_mfl = arcpy.mapping.ListLayers(mxd, "TB_MZS_ORD")[0]
            manzanas_mfl.definitionQuery = where_manzanas

            # manzanas_desast_mfl = arcpy.mapping.ListLayers(mxd, "TB_MZS_DESASTRE")[0]
            # manzanas_desast_mfl.definitionQuery = where_manzanas

            frentes_mfl = arcpy.mapping.ListLayers(mxd, "TB_FRENTES")[0]
            frentes_mfl.definitionQuery = where_manzanas
            frentes_mfl_1 = arcpy.mapping.ListLayers(mxd, "TB_FRENTES_1")[0]
            frentes_mfl_1.definitionQuery = where_manzanas

            frentes_mfl_2 = arcpy.mapping.ListLayers(mxd, "TB_FRENTES_2")[0]
            frentes_mfl_2.definitionQuery = where_manzanas

            frentes_mfl_3 = arcpy.mapping.ListLayers(mxd, "TB_FRENTES_3")[0]
            frentes_mfl_3.definitionQuery = where_manzanas

            puntos_multifamiliar_mfl = arcpy.mapping.ListLayers(mxd, "TB_RUTAS_PUNTOS")[0]
            if arcpy.Exists(tb_rutas_lineas_multifamiliar):
                rutas_lineas_multifamiliar_mfl = arcpy.mapping.ListLayers(mxd, "TB_RUTAS_LINEAS_MULTI")[0]
                rutas_lineas_multifamiliar_mfl.definitionQuery = where_aeu

            cant_rutas_lineas = int(arcpy.GetCount_management(rutas_lineas_mfl).getOutput(0))
            cant_manzanas = int(len(list_manzanas))
            if (cant_rutas_lineas == 0):
                puntos_multifamiliar_mfl.definitionQuery = where_aeu
            else:
                puntos_multifamiliar_mfl.definitionQuery = " AEU=-1"

            ################################################Obtemos la informacion del listado####################################################
            cabecera = [ccdd, dep, ccpp, prov, ccdi, dist, codccpp, nomccpp, 'CIUDAD', zona_etiqueta, subzona, seccion,
                        aeu, cant_viv]

            data = [[v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], v[11], v[12], v[13]] for v in
                    arcpy.da.SearchCursor(tb_viviendas_ordenadas,
                                          ["OR_VIV_AEU", "MANZANA", "ID_REG_OR", "FRENTE_ORD", "P20", "P21", "P22_A", "P23",
                                           "P24",
                                           "P25", "P26", "P27_A", "P28", "P32"], where_aeu)]

            list_data = []


            informacion = [cabecera, data]

            primera_puerta = data[0]

            ultima_puerta = data[-1]
            if (cant_manzanas <= 1):
                obs = u"<BOL>OBSERVACIONES: </BOL>El AEU N°{} comprende desde: {}{}{}{}{}{}{}{} hasta: {}{}{}{}{}{}{}{}.".format \
                    (aeu,
                     u"La  MZ N°{}".format(primera_puerta[1]),
                     u", {} {}".format(primera_puerta[4], primera_puerta[5]),
                     u", N° de puerta {}".format(primera_puerta[6]) if (
                         primera_puerta[6] != " " and primera_puerta[6] != " ") else "",
                     u", N° de block {}".format(primera_puerta[7]) if (
                         primera_puerta[7] != 0 and primera_puerta[7] != " ") else "",
                     u", Lote N°{}".format(primera_puerta[9]) if (
                         primera_puerta[9] != 0 and primera_puerta[9] != " ") else "",
                     u", Piso N°{}".format(primera_puerta[10]) if (
                         primera_puerta[10] != 0 and primera_puerta[10] != " ") else "",
                     u", Interior N°{}".format(primera_puerta[11]) if (
                         primera_puerta[11] != 0 and primera_puerta[11] != " ") else "",
                     u", {}".format(primera_puerta[13]),
                     u"La  MZ N°{}".format(ultima_puerta[1]),
                     u", {} {}".format(ultima_puerta[4], ultima_puerta[5]),
                     u", N° de puerta {}".format(ultima_puerta[6]) if (
                         ultima_puerta[6] != 0 and ultima_puerta[6] != " ") else "",
                     u", N° de block {}".format(ultima_puerta[7]) if (
                         ultima_puerta[7] != 0 and ultima_puerta[7] != " ")  else "",
                     u", Lote N°{}".format(ultima_puerta[9]) if (
                         ultima_puerta[9] != 0 and ultima_puerta[9] != " ") else "",
                     u", Piso N°{}".format(ultima_puerta[10]) if (
                         ultima_puerta[10] != 0 and ultima_puerta[10] != " ") else "",
                     u", Interior N°{}".format(ultima_puerta[11]) if (
                         ultima_puerta[11] != 0 and ultima_puerta[11] != " ")else "",
                     u", {}".format(ultima_puerta[13])
                     )
            else:

                j = 0
                obs_mzs = ''
                for manzana in list_manzanas:
                    if j == 0:
                        obs_mzs = u'{}'.format(manzana[2])
                    else:
                        obs_mzs = u'{},{}'.format(obs_mzs, manzana[2])
                    j = j + 1

                obs = u"<BOL>OBSERVACIONES: </BOL>El AEU N°{} comprende las manzanas  {} . ".format(aeu, obs_mzs)

            #########################################Asignado los valores de las variables de los croquis ################################################
            list_text_el = [["COD_BARRA", "*{}*".format(codigo)], ["TEXT_COD_BARRA", "*{}*".format(codigo)], ["CCDD", ccdd],
                            ["CCPP", ccpp],
                            ["CCDI", ccdi], ["CODCCPP", codccpp], ["DEPARTAMENTO", dep], ["PROVINCIA", prov],
                            ["DISTRITO", dist], ["NOMCCPP", nomccpp],
                            ["ZONA", zona_etiqueta], ["SUBZONA", subzona], ["SECCION", seccion], ["AEU", aeu],
                            ["CANT_VIV", cant_viv], ["FRASE", obs]]
            for text_el in list_text_el:
                el = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", text_el[0])[0]
                el.text = text_el[1]
            ##############################################Asignado la escala del dibujo##############################################################################
            df.extent = manzanas_mfl.getSelectedExtent()
            dflinea = arcpy.Polyline(
                arcpy.Array([arcpy.Point(df.extent.XMin, df.extent.YMin), arcpy.Point(df.extent.XMax, df.extent.YMax)]),
                df.spatialReference)
            distancia = dflinea.getLength("GEODESIC", "METERS")
            if (float(distancia) <= 50):
                df.scale = df.scale * 4
            if (float(distancia) > 50 and float(distancia) <= 100):
                df.scale = df.scale * 3
            if (float(distancia) > 100 and float(distancia) <= 490):
                df.scale = df.scale * 2
            if (float(distancia) > 490 and float(distancia) <= 900):
                df.scale = df.scale * 1.5
            if (float(distancia) > 900 and float(distancia) <= 1200):
                df.scale = df.scale * 1.25
            if (float(distancia) > 1200 and float(distancia) <= 1800):
                df.scale = df.scale * 1.10
            if (float(distancia) > 1800):
                df.scale = df.scale * 1.02

            if df.scale > 2000.0:
                if (df.scale >= 2000.0 and df.scale < 3000.0):
                    d = 10
                elif (df.scale >= 3000.0 and df.scale < 4000.0):
                    d = 15
                elif (df.scale >= 4000.0 and df.scale < 5000.0):
                    d = 18
                elif (df.scale >= 5000.0 and df.scale < 6000.0):
                    d = 20
                else:
                    d = 25
                    ######################################Insertamos los multifamiliares con la geometria modificada y aumentada############################
                if arcpy.Exists(tb_rutas_lineas_multifamiliar):
                    tb_multifamiliar_temp = CambiarLongitudLineaMultifamiliar(tb_rutas_lineas_multifamiliar, d)
                    rutas_lineas_multifamiliar_mfl.definitionQuery = "AEU=-1"
                    arcpy.MakeFeatureLayer_management(tb_multifamiliar_temp, "rutas_lineas_multi_2", where_aeu)
                    lyrFile2 = arcpy.mapping.Layer("rutas_lineas_multi_2")
                    arcpy.ApplySymbologyFromLayer_management(lyrFile2,
                                                             path_plantillas_layers + "/rutas_lineas_multifamiliar.lyr")
                    arcpy.mapping.AddLayer(df, lyrFile2)

            ############################################Exportando croquis#########################################
            out_croquis = "{}\\{}\\{}\\{}.pdf".format(path_urbano_croquis, ubigeo, zona, codigo)
            out_listado = "{}\\{}\\{}\\{}.pdf".format(path_urbano_listados, ubigeo, zona, codigo)
            out_final = "{}\\{}\\{}\\{}.pdf".format(path_urbano_croquis_listado, ubigeo, zona, codigo)

            arcpy.mapping.ExportToPDF(mxd, out_croquis, data_frame="PAGE_LAYOUT", resolution=300)
            error_export_pdf(path_urbano_croquis, ubigeo, zona, codigo, out_croquis, mxd)

            listado.ListadoAEU(informacion, out_listado)

            pdfDoc = arcpy.mapping.PDFDocumentCreate(out_final)
            pdfDoc.appendPages(out_croquis)
            pdfDoc.appendPages(out_listado)

            pdfDoc.saveAndClose()

            [cant_pag,nomb_web]=obtener_datos_pdf(out_final)


            row[6]=cant_pag
            row[7]=codigo
            row[8]=out_final
            row[9]=nomb_web
            cursor.updateRow(row)


            arcpy.mapping.RemoveLayer(df, lyrFile1)
            if ((df.scale > 2000.0) and arcpy.Exists(tb_rutas_lineas_multifamiliar)):
                arcpy.mapping.RemoveLayer(df, lyrFile2)

            del pdfDoc
            del df
            del mxd


def ActualizarCantPaginas(data):
    list_dir_zonas = ['{}\\{}\\{}'.format(path_urbano_croquis_listado, x[0], x[1]) for x in data]
    for dir_zona in list_dir_zonas:
        for archivo in os.listdir(dir_zona):
            if (archivo.endswith('.pdf')):

                l = len(archivo)
                cod = archivo[0:l - 4]
                nom_pdf = "{}\\{}".format(dir_zona, archivo)
                pdf = pyPdf.PdfFileReader(open(nom_pdf, "rb"))
                cant_pag = pdf.getNumPages()

                list_web = nom_pdf.split("\\")[3:]
                nom_web = ""

                for i in list_web:
                    nom_web = nom_web + '/' + i
                # print nom_web

                nom_web = nom_web.replace("\\", "/")

                # print nom_web
                conx.ActualizarCantidadPaginas(fase,cod, cant_pag, nom_pdf, nom_web)

def actualizar_cant_pag():

    #arcpy.da.UpdateCursor(tb_subzonas)
    arcpy.AddField_management(tb_subzonas,'CANT_PAG','')
    #with arcpy.da.UpdateCursor(tb_subzonas, ['']) as cursor:


def CambiarLongitudLineaMultifamiliar(tb_rutas_lineas_multifamiliar_mfl,d):
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(32718)
    tb_copia_multi = arcpy.CopyFeatures_management(tb_rutas_lineas_multifamiliar_mfl, 'in_memory/tb_copia_multi')
    with arcpy.da.UpdateCursor(tb_copia_multi, ['SHAPE@']) as cursor:
        for x in cursor:
            lineas = x[0]
            puntos = []
            for linea in lineas:
                for pnt in linea:
                    puntos.append((pnt.X, pnt.Y))
            dx=puntos[1][0] - puntos[0][0]
            dy=puntos[1][1] - puntos[0][1]
            h=math.hypot(dx,dy)

            x1 = (puntos[1][0] - puntos[0][0]) * d / h + puntos[0][0]
            y1 = (puntos[1][1] - puntos[0][1]) * d / h + puntos[0][1]
            x[0] = arcpy.Polyline(arcpy.Array([arcpy.Point(puntos[0][0], puntos[0][1]), arcpy.Point(x1, y1)]))
            cursor.updateRow(x)

    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
    tb_multifamiliar_temp = arcpy.CopyFeatures_management(tb_copia_multi, 'in_memory/tb_multifamiliar_temp')
    return tb_multifamiliar_temp

def ExportarCroquisUrbanoSeccion(where_expression=''):
    arcpy.env.workspace =path_ini+""

    with arcpy.da.UpdateCursor(final_seccion,
                               ['UBIGEO', 'ZONA','SUBZONA' ,'CODCCPP','SECCION','CANT_VIV','CANT_PAG', 'COD_CROQ',
                                'RUTA_CROQ','RUTA_WEB'], where_expression) as cursor:


        for row in cursor:
            ubigeo=row[0]
            ccdd=ubigeo[0:2]
            ccpp = ubigeo[2:4]
            ccdi = ubigeo[4:6]
            zona=row[1]
            zona_etiqueta=expresion.EtiquetaZona(zona)
            subzona=row[2]
            codccpp = row[3]
            seccion= '{}'.format(row[4]).zfill(3)
            cant_viv = row[5]
            ##obs=u"<BOL>OBSERVACIONES: </BOL> {}".format()
            #######################################Creamos los filtros de la seccion############################################################


            where_seccion = " UBIGEO='{}' AND ZONA='{}' AND SECCION={}".format(ubigeo, zona, seccion)
            where_zona = " UBIGEO='{}' AND ZONA='{}' ".format(ubigeo, zona)
            list_aeus=[[x[0],x[1],x[2]] for x in arcpy.da.SearchCursor(tb_aeus,['UBIGEO','ZONA','AEU'],where_seccion)]
            where_aeus=expresion.Expresion_2(list_aeus, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["AEU", "SHORT"]])
            #list_manzanas = [[x[0], x[1], x[2]] for x in arcpy.da.SearchCursor(tb_rutas, ['UBIGEO', 'ZONA', 'MANZANA'], where_aeus)]
            list_manzanas = list(set((x[0], x[1], x[2]) for x in
                                     arcpy.da.SearchCursor(tb_rutas, ['UBIGEO', 'ZONA', 'MANZANA'], where_aeus)))

            where_manzanas = expresion.Expresion_2(list_manzanas, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["MANZANA", "TEXT"]])
            for x in arcpy.da.SearchCursor(tb_zonas, ['DEPARTAMEN','PROVINCIA','DISTRITO','NOMCCPP'],where_zona):
                dep=x[0]
                prov = x[1]
                dist = x[2]
                nomccpp = x[3]
            aeu_inicial = '{}'.format(list_aeus[0][2]).zfill(3)
            aeu_final = '{}'.format(list_aeus[-1][2]).zfill(3)
            codigo = '{}{}{}{}'.format(ubigeo,zona,subzona,seccion)
            #########################################Listamos los layes del mxd #####################################
            mxd = arcpy.mapping.MapDocument(path_plantillas_croquis + "/CroquisUrbanoSeccion.mxd")
            df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
            arcpy.MakeFeatureLayer_management('{}/sitios_interes{}{}.shp'.format(path_ini,ubigeo,zona),"sitios_interes")
            lyrFile1 = arcpy.mapping.Layer("sitios_interes")
            arcpy.ApplySymbologyFromLayer_management(lyrFile1, path_plantillas_layers + "/sitios_interes.lyr")
            arcpy.mapping.AddLayer(df, lyrFile1)
            seccion_mfl = arcpy.mapping.ListLayers(mxd, "TB_SECCION")[0]
            seccion_mfl.definitionQuery=where_seccion
            mzs_mfl = arcpy.mapping.ListLayers(mxd, "TB_MZS_ORD")[0]
            mzs_mfl.definitionQuery=where_manzanas

            #manzanas_desast_mfl = arcpy.mapping.ListLayers(mxd, "TB_MZS_DESASTRE")[0]
            #manzanas_desast_mfl.definitionQuery = where_manzanas

            ################################################Obtemos la informacion del listado####################################################
            cabecera = [ccdd, dep, ccpp, prov, ccdi, dist, codccpp, nomccpp, 'CIUDAD', zona_etiqueta, subzona,seccion,
                        u'DEL {} AL {} '.format(aeu_inicial, aeu_final), cant_viv]
            data=[]
            for x in arcpy.da.SearchCursor(tb_aeus,['UBIGEO','ZONA','SECCION','AEU','CANT_VIV'],where_seccion):
                mzs=""
                i=0
                for y in  arcpy.da.SearchCursor(tb_rutas,['MANZANA'],"UBIGEO='{}' AND ZONA='{}' AND AEU={}".format(x[0],x[1],x[3])):
                    i=i+1
                    if (i==1):
                        mzs=u"{}".format(y[0])
                    else:
                        mzs = u"{}-{}".format(mzs,y[0])
                data.append([u'{}'.format(x[3]).zfill(3),mzs,x[4]])
            informacion=[cabecera,data]
            #####################################Creacion de la obs####################################################################################
            viv1 = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendas_ordenadas_temp")
            viv2 = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendas_ordenadas_temp2")

            ids_aeus = [[x[0],x[1],x[2]] for x in arcpy.da.SearchCursor(tb_aeus, ["UBIGEO","ZONA","AEU"],where_seccion)]
            ids_aeus_ord = sorted(ids_aeus, key=lambda tup: tup[2])
            id_aeu_min=ids_aeus_ord[0]
            id_aeu_max=ids_aeus_ord[-1]

            where_viv_min=" UBIGEO='{}' AND ZONA='{}' AND AEU={}".format(id_aeu_min[0],id_aeu_min[1],id_aeu_min[2])
            where_viv_max = " UBIGEO='{}' AND ZONA='{}' AND AEU={}".format(id_aeu_max[0], id_aeu_max[1], id_aeu_max[2])

            where_aeu=expresion.Expresion_2(ids_aeus, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["AEU", "SHORT"]])

            #print where_viv_min
            #print where_viv_max

            viviendas_aeu_min=arcpy.SelectLayerByAttribute_management(viv1, "NEW_SELECTION", where_viv_min)
            viviendas_aeu_max= arcpy.SelectLayerByAttribute_management(viv2, "NEW_SELECTION",where_viv_max)

            list_viv_aeu_min=[ [v[0],v[1],v[2],v[3],v[4],v[5],v[6],v[7],v[8],v[9],v[10],v[11],v[12],v[13]] for v in arcpy.da.SearchCursor(viviendas_aeu_min, ['MANZANA', 'P29', 'P20', 'P21', 'P22_A', 'P23', 'P25', 'P26', 'P27_A', 'P32','P19A', 'FRENTE_ORD', 'P29_A', 'P29_P'])]
            list_viv_aeu_max = [[v[0], v[1], v[2], v[3], v[4], v[5], v[6],v[7], v[8], v[9], v[10], v[11], v[12], v[13]] for v in arcpy.da.SearchCursor(viviendas_aeu_max,['MANZANA', 'P29', 'P20', 'P21', 'P22_A', 'P23', 'P25', 'P26','P27_A', 'P32', 'P19A', 'FRENTE_ORD', 'P29_A', 'P29_P'])]
            primera_puerta=list_viv_aeu_min[0]
            ultima_puerta = list_viv_aeu_max[-1]

            obs = u"<BOL>OBSERVACIONES: </BOL> La seccion N°{} comprende desde: {}{}{}{}{}{}{}{} hasta: {}{}{}{}{}{}{}{}.".format \
                (seccion,
                 u"La  MZ N°{}".format(primera_puerta[0]),
                 u", {} {}".format(primera_puerta[2], primera_puerta[3]),
                 u", N° de puerta {}".format(primera_puerta[4]) if (primera_puerta[4] != " " and  primera_puerta[4] != " ") else "",
                 u", N° de block {}".format(primera_puerta[5]) if (primera_puerta[5] != 0 and  primera_puerta[5] != " ") else "",

                 u", Lote N°{}".format(primera_puerta[6]) if (primera_puerta[6] != 0 and primera_puerta[6] != " ") else "",
                 u", Piso N°{}".format(primera_puerta[7]) if (primera_puerta[7] != 0  and primera_puerta[7] != " ") else "",
                 u", Interior N°{}".format(primera_puerta[8]) if (primera_puerta[8] != 0  and primera_puerta[8] != " ") else "",
                 u", {}".format(primera_puerta[9]),
                 u"La  MZ N°{}".format(ultima_puerta[0]),
                 u", {} {}".format(ultima_puerta[2], ultima_puerta[3]),
                 u", N° de puerta {}".format(ultima_puerta[4]) if (ultima_puerta[4] != 0 and ultima_puerta[4] != " ") else "",
                 u", N° de block {}".format(ultima_puerta[5]) if (ultima_puerta[5] != 0 and ultima_puerta[5] != " ")  else "",
                 u", Lote N°{}".format(ultima_puerta[6]) if (ultima_puerta[6] != 0 and ultima_puerta[6] != " ") else "",
                 u", Piso N°{}".format(ultima_puerta[7]) if (ultima_puerta[7] != 0 and ultima_puerta[7] != " ") else "",
                 u", Interior N°{}".format(ultima_puerta[8]) if (ultima_puerta[8] != 0 and ultima_puerta[8] != " " )else "",
                 u", {}".format(ultima_puerta[9])
                 )

            #########################################Asignado los valores de las variables de los croquis ################################################




            list_text_el = [["COD_BARRA", "*{}*".format(codigo)], ["TEXT_COD_BARRA", "*{}*".format(codigo)], ["CCDD", ccdd],
                            ["CCPP", ccpp], ["CCDI", ccdi], ["CODCCPP", codccpp],
                            ["DEPARTAMENTO", dep], ["PROVINCIA", prov], ["DISTRITO", dist], ["NOMCCPP", nomccpp],
                            ["ZONA", zona_etiqueta], ["SUBZONA", subzona], ["SECCION", seccion],
                            ["AEU_INICIAL", aeu_inicial], ["AEU_FINAL", aeu_final],
                            ["CANT_VIV", cant_viv], ["FRASE", obs]]
            for text_el in list_text_el:
                el = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", text_el[0])[0]
                el.text = text_el[1]
            #######################################Asignamos la escala del dibujo#################################################
            df.extent = mzs_mfl.getSelectedExtent()
            df.scale = df.scale *2
            ########################################Exportamos los croquis y listados####################################################
            out_croquis = '{}\\{}\\{}\\{}.pdf'.format(path_urbano_croquis ,row[0],row[1], codigo)
            out_listado ='{}\\{}\\{}\\{}.pdf'.format(path_urbano_listados ,row[0],row[1], codigo)
            out_final='{}\\{}\\{}\\{}.pdf'.format(path_urbano_croquis_listado ,row[0],row[1], codigo)

            arcpy.mapping.ExportToPDF(mxd, out_croquis, "PAGE_LAYOUT")
            error_export_pdf(path_urbano_croquis, ubigeo, zona, codigo, out_croquis, mxd)
            listado.ListadoSeccion(informacion,out_listado)

            pdfDoc = arcpy.mapping.PDFDocumentCreate(out_final)

            pdfDoc.appendPages(out_croquis)
            pdfDoc.appendPages(out_listado)
            pdfDoc.saveAndClose()

            [cant_pag, nomb_web] = obtener_datos_pdf(out_final)

            #cant_pag += 1
            row[6] = cant_pag
            row[7] = codigo
            row[8] = out_final
            row[9] = nomb_web




            cursor.updateRow(row)
            arcpy.mapping.RemoveLayer(df, lyrFile1)
            del pdfDoc
            del mxd
            del df

def ExportarCroquisUrbanoZona_SubZona(where_expression):
    arcpy.env.workspace =path_ini+""

    with arcpy.da.UpdateCursor(final_subzona,
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


            where_secciones=expresion.Expresion_2(list_secciones, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["SECCION", "SHORT"]])

            list_aeus=[[x[0],x[1],x[2]] for x in arcpy.da.SearchCursor(tb_aeus,['UBIGEO','ZONA','AEU'],where_secciones)]
            where_aeus=expresion.Expresion_2(list_aeus, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["AEU", "SHORT"]])
            list_manzanas = [[x[0], x[1], x[2]] for x in arcpy.da.SearchCursor(tb_rutas, ['UBIGEO', 'ZONA', 'MANZANA'], where_aeus)]

            #list_manzanas = list(set((x[0], x[1], x[2]) for x in
            #                 arcpy.da.SearchCursor(tb_rutas, ['UBIGEO', 'ZONA', 'MANZANA'], where_aeus)))


            where_manzanas = expresion.Expresion_2(list_manzanas, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["MANZANA", "TEXT"]])

            for row4 in arcpy.da.SearchCursor(tb_zonas, ['DEPARTAMEN', 'PROVINCIA', 'DISTRITO', 'NOMCCPP'], where_zona):
                dep = row4[0]
                prov = row4[1]
                dist = row4[2]
                nomccpp = row4[3]
            #########################################Listamos los layers del mxd #####################################
            mxd = arcpy.mapping.MapDocument(path_plantillas_croquis + "/CroquisUrbanoZona.mxd")
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
                for y in  arcpy.da.SearchCursor(tb_rutas,['MANZANA'],"UBIGEO='{}' AND ZONA='{}' AND AEU={}".format(x[0],x[1],x[3])):
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

def ExportarListadoUrbanoDistrito(ubigeos):
    for ubigeo in ubigeos:

        out = path_urbano_croquis_listado + "\\{}\\{}.pdf".format(ubigeo, ubigeo)
        informacion = conx.ObtenerReporteDistrital(ubigeo)[:]
        listado.ListadoDistrito(informacion, out)

def ExportarListadoDeEstudiantes(zonas):
    for el in zonas:
        ubigeo=el[0]
        zona=el[1]

        out_listado = path_urbano_croquis_listado + "\\{}\\{}\\ListadoEstudiantes{}.pdf".format(ubigeo, zona,ubigeo+zona)
        informacion = conx.obtener_informacion_reporte_estudiantes(ubigeo, zona)[:]
        listado.ListadoDeEstudiantes(informacion, out_listado)


def Validadar_Tablas(data=[],campos=["UBIGEO","ZONA"]):
    if len(data)==0:
        data= conx.obtener_lista_zonas_segmentacion(15)[:]
        print data
    if len(data)>0:
        print datetime.today()
    return data

def SegmentacionTabular(data,campos=["UBIGEO","ZONA"]):
    where_expresion=expresion.Expresion(data, campos)
    print datetime.today()
    conx.actualizar_cant_viv_mzs(data, 0)
    conx.ActualizarJefeHogar(data,1)
    print datetime.today()
    ImportarTablasTrabajo(data, campos)
    print "ImportarTablasTrabajo"
    print datetime.today()
    OrdenarManzanasFalsoCod(where_expresion)
    print "OrdenarManzanasFalsoCod"
    print datetime.today()
    CrearViviendasOrdenadas()
    print "CrearViviendasOrdenadas"
    print datetime.today()
    EnumerarAEUEnViviendasDeManzanas(where_expresion)
    print "EnumerarAEUEnViviendasDeManzanas"
    print datetime.today()
    CrearRutas(where_expresion)
    print "CrearRutas"
    print datetime.today()
    CrearAEUS()
    print "CrearAEUS"
    print datetime.today()
    agrupar_aeus_viv_cero()
    print "agrupar_aeus_viv_cero"
    print datetime.today()
    if techo_segunda_pasada>0:
        SegundaPasada()
        print datetime.today()
        CrearAEUSSegundaPasada()
        print datetime.today()
    CrearRutasPuntos()
    print "CrearRutasPuntos"
    CrearPuntosDeCorte()
    print "CrearPuntosDeCorte"
    CrearRutasPreparacion(tb_manzanas_ordenadas)
    print "CrearRutasPreparacion"
    print datetime.today()
    RelacionarRutasLineasConAEU()
    print "RelacionarRutasLineasConAEU"
    print datetime.today()
    EnumerarSecciones()
    print "EnumerarSecciones"
    print datetime.today()
    CrearSecciones()
    print "CrearSecciones"
    print datetime.today()
    CrearSubZonas()
    print "CrearSubZonas"
    print datetime.today()
    SeleccionAEUParaCalidad()
    print "SeleccionAEUParaCalidad"
    print datetime.today()
    SeleccionLegajoParaCalidad()
    print "SeleccionLegajoParaCalidad"
    print datetime.today()
    CrearSitiosInteresPorZonas()
    print "CrearSitiosInteresPorZonas"
    print datetime.today()
    crear_frentes_por_escala()
    print "crear_frentes_por_escala"
    print datetime.today()

    if int(arcpy.GetCount_management(tb_rutas_puntos).getOutput(0))>0:
        ClasificarPuntosIntermedios()
        print "ClasificarPuntosIntermedios"
        print datetime.today()
        CrearLineasMultifamiliar()
        print "CrearLineasMultifamiliar"
        print datetime.today()
        CrearRutasPreparacion(tb_manzanas_final)
        RelacionarRutasLineasConAEU()
        print "RelacionarRutasLineasConAEU"
        ClasificarLineasRutas()
        print "ClasificarLineasRutas"
        ExtenderRutasLineas()
        print "ExtenderRutasLineas"
        print datetime.today()
    print datetime.today()

    list_aeus=[[x[0],x[1],x[2],x[3]] for x in arcpy.da.SearchCursor(tb_aeus,['UBIGEO','ZONA','AEU','CANT_VIV'])]
    list_rutas =[ [x[0],x[1],x[2],x[3],x[4]] for x in  arcpy.da.SearchCursor(tb_rutas, ['UBIGEO', 'ZONA', 'AEU','MANZANA', 'CANT_VIV'])]

    for el in list_aeus:
        print el
    for el in  list_rutas:
        print  el
    preparar_registros()
    print "preparar_registros"


def actualizar_imp_total_zona(ubigeo,zona,flag,fase='CPV2017'):
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

def procesar_merge(data):
    for row in data:
        ubigeo=row[0]
        zona=row[1]

        proceso = subprocess.Popen("python merge_pdfs.py {} {} ".format(ubigeo, zona), shell=True,
                                   stderr=subprocess.PIPE)

        errores = proceso.stderr.read()
        errores_print = '{}'.format(errores)
        print errores_print

        if len(errores_print) > 0 and('PdfReadWarning' not in errores_print):
            print 'algo salido mal'
            actualizar_imp_total_zona(ubigeo,zona,0)

        else:
            print 'nada salio mal'
            actualizar_imp_total_zona(ubigeo, zona, 1)





def ExportarSegmTab(data=[],campos=["UBIGEO","ZONA"]):
    if len(data)>0:
        where_expresion = expresion.Expresion(data, campos)
        CrearCarpetasExportarCroquis(data,campos)
        ExportarCroquisUrbanoAEU(where_expresion)
        ExportarCroquisUrbanoSeccion(where_expresion)
        ExportarCroquisUrbanoZona_SubZona(where_expresion)

        insertar_registros(data)
        print "InsertarRegistros"
        insertar_registros_2(data)
        print "InsertarRegistros2"
        print datetime.today()
        for el in arcpy.da.SearchCursor(tb_zonas, ['UBIGEO', 'ZONA']):
            zona = "{}{}".format(el[0], el[1])
            print 'actualizar_registros'
            conx.ActualizarRegistrosCPVSegmentacion(zona, fase)
        print datetime.today()
        print "ActualizarRegistrosCPVSegmentacion"

        ActualizarCantPaginas(data)
    else:
        "No hay zonas para exportar"

def Segmentacion(data=[],campos=["UBIGEO","ZONA"]):
    data_final=Validadar_Tablas(data)[:]
    if len(data_final)>0:
        SegmentacionTabular(data_final,campos)
        data_excluida=[]

        for el in data:
            if el not in data_final:
                data_excluida.append(el)
        print "Zonas procesadas:", data_final
        print "Zonas excluidas por inconsistencias:", data_excluida if (len(data_excluida)>0) else "Ninguna"

    else:
        "No hay zonas a procesar"
        print "Zonas excluidas por inconsistencias:", data if (len(data) > 0) else "Ninguna"


def CroquisListados(data):
    ExportarSegmTab(data)



datax=[[ubigeox,zonax]]
campos=["UBIGEO","ZONA"]

Segmentacion(datax,campos)
print datetime.today()
ExportarSegmTab(datax,campos)
print datetime.today()
procesar_merge(datax)
print datetime.today()


