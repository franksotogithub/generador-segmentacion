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
aeux='000'


if len(sys.argv)>2:
    aeux='{}'.format(sys.argv[3])



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


    sql=expresion.Expresion(data, campos)
    manzanas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_MZS',"SELECT geom,UBIGEO,CODCCPP,ZONA,MANZANA,VIV_MZ,FALSO_COD,MZS_COND,CANT_BLOCK FROM TB_MANZANA WHERE UBIGEO IN ({}) ".format(temp_ubigeos))
    zonas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.VW_ZONA_CENSAL',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_ZONA_CENSAL WHERE {} ".format(sql))
    eje_vial_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.TB_EJE_VIAL',"SELECT * FROM CPV_SEGMENTACION.dbo.TB_EJE_VIAL where UBIGEO IN  ({}) ".format(temp_ubigeos))
    frentes_mfl = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.TB_FRENTES',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_FRENTES WHERE {} ".format(sql))
    manzanas_mfl = arcpy.MakeFeatureLayer_management(manzanas_Layer,"manzanas_mfl")
    zonas_mfl=arcpy.MakeFeatureLayer_management(zonas_Layer, "zonas_mfl")
    eje_vial_mfl = arcpy.MakeFeatureLayer_management(eje_vial_Layer, "eje_vial_mfl")
    list_mfl=[[manzanas_mfl,tb_manzanas],[frentes_mfl,tb_frentes]]

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

                ["{}.sde.SEGM_RUTAS_LINEAS".format(database), tb_rutas_lineas, 1],
                ["{}.sde.SEGM_RUTAS_LINEAS_MULTIFAMILIAR".format(database), tb_rutas_lineas_multifamiliar, 1],
                ["{}.sde.SEGM_FRENTES_1".format(database), tb_frentes_1, 1],
                ["{}.sde.SEGM_FRENTES_2".format(database), tb_frentes_2, 1],
                ["{}.sde.SEGM_FRENTES_3".format(database), tb_frentes_3, 1],
                ["{}.sde.SEGM_SECCION".format(database), tb_secciones, 1],
                ["{}.sde.SEGM_SITIOS_INTERES".format(database), tb_sitios_interes, 1],
                ["{}.sde.VW_VIVIENDAS_U".format(database), tb_viviendas_ordenadas_temp, 2],
                ["{}.sde.SEGM_RUTAS_PUNTOS".format(database), tb_rutas_puntos, 1]
                ]

    for i,capa in enumerate(list_capas):

        if capa[2]==1:
            #print 'aqui'

            if capa[1]==tb_viviendas_ordenadas_temp:
                x = arcpy.MakeQueryLayer_management(path_conexion2, 'capa{}'.format(i),"select * from {} where {}".format(capa[0], sql))

            else:
                x = arcpy.MakeQueryLayer_management(path_conexion2, 'capa{}'.format(i),"select * from {} where {} ".format(capa[0],sql))

        else:
            x = arcpy.MakeQueryTable_management(capa[0], "capa{}".format(i), "USE_KEY_FIELDS", "objectid", "", sql)


        if capa[1] in (tb_rutas,tb_rutas_puntos,tb_viviendas_ordenadas_temp):
            #####tratamiento del campo manzana para las Ñ #####

            if capa[1]in (tb_rutas,tb_viviendas_ordenadas_temp) :
                temp = arcpy.CopyRows_management(capa[0], 'in_memory/temp_m{}'.format(i))
            else:
                temp = arcpy.CopyFeatures_management(capa[0], 'in_memory/temp_m{}'.format(i))


            arcpy.AddField_management(temp, 'MANZANA2', 'TEXT', 50)
            arcpy.CalculateField_management(temp, 'MANZANA2', '!MANZANA!', "PYTHON_9.3")
            arcpy.DeleteField_management(temp, ['MANZANA'])

            #####copiando archivos

            if capa[1] in (tb_rutas,tb_viviendas_ordenadas_temp):
                temp = arcpy.CopyRows_management(temp, capa[1])
            else:
                temp = arcpy.CopyFeatures_management(temp, capa[1])

            arcpy.AddField_management(capa[1], 'MANZANA', 'TEXT', 50)
            arcpy.CalculateField_management(capa[1], 'MANZANA', '!MANZANA2!', "PYTHON_9.3")
            arcpy.DeleteField_management(capa[1], ['MANZANA2'])

            ########tratamiento del campo aeu para que tenga tres digitos #####
            arcpy.AddField_management(capa[1], 'AEU2', 'TEXT', 3)
            arcpy.CalculateField_management(capa[1], 'AEU2', 'str(!AEU!).zfill(3)', "PYTHON_9.3")
            arcpy.DeleteField_management(capa[1], ['AEU'])
            arcpy.AddField_management(capa[1], 'AEU', 'TEXT', 3)
            arcpy.CalculateField_management(capa[1], 'AEU', '!AEU2!', "PYTHON_9.3")
            arcpy.DeleteField_management(capa[1], ['AEU2'])
        else:

            if capa[2] > 1:
                arcpy.CopyRows_management(x, capa[1])
            else:
                arcpy.CopyFeatures_management(x, capa[1])



    arcpy.Sort_management(tb_viviendas_ordenadas_temp,tb_viviendas_ordenadas,['UBIGEO','ZONA','AEU','FALSO_COD','ID_REG_OR'])




def obtener_datos_pdf_con_error(nom_pdf):

    #input= os.path.join(path_croquis_listado,nom_pdf)
    input=nom_pdf


    out=os.path.join(path_urbano_listados,nom_pdf.split("\\")[-1])
    pdf = pyPdf.PdfFileReader(open(input, "rb"))
    pdf_listado=pyPdf.PdfFileWriter()
    cant_pag = pdf.getNumPages()
    for i in range(1,cant_pag+1):
        p=pdf.getPage(i)
        pdf_listado.addPage(p)

    with open(out, 'wb') as f:
        pdf_listado.write(f)



def ordenar_manzanas_cod_falso(where_expression):
    manzanas_selecc= arcpy.Select_analysis(tb_manzanas, "in_memory//manzanas_selecc", where_expression)
    manzanas_ordenadas=arcpy.Sort_management(manzanas_selecc, tb_manzanas_ordenadas, ["UBIGEO", "ZONA", "FALSO_COD"])
    expression = "flg_manzana(!VIV_MZ!)"

    arcpy.AddField_management(manzanas_ordenadas, "FLG_MZ", "SHORT")


def exportar_croquis_aeu_corregidos(where_expression=''):
    arcpy.env.workspace = path_ini + ""
    with arcpy.da.UpdateCursor(tb_aeus, ["UBIGEO", "ZONA","CODCCPP" ,"SECCION", "AEU", "CANT_VIV",'CANT_PAG','COD_CROQ' ,'RUTA_CROQ',
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
            where_aeu = "UBIGEO ='{}' AND ZONA='{}' AND AEU='{}'".format(ubigeo, zona, aeu)
            where_seccion = "UBIGEO='{}' AND ZONA='{}' AND SECCION='{}'".format(ubigeo, zona, seccion)
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
            mxd = arcpy.mapping.MapDocument(path_plantillas_croquis + "/CroquisUrbanoAEUCorregido.mxd")
            df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]

            arcpy.MakeFeatureLayer_management(tb_sitios_interes, "sitios_interes")

            lyrFile1 = arcpy.mapping.Layer("sitios_interes")
            arcpy.ApplySymbologyFromLayer_management(lyrFile1, path_plantillas_layers + "/sitios_interes.lyr")
            arcpy.mapping.AddLayer(df, lyrFile1)

            rutas_lineas_mfl = arcpy.mapping.ListLayers(mxd, "TB_RUTAS_LINEAS")[0]
            rutas_lineas_mfl.definitionQuery = where_aeu
            manzanas_mfl = arcpy.mapping.ListLayers(mxd, "TB_MZS_ORD")[0]
            manzanas_mfl.definitionQuery = where_manzanas



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
                if arcpy.Exists(tb_rutas_lineas_multifamiliar) and int(arcpy.GetCount_management(tb_rutas_lineas_multifamiliar)[0])>0:
                    tb_multifamiliar_temp = cambiar_longitud_lineas_multifamiliar(tb_rutas_lineas_multifamiliar, d)
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


            print out_final
            arcpy.mapping.ExportToPDF(mxd, out_croquis, data_frame="PAGE_LAYOUT", resolution=300)
            error_export_pdf(path_urbano_croquis, ubigeo, zona, codigo, out_croquis, mxd)
            obtener_datos_pdf_con_error(out_final)


            pdfDoc = arcpy.mapping.PDFDocumentCreate(out_final)
            pdfDoc.appendPages(out_croquis)
            pdfDoc.appendPages(out_listado)

            pdfDoc.saveAndClose()




            arcpy.mapping.RemoveLayer(df, lyrFile1)
            if ((df.scale > 2000.0) and arcpy.Exists(tb_rutas_lineas_multifamiliar) and int(arcpy.GetCount_management(tb_rutas_lineas_multifamiliar)[0] )>0 ):
                arcpy.mapping.RemoveLayer(df, lyrFile2)

            del pdfDoc
            del df
            del mxd

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


def cambiar_longitud_lineas_multifamiliar(tb_rutas_lineas_multifamiliar_mfl, d):
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
where2 = expresion.Expresion(data=[[ubigeox,zonax,aeux]], campos=['UBIGEO','ZONA','AEU'])
importar_tablas_corregidas(data=data, campos=campos)
ordenar_manzanas_cod_falso(where_expression=where1)
crear_carpetas_exportacion(data=data,campos=campos)
exportar_croquis_aeu_corregidos(where_expression=where2)