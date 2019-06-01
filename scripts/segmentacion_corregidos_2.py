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
#fase='{}'.format(sys.argv[3])
tipo='{}'.format(sys.argv[3])




#aeux=''
fase='CPV2017'

#if len(sys.argv)>3:
#    aeux='{}'.format(sys.argv[4])



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
    database='CPV_SEGMENTACION_GDB'
    ip = '172.18.1.93'
    usuario='sde'
    password='$deDEs4Rr0lLo'
    path_conexion=conx.conexion_arcgis(database,ip,usuario,password)
    arcpy.env.workspace =path_conexion

    temp_ubigeos = ""
    i=0
    for x in data:
        i=i+1
        if (i==1):
            temp_ubigeos="'{}'".format(x[0])
        else:
            temp_ubigeos = "{},'{}'".format(temp_ubigeos,x[0])

    sql=expresion.Expresion(data, campos)

    list_capas=[
                ["{}.sde.TB_EJE_VIAL".format(database), tb_ejes_viales, 1],
                ["{}.sde.VW_ZONA_CENSAL".format(database), tb_zonas, 1],
                ["{}.sde.VW_FRENTES".format(database), tb_frentes, 1],
                ["{}.sde.TB_MANZANA".format(database), tb_manzanas, 1],
                ["{}.sde.SEGM_U_AEU".format(database),tb_aeus,2],
                ["{}.sde.SEGM_U_RUTA".format(database), tb_rutas, 2],
                ["{}.sde.SEGM_U_SUBZONA".format(database), tb_subzonas, 2],
                ["{}.sde.SEGM_RUTAS_LINEAS".format(database), tb_rutas_lineas, 1],
                ["{}.sde.SEGM_RUTAS_LINEAS_MULTIFAMILIAR".format(database), tb_rutas_lineas_multifamiliar, 1],
                ["{}.sde.SEGM_FRENTES_1".format(database), tb_frentes_1, 1],
                ["{}.sde.SEGM_FRENTES_2".format(database), tb_frentes_2, 1],
                ["{}.sde.SEGM_FRENTES_3".format(database), tb_frentes_3, 1],
                ["{}.sde.SEGM_U_SECCION".format(database), tb_secciones, 1],

                ["{}.sde.SEGM_SITIOS_INTERES".format(database), tb_sitios_interes, 1],
                ["{}.sde.SEGM_RUTAS_PUNTOS".format(database), tb_rutas_puntos, 1],
                ]


    for i,capa in enumerate(list_capas):
        print capa
        if capa[2] == 1:
            if capa[1] in [tb_manzanas,tb_sitios_interes,tb_ejes_viales]:

                if capa[1] == tb_sitios_interes:
                    x = arcpy.MakeQueryLayer_management(path_conexion, 'capa{}'.format(i),
                                                        "select * from {} where ubigeo in ({}) AND (CODIGO<91 AND CODIGO<>26) ".format(
                                                            capa[0],
                                                            temp_ubigeos))
                else:
                    x = arcpy.MakeQueryLayer_management(path_conexion, 'capa{}'.format(i),
                                                        "select * from {} where ubigeo in ({})  ".format(capa[0],
                                                                                                         temp_ubigeos))

            else:
                #print "select * from {} where {} ".format(capa[0], sql)
                x = arcpy.MakeQueryLayer_management(path_conexion, 'capa{}'.format(i),
                                                    "select * from {} where {} ".format(capa[0], sql ))

        else:
            x = arcpy.MakeQueryTable_management(capa[0], "capa{}".format(i), "USE_KEY_FIELDS", "objectid", "", sql)



        if capa[1] in [tb_manzanas,tb_frentes,tb_rutas,tb_rutas_puntos]:
            if capa[2]==1:
                temp = arcpy.CopyFeatures_management(x, "in_memory\\temp_{}".format(i))
                arcpy.AddField_management(temp, 'MANZANA2', 'TEXT', 50)
                arcpy.CalculateField_management(temp, 'MANZANA2', '!MANZANA!', "PYTHON_9.3")
                arcpy.DeleteField_management(temp, ['MANZANA'])
                arcpy.CopyFeatures_management(temp, capa[1])
                arcpy.AddField_management(capa[1], 'MANZANA', 'TEXT', 50)
                arcpy.CalculateField_management(capa[1], 'MANZANA', '!MANZANA2!', "PYTHON_9.3")
                arcpy.DeleteField_management(capa[1], ['MANZANA2'])
            else:
                temp = arcpy.CopyRows_management(x, "in_memory\\temp_{}".format(i))
                arcpy.AddField_management(temp, 'MANZANA2', 'TEXT', 50)
                arcpy.CalculateField_management(temp, 'MANZANA2', '!MANZANA!', "PYTHON_9.3")
                arcpy.DeleteField_management(temp, ['MANZANA'])
                arcpy.CopyRows_management(temp, capa[1])
                arcpy.AddField_management(capa[1], 'MANZANA', 'TEXT', 50)
                arcpy.CalculateField_management(capa[1], 'MANZANA', '!MANZANA2!', "PYTHON_9.3")
                arcpy.DeleteField_management(capa[1], ['MANZANA2'])

        else:


            if capa[2] > 1:
                arcpy.CopyRows_management(x, capa[1])
            else:
                arcpy.CopyFeatures_management(x, capa[1])
    arcpy.env.workspace = path_ini + ""



def viviendas(data,campos):
    sql = expresion.Expresion(data, campos)

    conn = conx.Conexion2()
    cursor = conn.cursor()

    sql_query = """select  * from sde.VW_VIVIENDAS_U_CORREGIDO WHERE {} ORDER BY UBIGEO,ZONA,AEU,FALSO_COD,ID_REG_OR """.format(sql)

    cursor.execute(sql_query)

    list_viv = []
    for row in cursor:
        list_viv.append(row)

    #conn.commit()

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


            print where_aeu
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

            print ' cantidad de viviendas:', len(data)


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



            if df.scale >= 1300.0:

                if (df.scale >= 1300.0 and df.scale < 1500.0):
                    d = 5.5
                elif (df.scale >= 1500.0 and df.scale < 2000.0):
                    d = 7
                elif (df.scale >= 2000.0 and df.scale < 3000.0):
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
            if ((df.scale >= 1300.0) and arcpy.Exists(tb_rutas_lineas_multifamiliar) and int(arcpy.GetCount_management(tb_rutas_lineas_multifamiliar)[0] )>0 ):
                arcpy.mapping.RemoveLayer(df, lyrFile2)

            del pdfDoc
            del df
            del mxd




def exportar_croquis_seccion_corregidos(where_expression=''):
    arcpy.env.workspace =path_ini+""

    with arcpy.da.UpdateCursor(tb_secciones,
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



            where_seccion = " UBIGEO='{}' AND ZONA='{}' AND SECCION='{}'".format(ubigeo, zona, seccion)
            where_zona = " UBIGEO='{}' AND ZONA='{}' ".format(ubigeo, zona)
            list_aeus=[[x[0],x[1],x[2]] for x in arcpy.da.SearchCursor(tb_aeus,['UBIGEO','ZONA','AEU'],where_seccion)]
            where_aeus=expresion.Expresion_2(list_aeus, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["AEU", "TEXT"]])
            #list_manzanas = [[x[0], x[1], x[2]] for x in arcpy.da.SearchCursor(tb_rutas, ['UBIGEO', 'ZONA', 'MANZANA'], where_aeus)]
            list_manzanas = list(set((x[0], x[1], x[2]) for x in
                                     arcpy.da.SearchCursor(tb_rutas, ['UBIGEO', 'ZONA', 'MANZANA'], where_aeus)))

            arcpy.MakeFeatureLayer_management(tb_sitios_interes, "sitios_interes")



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
            mxd = arcpy.mapping.MapDocument(path_plantillas_croquis + "/CroquisUrbanoSeccionCorregido.mxd")
            df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]


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
                for y in  arcpy.da.SearchCursor(tb_rutas,['MANZANA'],"UBIGEO='{}' AND ZONA='{}' AND AEU='{}'".format(x[0],x[1],x[3])):
                    i=i+1
                    if (i==1):
                        mzs=u"{}".format(y[0])
                    else:
                        mzs = u"{}-{}".format(mzs,y[0])
                data.append([u'{}'.format(x[3]).zfill(3),mzs,x[4]])
            informacion=[cabecera,data]
            #####################################Creacion de la obs####################################################################################
            #viv1 = arcpy.MakeTableView_management(tb_viviendas_ordenadas, "viviendas_ordenadas_temp")
            #viv2 = arcpy.MakeTableView_management(tb_viviendas_ordenadas, "viviendas_ordenadas_temp2")

            ids_aeus = [[x[0],x[1],x[2]] for x in arcpy.da.SearchCursor(tb_aeus, ["UBIGEO","ZONA","AEU"],where_seccion)]
            ids_aeus_ord = sorted(ids_aeus, key=lambda tup: tup[2])
            id_aeu_min=ids_aeus_ord[0]
            id_aeu_max=ids_aeus_ord[-1]

            where_viv_min=" UBIGEO='{}' AND ZONA='{}' AND AEU='{}'".format(id_aeu_min[0],id_aeu_min[1],id_aeu_min[2])
            where_viv_max = " UBIGEO='{}' AND ZONA='{}' AND AEU='{}'".format(id_aeu_max[0], id_aeu_max[1], id_aeu_max[2])

            where_aeu=expresion.Expresion_2(ids_aeus, [["UBIGEO", "TEXT"], ["ZONA", "TEXT"], ["AEU", "TEXT"]])

            #viviendas_aeu_min=arcpy.SelectLayerByAttribute_management(viv1, "NEW_SELECTION", where_viv_min)
            #viviendas_aeu_max= arcpy.SelectLayerByAttribute_management(viv2, "NEW_SELECTION",where_viv_max)


            list_viv_aeu_min = [[v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9]]
                                for v in arcpy.da.SearchCursor(tb_viviendas_ordenadas,
                                                               ['MANZANA', 'OR_VIV_AEU', 'P20', 'P21', 'P22_A', 'P23', 'P25',
                                                                'P26', 'P27_A', 'P32'],where_viv_min

                                                               )]



            #["OR_VIV_AEU", "MANZANA", "ID_REG_OR", "FRENTE_ORD", "P20", "P21", "P22_A", "P23",
            #"P24",
            #"P25", "P26", "P27_A", "P28", "P32"]



            list_viv_aeu_max = [[v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9]]
                                for v in arcpy.da.SearchCursor(tb_viviendas_ordenadas,
                                                               ['MANZANA', 'OR_VIV_AEU', 'P20', 'P21', 'P22_A', 'P23', 'P25',
                                                                'P26', 'P27_A', 'P32'],where_viv_max)


                                ]
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
            #arcpy.mapping.RemoveLayer(df, lyrFile1)
            del pdfDoc
            del mxd
            del df






def exportar_croquis_urbano_zona_subzona_corregidos(where_expression=''):
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

        path_urbano_croquis_listado,

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
                if os.path.exists(path + "\\" + str(row[0]) ) == False:
                    os.mkdir(path + "\\" + str(row[0]))

                if os.path.exists(path + "\\" + str(row[0]) + "\\" + str(row[1])) == False:
                    os.mkdir(path + "\\" + str(row[0]) + "\\" + str(row[1]))



def procesar_merge(data):
    for row in data:
        ubigeo=row[0]
        zona=row[1]

        proceso = subprocess.Popen("python d:\Dropbox\scripts\merge_pdfs.py {} {} ".format(ubigeo, zona), shell=True,
                                   stderr=subprocess.PIPE)

        errores = proceso.stderr.read()
        errores_print = '{}'.format(errores)
        print errores_print

        if len(errores_print) > 0 and('PdfReadWarning' not in errores_print):
            print 'algo salido mal'
            conx.actualizar_imp_total_zona(ubigeo,zona,0)

        else:
            print 'nada salio mal'
            conx.actualizar_imp_total_zona(ubigeo, zona, 1)


data=[[ubigeox,zonax]]
campos=['UBIGEO','ZONA']



where1 = expresion.Expresion(data=[[ubigeox,zonax]], campos=['UBIGEO','ZONA'])

importar_tablas_corregidas(data=data, campos=campos)
viviendas(data=data,campos=campos)
ordenar_manzanas_cod_falso(where_expression=where1)
crear_carpetas_exportacion(data=data, campos=campos)




print tipo

if tipo=='1':

    exportar_croquis_aeu_corregidos(where_expression=where1)
    exportar_croquis_seccion_corregidos(where_expression=where1)
    exportar_croquis_urbano_zona_subzona_corregidos(where_expression=where1)
    procesar_merge(data)

elif tipo=='2':
    exportar_croquis_seccion_corregidos(where_expression=where1)
    exportar_croquis_urbano_zona_subzona_corregidos(where_expression=where1)
    procesar_merge(data)

else:
    exportar_croquis_urbano_zona_subzona_corregidos(where_expression=where1)
    procesar_merge(data)


