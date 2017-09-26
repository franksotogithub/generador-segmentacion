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
from datetime import *
import subprocess

#import  EtiquetasUrbano as etiquetas
from itertools import groupby
import pyPdf
import sys

fase='CPV2017'
path_proyecto_segm = "D:/proyecto-segmentacion-urbana/"

path_ini = "D:/proyecto-segmentacion-urbana/segmentacion-2"
#path_out = "\\\srv-fileserver\\CPV2017"
path_out = "D:"
path_croquis=path_out + "\\croquis"
path_listados=path_out + "\\listados"
path_croquis_listado=path_out + "\\croquis-listado"
path_etiquetas=path_out + "\\etiquetas"

path_out_final="\\\\192.168.201.115\\cpv2017"


path_urbano_croquis = path_out + "\\croquis\\urbano"
path_urbano_listados = path_out + "\\listados\\urbano"
#path_urbano_croquis_listado = path_out_final + "\\croquis-listado\\urbano"
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
tb_secciones = path_ini + "/tb_secciones.shp"
tb_frentes_1 = path_ini+"/tb_frentes_1.shp"
tb_frentes_2 = path_ini+"/tb_frentes_2.shp"

arcpy.env.workspace = path_ini + ""
arcpy.env.overwriteOutput = True

tb_subzonas = path_ini + "/tb_subzonas.dbf"
final_subzona=path_ini + "/final_tb_subzonas.dbf"
final_aeu=path_ini+'/final_tb_aeus.dbf'
final_seccion=path_ini+'/final_tb_secciones.shp'


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


def ExportarCroquisUrbanoAEU(where_expression=''):
    arcpy.env.workspace = path_ini + ""
    with arcpy.da.UpdateCursor(final_aeu, ["UBIGEO", "ZONA","CODCCPP" ,"SECCION", "AEU", "CANT_VIV",'CANT_PAG','COD_CROQ' ,'RUTA_CROQ',
                                           'RUTA_WEB']) as cursor:


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
            mxd = arcpy.mapping.MapDocument(path_plantillas_croquis + "/CroquisUrbanoAEU2.mxd")
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

            # for manzana in list_manzanas:
            #    data_x = [[v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], v[11], v[12], v[13]] for v in
            #            arcpy.da.SearchCursor(tb_viviendas_ordenadas,
            #                                  ["OR_VIV_AEU", "MANZANA", "ID_REG_OR", "FRENTE_ORD", "P20", "P21", "P22_A",
            #                                   "P23", "P24",
            #                                   "P25", "P26", "P27_A", "P28", "P32"], "{} AND MANZANA='{}'".format(where_aeu,manzana[2])) ]
            #
            #    list_data.append(data_x)


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
                    d = 7
                elif (df.scale >= 3000.0 and df.scale < 4000.0):
                    d = 8.5
                elif (df.scale >= 4000.0 and df.scale < 5000.0):
                    d = 10
                elif (df.scale >= 5000.0 and df.scale < 6000.0):
                    d = 12
                else:
                    d = 15
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

            [cant_pag,nomb_web]=obtener_datos_pdf(out_final)

            cant_pag+=1
            row[6]=cant_pag
            row[7]=codigo
            row[8]=out_final
            row[9]=nomb_web


            print out_final
            pdfDoc = arcpy.mapping.PDFDocumentCreate(out_final)
            pdfDoc.appendPages(out_croquis)
            pdfDoc.appendPages(out_listado)

            pdfDoc.saveAndClose()

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
                # print archivo
                l = len(archivo)
                cod = archivo[0:l - 4]
                nom_pdf = "{}\\{}".format(dir_zona, archivo)
                pdf = pyPdf.PdfFileReader(open(nom_pdf, "rb"))
                cant_pag = pdf.getNumPages()

                list_web = nom_pdf.split("\\")[3:]
                nom_web=""

                for i in list_web:
                    nom_web=nom_web+'/'+i
                #print nom_web

                nom_web=nom_web.replace("\\","/")

                #print nom_web
                conx.ActualizarCantidadPaginas(cod, cant_pag, nom_pdf, nom_web)


#data=[['150604','00500']]
#ActualizarCantPaginas(data)
def frentes():

    list_frentes= [ [x[0],x[1],x[2],int(x[3])] for x in  arcpy.da.SearchCursor(tb_frentes,['UBIGEO','ZONA','MANZANA','FRENTE_ORD'])]
    puntos=arcpy.FeatureToPoint_management(tb_frentes, 'in_memory/puntos', "INSIDE")
    puntos_buffer=arcpy.Buffer_analysis(puntos,'in_memory/buffer_puntos','3 meters')

    frente_lyr=arcpy.MakeFeatureLayer_management(tb_frentes,'frente_lyr')

    buffer_lyr=arcpy.MakeFeatureLayer_management(puntos_buffer,'buffer_lyr')

    list_cortes=[]

    for x in list_frentes:
        frente_lyr=arcpy.SelectLayerByAttribute_management(frente_lyr,"NEW_SELECTION","UBIGEO='{}' AND ZONA='{}' AND MANZANA='{}' AND FRENTE_ORD={}".format(x[0],x[1],x[2],x[3]) )
        buffer_lyr=arcpy.SelectLayerByAttribute_management(buffer_lyr,"NEW_SELECTION","UBIGEO='{}' AND ZONA='{}' AND MANZANA='{}' AND FRENTE_ORD={}".format(x[0],x[1],x[2],x[3]) )
        clip=arcpy.Clip_analysis(frente_lyr,buffer_lyr,'in_memory\\clip{}{}{}{}'.format(x[0],x[1],x[2],x[3]))
        list_cortes.append(clip)

    arcpy.Merge_management(list_cortes, tb_frentes_1)





def actualizar_capas():
    list_capas=[final_aeu,final_seccion,final_subzona]
    for el in list_capas:


        arcpy.AddField_management(el,'CANT_PAG','SHORT')
        arcpy.AddField_management(el, 'COD_CROQ', 'TEXT')
        arcpy.AddField_management(el, 'RUTA_CROQ', 'TEXT')
        arcpy.AddField_management(el, 'RUTA_WEB', 'TEXT')

    with arcpy.da.UpdateCursor(final_aeu, ['UBIGEO', 'ZONA', 'SUBZONA', 'CANT_PAG', 'COD_CROQ', 'RUTA_CROQ',
                                             'RUTA_WEB']) as cursor:
        for x in cursor:
            ubigeo = '{}'.format(x[0])
            zona = '{}'.format(x[1])
            cod_croquis = '{}{}{}'.format(x[0], x[1], x[2])
            nom_pdf = '{}\\{}\\{}\\{}.pdf'.format(path_urbano_croquis_listado, ubigeo, zona, cod_croquis)
            [cant_pag, nom_web] = obtener_datos_pdf(nom_pdf)

            x[3] = cant_pag
            x[4] = cod_croquis
            x[5] = nom_pdf
            x[6] = nom_web

            cursor.updateRow(x)



    with arcpy.da.UpdateCursor(final_seccion, ['UBIGEO', 'ZONA', 'SUBZONA', 'CANT_PAG', 'COD_CROQ', 'RUTA_CROQ',
                                             'RUTA_WEB']) as cursor:
        for x in cursor:
            ubigeo = '{}'.format(x[0])
            zona = '{}'.format(x[1])
            cod_croquis = '{}{}{}'.format(x[0], x[1], x[2])
            nom_pdf = '{}\\{}\\{}\\{}.pdf'.format(path_urbano_croquis_listado, ubigeo, zona, cod_croquis)
            [cant_pag, nom_web] = obtener_datos_pdf(nom_pdf)

            x[3] = cant_pag
            x[4] = cod_croquis
            x[5] = nom_pdf
            x[6] = nom_web

            cursor.updateRow(x)

    with arcpy.da.UpdateCursor(final_subzona, ['UBIGEO','ZONA','SUBZONA','CANT_PAG','COD_CROQ','RUTA_CROQ','RUTA_WEB']) as cursor:
        for x in cursor:
            ubigeo='{}'.format(x[0])
            zona = '{}'.format(x[1])
            cod_croquis='{}{}{}'.format(x[0],x[1],x[2])
            nom_pdf='{}\\{}\\{}\\{}.pdf'.format(path_urbano_croquis_listado,ubigeo,zona,cod_croquis)
            [cant_pag,nom_web]=obtener_datos_pdf(nom_pdf)

            x[3]=cant_pag
            x[4]=cod_croquis
            x[5]=nom_pdf
            x[6]=nom_web

            cursor.updateRow(x)




            #list_dir_zonas = ['{}\\{}\\{}'.format(path_urbano_croquis_listado, x[0], x[1]) for x in data]


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
















'''ExportarCroquisUrbanoAEU()'''
'''
def actualizar_paginas(data):
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


'''