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



path_ini = "D:/proyecto-segmentacion-urbana/segmentacion"
tb_frentes = path_ini+"/tb_frentes.shp"
tb_frentes_temp = path_ini+"/tb_frentes_temp.shp"
tb_viviendas_nuevas_temp=path_ini+"/tb_viviendas_nuevas_temp.shp"
tb_viviendas_nuevas=path_ini+"/tb_viviendas_nuevas.shp"
tb_viviendas_nuevas_dbf=path_ini+"/tb_viviendas_nuevas.dbf"



arcpy.env.workspace = path_ini + ""

arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
arcpy.env.overwriteOutput = True

def importar_tablas_trabajo(data, campos):
    arcpy.env.overwriteOutput = True

    db='CPV_SEGMENTACION_GDB'
    ip = '172.18.1.93'
    usuario='sde'
    password='$deDEs4Rr0lLo'
    path_conexion=conx.conexion_arcgis(db,ip,usuario,password)
    arcpy.env.workspace =path_conexion

    temp_ubigeos = ""
    i=0
    for x in data:
        i=i+1
        if (i==1):
            temp_ubigeos="'{}'".format(x[0])
        else:
            temp_ubigeos = "{},'{}'".format(temp_ubigeos,x[0])

    if len(data)>0:
        sql=expresion.Expresion_2(data, campos)
    else:
        sql =' FLAG_NUEVO=1'

    list_capas = [

        ["{}.sde.VW_FRENTES".format(db), tb_frentes_temp, 1],

    ]


    for i,capa in enumerate(list_capas):
        if capa[2] == 1:

            print "select * from {} where {} ".format(capa[0], sql)
            x = arcpy.MakeQueryLayer_management(path_conexion, 'capa{}'.format(i),
                                                    "select * from {} where {} ".format(capa[0], sql ))

        else:
            x = arcpy.MakeQueryTable_management(capa[0], "capa{}".format(i), "USE_KEY_FIELDS", "objectid", "", sql)

        if capa[1] in [tb_frentes_temp]:
            temp = arcpy.CopyFeatures_management(x, 'in_memory/temp_{}'.format(i))
            arcpy.AddField_management(temp, 'MANZANA2', 'TEXT', 50)
            arcpy.CalculateField_management(temp, 'MANZANA2', '!MANZANA!', "PYTHON_9.3")
            arcpy.DeleteField_management(temp, ['MANZANA'])
            arcpy.CopyFeatures_management(temp, capa[1])
            arcpy.AddField_management(capa[1], 'MANZANA', 'TEXT', 50)
            arcpy.CalculateField_management(capa[1], 'MANZANA', '!MANZANA2!', "PYTHON_9.3")
            arcpy.DeleteField_management(capa[1], ['MANZANA2'])
        else:
            arcpy.CopyFeatures_management(x, capa[1])


    arcpy.Sort_management(tb_frentes_temp,tb_frentes,['UBIGEO','ZONA','MANZANA','FRENTE_ORD'])

def crear_viviendas_nuevas():
    arcpy.env.workspace = path_ini

    arcpy.SpatialReference(32718)
    arcpy.CreateFeatureclass_management(path_ini,"tb_viviendas_nuevas_temp.shp","POINT")
    list_fields=[["UBIGEO","TEXT"],["CODCCPP","TEXT"],["ZONA","TEXT"],["MANZANA","TEXT"],["FRENTE_ORD","SHORT"],["ID_REG_OR","SHORT"],["USOLOCAL","TEXT"],["P19A","SHORT"],["P29","SHORT"],["P20","SHORT"],["P21","TEXT"],["P29_A","SHORT"],["FLAG_NUEVO","SHORT"]]

    for field in list_fields:
        if field[0]=='P21':
            arcpy.AddField_management(tb_viviendas_nuevas_temp, field[0], field[1],'','',200)
        else:
            arcpy.AddField_management(tb_viviendas_nuevas_temp ,field[0],field[1] )

    list_manzanas= list(set((x[0],x[1],x[2],x[3]) for x in arcpy.da.SearchCursor(tb_frentes,['UBIGEO','ZONA','MANZANA','CODCCPP'])))

    insertar_cursor = arcpy.da.InsertCursor(tb_viviendas_nuevas_temp,['SHAPE','UBIGEO','ZONA','MANZANA','FRENTE_ORD','ID_REG_OR','USOLOCAL','P29','P19A','P20','P21','P29_A','FLAG_NUEVO','CODCCPP'])

    for manzana in list_manzanas:
        ubigeo=manzana[0]
        zona=manzana[1]
        mz = manzana[2]
        codccpp=manzana[3]
        id_reg_or=1
        tipo_viv=1
        p29=0
        p29_a=''


        for x in arcpy.da.SearchCursor(tb_frentes,['Shape@','FRENTE_ORD','CANT_VIV','CAT_VIA','NOM_VIA'],u"UBIGEO='{}' and ZONA='{}' and MANZANA='{}' ".format(ubigeo,zona,mz) ):
            frente=int(x[1])
            cant_viv=int(x[2])
            tam=x[0].length
            p20=int(x[3])
            p21 = x[4]

            if cant_viv==0:
                tam_pieza=tam/2.0
                point=x[0].positionAlongLine(tam_pieza)
                tipo_viv=5
                p29=tipo_viv
                p19a=id_reg_or
                insertar_cursor.insertRow([point,ubigeo,zona,mz,frente,id_reg_or,tipo_viv,p29,p19a,p20,p21,5,1,codccpp])

                id_reg_or=id_reg_or+1

            else:

                tam_pieza = tam / (cant_viv + 1)

                for i in range(cant_viv):
                    point = x[0].positionAlongLine((i + 1) * tam_pieza)
                    tipo_viv = 1
                    p29 = tipo_viv
                    p19a = id_reg_or
                    insertar_cursor.insertRow([point, ubigeo, zona, mz,frente, id_reg_or, tipo_viv, p29, p19a, p20, p21,0,1,codccpp])
                    id_reg_or = id_reg_or + 1

    arcpy.SpatialReference(4326)
    arcpy.CopyFeatures_management(tb_viviendas_nuevas_temp,tb_viviendas_nuevas)

def insertar_registros():
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
    db='CPV_SEGMENTACION_GDB'
    ip = '172.18.1.93'
    usuario='sde'
    password='$deDEs4Rr0lLo'
    path_conexion=conx.conexion_arcgis(db,ip,usuario,password)
    arcpy.env.workspace =path_conexion


    segm_vivienda_u_nuevo=path_conexion +"/{db}.SDE.TB_CPV0301_VIVIENDA_U_NUEVO".format(db=db)
    segm_vivienda_u=path_conexion + " /{db}.SDE.CARTO_EDIT/{db}.SDE.TB_VIVIENDA_U_NUEVO".format(db=db)




    list_capas = [
        [tb_viviendas_nuevas_dbf,segm_vivienda_u_nuevo,2],
        [tb_viviendas_nuevas, segm_vivienda_u,1],


    ]



    list_zonas = list(
        set((x[0], x[1]) for x in arcpy.da.SearchCursor(tb_frentes, ['UBIGEO', 'ZONA'])))


    sql=u'{}'.format(expresion.expresion_sql(data=list_zonas,campos=[["UBIGEO","TEXT"],["ZONA","TEXT"]]))

    conn = conx.Conexion2()
    cursor = conn.cursor()


    sql_query = u"""
                DELETE {db}.SDE.TB_CPV0301_VIVIENDA_U_NUEVO where {sql}
                DELETE {db}.SDE.TB_VIVIENDA_U_NUEVO where {sql}
                """.format(db=db,sql=sql)
    print sql_query
    cursor.execute(sql_query)
    conn.commit()



    conn.close()

    i=0
    for el in list_capas:
        print el[1]

        i = i + 1

        if (int(el[2])>1):
            a = arcpy.MakeTableView_management(el[0], "a{}".format(i), )



        else:
            a = arcpy.MakeFeatureLayer_management(el[0], "a{}".format(i))


        arcpy.Append_management(a, el[1], "NO_TEST")


def generar(data=[["230110","01001",1]], campos=[["UBIGEO","TEXT"],["ZONA","TEXT"],["FLAG_NUEVO","SHORT"]]):
    importar_tablas_trabajo(data=data, campos=campos)
#importar_tablas_trabajo(data=[["15",1]], campos=[["CCDD","TEXT"],["FLAG_NUEVO","SHORT"]])
    crear_viviendas_nuevas()
    insertar_registros()







