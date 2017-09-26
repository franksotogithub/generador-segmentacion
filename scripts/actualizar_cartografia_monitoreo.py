
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


path_proyecto="D:/proyecto-segmentacion-urbana"
path_actualizar=os.path.join(path_proyecto,"actualizar-cartografia")
tb_viviendas="{}/{}".format(path_actualizar,"tb_viv.shp")
tb_eje_vial="{}/{}".format(path_actualizar,"tb_eje_vial.shp")
tb_manzanas="{}/{}".format(path_actualizar,"tb_manzanas.shp")
tb_zonas="{}/{}".format(path_actualizar,"tb_zonas.shp")
tb_ccpp="{}/{}".format(path_actualizar,"tb_ccpp.shp")


def importar_tablas(where_list):
    arcpy.env.overwriteOutput = True


    if arcpy.Exists("CPV_SEGMENTACION.sde") == False:
        arcpy.CreateDatabaseConnection_management("Database Connections",
                                                  "CPV_SEGMENTACION.sde",
                                                  "SQL_SERVER",
                                                  "172.18.1.93",
                                                  "DATABASE_AUTH",
                                                  "us_arcgis_seg_2",
                                                  "MBs0p0rt301",
                                                  "#",
                                                  "CPV_SEGMENTACION",
                                                  "#",
                                                  "#",
                                                  "#",
                                                  "#")
    arcpy.env.workspace = "Database Connections/CPV_SEGMENTACION.sde"
    path_conexion="Database Connections/CPV_SEGMENTACION.sde"




    manzanas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_MZS',"SELECT geom,UBIGEO,CODCCPP,ZONA,MANZANA,VIV_MZ,FALSO_COD,MZS_COND,CANT_BLOCK FROM TB_MANZANA WHERE {} ".format(where_list))
    zonas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_ZONA',"SELECT * FROM CPV_SEGMENTACION.dbo.TB_ZONA WHERE {} ".format(where_list))
    eje_vial_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_EJE_VIAL',
                                                     "SELECT * FROM CPV_SEGMENTACION.dbo.TB_EJE_VIAL WHERE  {} ".format(where_list))
    viviendas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_VIVIENDAS_U',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_VIVIENDAS_NACIONAL WHERE {} ".format(where_list))



    ccpp_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_CCPP'," SELECT  ID,UBIGEO,CODCCPP,NOMCCPP,CAST(AREA AS INT)AREA,VIV_CCPP,CATEGORIA,CATEGORIA_O,LLAVE_CCPP,geom,COMUNIDAD,AER_INI,AER_FIN,IDSCR,IDAER,IDRUTA,ESTADO FROM TB_CCPP WHERE {} ".format(where_list))
    manzanas_mfl = arcpy.MakeFeatureLayer_management(manzanas_Layer,"manzanas_mfl")
    zonas_mfl=arcpy.MakeFeatureLayer_management(zonas_Layer, "zonas_mfl")
    eje_vial_mfl = arcpy.MakeFeatureLayer_management(eje_vial_Layer, "eje_vial_mfl")
    viviendas_mfl = arcpy.MakeFeatureLayer_management(viviendas_Layer, "viviendas_mfl")
    ccpp_mfl= arcpy.MakeFeatureLayer_management(ccpp_Layer, "ccpp_mfl")

    list_mfl = [[viviendas_mfl, tb_viviendas], [manzanas_mfl, tb_manzanas]]

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
    arcpy.CopyFeatures_management(eje_vial_mfl, tb_eje_vial)
    arcpy.CopyFeatures_management(ccpp_mfl, tb_ccpp)





def insertar_registros(data):
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


    carto_viv = os.path.join(path_dataset_db,"{}.SDE.CARTO_VIVIENDA".format(database))
    carto_ccpp = os.path.join(path_dataset_db, "{}.SDE.CARTO_CCPP".format(database))
    carto_manzanas=os.path.join(path_dataset_db,"{}.SDE.CARTO_MANZANA".format(database))
    carto_zona = os.path.join(path_dataset_db, "{}.SDE.CARTO_ZONA".format(database))
    carto_eje_vial=os.path.join(path_dataset_db, "{}.SDE.CARTO_EJE_VIAL".format(database))
    list_capas = [[tb_viviendas,carto_viv,1],[tb_manzanas,carto_manzanas,1],[tb_zonas,carto_zona,1],[tb_eje_vial,carto_eje_vial,1],[tb_ccpp,carto_ccpp,1]]
    i = 0

    conn = conx.Conexion3()
    cursor = conn.cursor()

    for el in data:
        ubigeo = el
        print 'ubigeo:', ubigeo
        sql_query = """
                delete {database}.SDE.CARTO_VIVIENDA where ubigeo='{ubigeo}'
                delete {database}.SDE.CARTO_MANZANA where ubigeo='{ubigeo}'
                delete {database}.SDE.CARTO_ZONA where ubigeo='{ubigeo}'
                delete {database}.SDE.CARTO_EJE_VIAL  where ubigeo='{ubigeo}'
                delete {database}.SDE.CARTO_CCPP  where ubigeo='{ubigeo}'
                """.format(database=database,ubigeo=ubigeo)
        cursor.execute(sql_query)
        conn.commit()
    conn.close()

    for el in list_capas:

        i = i + 1

        print el[0]
        if (int(el[2]) > 1):
            a = arcpy.MakeTableView_management(el[0], "a{}".format(i), )

        else:
            a = arcpy.MakeFeatureLayer_management(el[0], "a{}".format(i))

        arcpy.Append_management(a, el[1], "NO_TEST")



def actualizar_monitoreo():
    distrito=""
    conn = conx.Conexion()
    cursor = conn.cursor()
    sql_query = """
select distinct a.ubigeo from
(
select distinct ubigeo from dbo.TB_VIVIENDA_U
union all
select distinct ubigeo from dbo.TB_VIVIENDA_r) a
where ubigeo>'050506'
order by a.ubigeo

                """
    cursor.execute(sql_query)

    for row in cursor.fetchall():
        distrito="{}".format(row[0])


        where=expresion.Expresion_2([[distrito]],[["UBIGEO","TEXT"]])

        print where


        importar_tablas(where)
        insertar_registros([distrito])

        continue

    #sql_query2="""UPDATE A
    #             SET A.AREA=2
    #             FROM sde.CARTO_ccpp A
#
    #             UPDATE A
    #             SET A.AREA=1
    #             FROM sde.CARTO_ccpp  A
    #             INNER JOIN
    #            (SELECT DISTINCT UBIGEO ,CODCCPP FROM [LKN_93_SEGM].[CPV_SEGMENTACION].DBO.MARCO_ZONA) B ON A.UBIGEO=B.UBIGEO AND A.CODCCPP=B.CODCCPP"""
#
    #cursor.execute(sql_query2)

    conn.commit()
    conn.close()

actualizar_monitoreo()


