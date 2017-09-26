import arcpy
import os
import conection_sql as conx
import sys
import  pyPdf
import subprocess


#path_ini="\\\\172.18.1.93\\file_resultados_proceso_segm\\{}{}".format(ubigeox,zonax)



#path_ini="d:\\file_resultados_proceso_segm\\{}{}".format(ubigeox,zonax)
##path_ini = "D:\\proyecto-segmentacion-urbana\\segmentacion\\resultados\\{}{}".format(ubigeox,zonax)
#
#tb_rutas = path_ini + "/final_tb_rutas.dbf"
#tb_subzonas = path_ini + "/final_tb_subzonas.dbf"
#tb_rutas_lineas = path_ini + "/final_tb_rutas_lineas.shp"
#tb_rutas_lineas_multifamiliar = path_ini + "/final_tb_rutas_lineas_multifamiliar.shp"
#tb_rutas_puntos = path_ini + "/tb_rutas_puntos.shp"
#tb_viviendas_ordenadas_dbf= path_ini + "/final_tb_vivienda.dbf"
#tb_sitios_interes=path_ini + "/final_tb_sitios_interes.dbf"
#tb_aeus = path_ini + "/final_tb_aeus.dbf"
#tb_secciones = path_ini + "/final_tb_secciones.shp"
#tb_frentes_1 = path_ini+"/tb_frentes_1.shp"
#tb_frentes_2 = path_ini+"/tb_frentes_2.shp"
#tb_frentes_3 = path_ini+"/tb_frentes_3.shp"
#
#
#path_out_final="\\\\192.168.201.115\\cpv2017"
##path_urbano_croquis = path_out + "\\croquis\\urbano"
##path_urbano_listados = path_out + "\\listados\\urbano"
#path_urbano_croquis_listado = path_out_final + "\\croquis-listado\\urbano\\{}".format(fase)
#
#arcpy.env.workspace = path_ini
#arcpy.env.overwriteOutput = True

#ccdd='{}'.format(sys.argv[1])


def obtener_lista_zonas_insercion():
    conn = conx.Conexion()
    cursor = conn.cursor()
    sql_query = """
                select  ubigeo,zona from marco_zona
                where
                --flag_data_insert=0 and

                flag_proc_segm=1 and fase='CPV2017' and ubigeo +zona in  ('15010800301')


                """
    print sql_query
    cursor.execute(sql_query)
    data=[]

    for row in cursor:
        data.append(["{}".format(row[0]),"{}".format(row[1])])
    conn.commit()
    conn.close()
    return data




def insertar_registros():
    arcpy.env.overwriteOutput = True
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
    db='CPV_SEGMENTACION_GDB'
    ip = '172.18.1.93'
    usuario='sde'
    password='$deDEs4Rr0lLo'
    path_conexion="Database Connections/{}.sde".format(db)
    #path_conexion = "d:/conexion/{}.sde".format(db)
    #path_conexion=conx.conexion_arcgis(db,ip,usuario,password)

    list_zonas=obtener_lista_zonas_insercion()

    #print list_zonas

    list_rutas=[]
    list_subzonas = []
    list_rutas_lineas=[]
    list_rutas_lineas_multifamiliar=[]
    list_rutas_puntos=[]
    list_viviendas_ordenadas=[]
    list_sitios_interes=[]
    list_aeus=[]
    list_secciones=[]
    list_frentes_1=[]
    list_frentes_2 = []
    list_frentes_3 = []

    list_zonas_sql=""

    for i,r in enumerate(list_zonas):
        ubigeox=r[0]
        zonax = r[1]
        path_ini="d:\\file_resultados_proceso_segm\\{}{}".format(ubigeox,zonax)
        tb_rutas = path_ini + "/final_tb_rutas.dbf"
        tb_subzonas = path_ini + "/final_tb_subzonas.dbf"

        tb_rutas_lineas = path_ini + "/final_tb_rutas_lineas.shp"
        tb_rutas_lineas_multifamiliar = path_ini + "/final_tb_rutas_lineas_multifamiliar.shp"
        tb_rutas_puntos = path_ini + "/tb_rutas_puntos.shp"
        tb_viviendas_ordenadas_dbf= path_ini + "/final_tb_vivienda.dbf"
        tb_sitios_interes=path_ini + "/final_tb_sitios_interes.dbf"
        tb_aeus = path_ini + "/final_tb_aeus.dbf"
        tb_secciones = path_ini + "/final_tb_secciones.shp"
        tb_frentes_1 = path_ini+"/tb_frentes_1.shp"
        tb_frentes_2 = path_ini+"/tb_frentes_2.shp"
        tb_frentes_3 = path_ini+"/tb_frentes_3.shp"


        list_rutas.append(tb_rutas)
        list_subzonas.append(tb_subzonas)
        list_rutas_lineas.append(tb_rutas_lineas)
        list_rutas_lineas_multifamiliar.append(tb_rutas_lineas_multifamiliar)
        list_rutas_puntos.append(tb_rutas_puntos)
        list_viviendas_ordenadas.append(tb_viviendas_ordenadas_dbf)
        list_sitios_interes.append(tb_sitios_interes)
        list_aeus.append(tb_aeus)
        list_secciones.append(tb_secciones)
        list_frentes_1.append(tb_frentes_1)
        list_frentes_2.append(tb_frentes_2)
        list_frentes_3.append(tb_frentes_3)

        if i==0:
            list_zonas_sql="'{}{}'".format(ubigeox,zonax)
        else:
            list_zonas_sql = "{},'{}{}'".format(list_zonas_sql,ubigeox, zonax)

    arcpy.env.workspace =path_conexion



    segm_ruta=path_conexion + "/{db}.SDE.SEGM_U_RUTA".format(db=db)
    segm_aeu=path_conexion + "/{db}.SDE.SEGM_U_AEU".format(db=db)
    segm_subzona = path_conexion + "/{db}.SDE.SEGM_U_SUBZONA".format(db=db)
    segm_seccion=path_conexion + "/{db}.SDE.URBANO/{db}.SDE.SEGM_U_SECCION".format(db=db)
    segm_rutas_lineas=path_conexion + "/{db}.SDE.URBANO/{db}.SDE.SEGM_RUTAS_LINEAS".format(db=db)
    segm_rutas_lineas_multi = path_conexion + "/{db}.SDE.URBANO/{db}.SDE.SEGM_RUTAS_LINEAS_MULTIFAMILIAR".format(db=db)
    segm_rutas_puntos = path_conexion + "/{db}.SDE.URBANO/{db}.SDE.SEGM_RUTAS_PUNTOS".format(db=db)
    segm_frentes_1 = path_conexion + "/{db}.SDE.URBANO/{db}.SDE.SEGM_FRENTES_1".format(db=db)
    segm_frentes_2 = path_conexion + "/{db}.SDE.URBANO/{db}.SDE.SEGM_FRENTES_2".format(db=db)
    segm_frentes_3 = path_conexion + "/{db}.SDE.URBANO/{db}.SDE.SEGM_FRENTES_3".format(db=db)
    segm_sitios_interes = path_conexion + "/{db}.SDE.URBANO/{db}.SDE.SEGM_SITIOS_INTERES".format(db=db)
    segm_vivienda_u = path_conexion + "/{db}.SDE.SEGM_U_VIV".format(db=db)


    list_capas = [  [list_viviendas_ordenadas,segm_vivienda_u,2],
                    [list_rutas,segm_ruta,2],
                    [list_aeus,segm_aeu,2],
                    [list_secciones,segm_seccion,1],
                    [list_subzonas,segm_subzona,2],
                    [list_rutas_lineas,segm_rutas_lineas,1],
                    [list_rutas_lineas_multifamiliar, segm_rutas_lineas_multi, 1],
                    [list_rutas_puntos, segm_rutas_puntos, 1],
                    [list_sitios_interes, segm_sitios_interes, 1],
                    [list_frentes_1, segm_frentes_1, 1],
                    [list_frentes_2, segm_frentes_2, 1],
                    [list_frentes_3, segm_frentes_3, 1],

                ]


    conn = conx.Conexion2()
    cursor = conn.cursor()
    sql_query = """
                DELETE {db}.SDE.SEGM_U_RUTA where ubigeo+zona in ({ccdd})
                DELETE {db}.SDE.SEGM_U_AEU where ubigeo+zona in ({ccdd})
                delete {db}.SDE.SEGM_U_VIV  where ubigeo+zona in ({ccdd})
                delete {db}.SDE.SEGM_U_SECCION where ubigeo+zona in ({ccdd})
                delete {db}.SDE.SEGM_U_SUBZONA  where ubigeo+zona in ({ccdd})
                delete {db}.SDE.SEGM_RUTAS_LINEAS  where ubigeo+zona in ({ccdd})
                delete {db}.SDE.SEGM_RUTAS_LINEAS_MULTIFAMILIAR  where ubigeo+zona in ({ccdd})
                delete {db}.SDE.SEGM_FRENTES_1  where ubigeo+zona in ({ccdd})
                delete {db}.SDE.SEGM_FRENTES_2  where ubigeo+zona in ({ccdd})
                delete {db}.SDE.SEGM_FRENTES_3  where ubigeo+zona in ({ccdd})
                delete {db}.SDE.SEGM_SITIOS_INTERES  where ubigeo+zona in ({ccdd})
                delete {db}.SDE.SEGM_RUTAS_PUNTOS  where ubigeo+zona in ({ccdd})
                """.format(db=db,ccdd=list_zonas_sql)
    print sql_query
    cursor.execute(sql_query)
    conn.commit()

    conn.close()
    #print list_capas

    i=0
    for el in list_capas:
        print el[1]

        arcpy.Append_management(el[0], el[1], "NO_TEST")


    db = 'CPV_MONITOREO_GIS'
    ip = '192.168.202.84'
    usuario = 'sde'
    password = 'wruvA7a*tat'
    path_conexion = "Database Connections/{db}.sde".format(db=db)
    arcpy.env.workspace = path_conexion
    nom_dataset = "{}.sde.{}".format(db, 'CPV2017')
    path_dataset_db = os.path.join(path_conexion, nom_dataset)
    tb_viv=os.path.join(path_conexion,'{db}.SDE.TAB_SEGM_U_VIV'.format(db=db))
    tb_secc=os.path.join(path_dataset_db,'{db}.SDE.CARTO_U_SECCION'.format(db=db))



    conn = conx.Conexion3()
    cursor = conn.cursor()
    sql_query = """
            DELETE SDE.CARTO_U_SECCION where ubigeo+zona in ({ccdd})
            DELETE  SDE.TAB_SEGM_U_VIV where ubigeo+zona in ({ccdd})
            """.format(ccdd=list_zonas_sql)
    cursor.execute(sql_query)
    conn.commit()


    conn.close()

    list_capas = [
        [list_secciones, tb_secc, 1],
        [list_viviendas_ordenadas, tb_viv, 2],

    ]


    for el in list_capas:
        print el[1]

        arcpy.Append_management(el[0], el[1], "NO_TEST")

    for el in  list_zonas:
        ubigeo=el[0]
        zona = el[1]
        conx.ActualizarRegistrosCPVSegmentacion(codigo="{}{}".format(ubigeo, zona), fase='CPV2017')
        #proceso = subprocess.Popen("python D:\Dropbox\scripts\merge_pdfs.py {} {} ".format(ubigeo, zona), shell=True,
        #                           stderr=subprocess.PIPE)
    actualizar_flag_insercion(list_zonas_sql)


def actualizar_flag_insercion(list_zonas_sql):
    conn = conx.Conexion()
    cursor = conn.cursor()
    sql_query = """
                   update marco_zona
                   set flag_data_insert=1
                   where flag_data_insert=0 and flag_proc_segm=1 and fase='CPV2017' and ubigeo+zona in ({ccdd})

                   """.format(ccdd=list_zonas_sql)
    print sql_query
    cursor.execute(sql_query)
    data = []

    for row in cursor:
        data.append(["{}".format(row[0]), "{}".format(row[1])])
    conn.commit()
    conn.close()
    return data



print "insertando registros"
insertar_registros()
