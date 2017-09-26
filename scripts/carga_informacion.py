import arcpy
import os
import conection_sql as conx
import sys
import  pyPdf


path_ini = "D:\\proyecto-segmentacion-urbana\\segmentacion\\resultados"
ubigeox='{}'.format(sys.argv[1])
zonax='{}'.format(sys.argv[2])
fase ='{}'.format(sys.argv[3])

#path_ini="\\\\172.18.1.93\\file_resultados_proceso_segm\\{}{}".format(ubigeox,zonax)
path_ini="d:\\file_resultados_proceso_segm\\{}{}".format(ubigeox,zonax)
#path_ini = "D:\\proyecto-segmentacion-urbana\\segmentacion\\resultados\\{}{}".format(ubigeox,zonax)

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


path_out_final="\\\\192.168.201.115\\cpv2017"
#path_urbano_croquis = path_out + "\\croquis\\urbano"
#path_urbano_listados = path_out + "\\listados\\urbano"
path_urbano_croquis_listado = path_out_final + "\\croquis-listado\\urbano\\{}".format(fase)

arcpy.env.workspace = path_ini
arcpy.env.overwriteOutput = True

def insertar_registros(data):
    arcpy.env.overwriteOutput = True
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
    db='CPV_SEGMENTACION_GDB'
    ip = '172.18.1.93'
    usuario='sde'
    password='$deDEs4Rr0lLo'
    path_conexion="Database Connections/{}.sde".format(db)
    #path_conexion = "d:/conexion/{}.sde".format(db)
    #path_conexion=conx.conexion_arcgis(db,ip,usuario,password)
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
    #list_capas_ini = [tb_viviendas_ordenadas_dbf, tb_rutas, tb_aeus, tb_secciones, tb_subzonas, tb_rutas_lineas,tb_rutas_lineas_multifamiliar]

    #list_capas_fin = [segm_vivienda_u, segm_ruta, segm_aeu, segm_seccion, segm_subzona, segm_rutas_lineas,segm_rutas_lineas_multi]

    list_capas = [
                    [tb_viviendas_ordenadas_dbf,segm_vivienda_u,2],
                    [tb_rutas,segm_ruta,2],
                    [tb_aeus, segm_aeu, 2],
                    [tb_subzonas,segm_subzona,2],
                    [tb_secciones, segm_seccion, 1],
                    [tb_rutas_lineas,segm_rutas_lineas,1],
                    [tb_rutas_lineas_multifamiliar, segm_rutas_lineas_multi,1],
                    [tb_frentes_1, segm_frentes_1, 1],
                    [tb_frentes_2, segm_frentes_2, 1],
                    [tb_frentes_3, segm_frentes_3, 1],
                    [tb_sitios_interes, segm_sitios_interes, 1],
                    [tb_rutas_puntos, segm_rutas_puntos, 1],
                ]

    if arcpy.Exists(tb_rutas_lineas_multifamiliar):
        list_capas.append([tb_rutas_lineas_multifamiliar,segm_rutas_lineas_multi,1])

    #tb_rutas, tb_aeus, tb_secciones, tb_subzonas, tb_rutas_lineas]



    #for i,el in enumerate(list_capas_ini):
        #dir = os.path.split(el)
        #formato = dir[1].split(".")[1]


        #dir_copia = os.path.join(dir[0], "final_{}".format(dir[1]))
        #print dir_copia


    #for i, el in enumerate(list_capas_ini):
    #    dir = os.path.split(el)
    #    dir_copia = os.path.join(dir[0], "final_{}".format(dir[1]))
    #    print dir_copia
#
    #    formato = dir[1].split(".")[1]
#
    #    if el == tb_rutas_lineas_multifamiliar:
    #        if arcpy.Exists(tb_rutas_lineas_multifamiliar):
    #            list_capas.append([dir_copia, segm_rutas_lineas_multi, 1])
#
#
    #    else:
#
    #        if formato == 'shp':
    #            list_capas.append([dir_copia, list_capas_fin[i], 1])
    #        else:
    #            list_capas.append([dir_copia, list_capas_fin[i], 2])
#

    conn = conx.Conexion2()
    cursor = conn.cursor()

    for el in data:
        ubigeo = el[0]
        zona = el[1]
        sql_query = """
                DELETE {db}.SDE.SEGM_U_RUTA where ubigeo='{ubigeo}' and zona='{zona}'
                DELETE {db}.SDE.SEGM_U_AEU where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_U_VIV  where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_U_SECCION where ubigeo='{ubigeo}' and zona='{zona}'
                delete {db}.SDE.SEGM_U_SUBZONA  where ubigeo='{ubigeo}' and zona='{zona}'
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
    print list_capas

    i=0
    for el in list_capas:
        print el[0]
        i = i + 1
        if (int(el[2])>1):
            a = arcpy.MakeTableView_management(el[0], "a{}".format(i), )

        else:
            a = arcpy.MakeFeatureLayer_management(el[0], "b{}".format(i))
            cant_el=int(arcpy.GetCount_management(a).getOutput(0))
            print 'cantidad de elementos: ', cant_el

        arcpy.Append_management(a, el[1], "NO_TEST")

def insertar_registros_2(data):
    arcpy.env.overwriteOutput = True
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
    db = 'CPV_MONITOREO_GIS'
    ip = '192.168.202.84'
    usuario = 'sde'
    password = 'wruvA7a*tat*'


    path_conexion = "Database Connections/{}.sde".format(db)

    #path_conexion = "d:/conexion/{}.sde".format(db)
    nom_dataset="{}.sde.{}".format(db,'CPV2017')
    path_dataset_db = os.path.join(path_conexion,nom_dataset)

    tb_viv=os.path.join(path_conexion,'{}.SDE.TAB_SEGM_U_VIV'.format(db))
    tb_secc=os.path.join(path_dataset_db,'{}.SDE.CARTO_U_SECCION'.format(db))


    list_capas_ini = [ tb_secciones,tb_viviendas_ordenadas_dbf]
    list_capas_fin = [ tb_secc,tb_viv]

    list_capas = [
        [tb_secciones, tb_secc, 1],
        [tb_viviendas_ordenadas_dbf, tb_viv, 2],

    ]


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


    i=0

    for el in list_capas:
        i = i + 1
        print el[0]
        if (int(el[2])>1):
            a = arcpy.MakeTableView_management(el[0], "c{}".format(i), )

        else:
            a = arcpy.MakeFeatureLayer_management(el[0], "d{}".format(i))
        arcpy.Append_management(a, el[1], "NO_TEST")

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



data=[[ubigeox,zonax]]
insertar_registros(data)
insertar_registros_2(data)
conx.ActualizarRegistrosCPVSegmentacion(codigo="{}{}".format(ubigeox,zonax), fase=fase)
ActualizarCantPaginas(data)