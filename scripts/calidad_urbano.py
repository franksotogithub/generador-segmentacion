
#import math
#import numpy as np
#import random
import os
#import shutil
import arcpy
import expresiones_consulta_arcpy
import conection_sql as conex
import  sys



print sys.argv[1],sys.argv[2]
#
ubigeo='{}'.format(sys.argv[1])
zona='{}'.format(sys.argv[2])


arcpy.env.overwriteOutput = True

path_proyecto="D:/proyecto-segmentacion-urbana"

path_calidad= "D:/proyecto-segmentacion-urbana/calidad"
path_ini=path_calidad

tb_rutas = path_calidad + "/tb_rutas.dbf"
tb_subzonas = path_calidad + "/tb_subzonas.dbf"
tb_rutas_lineas = path_calidad + "/tb_rutas_lineas.shp"
tb_rutas_lineas_multifamiliar = path_calidad + "/tb_rutas_lineas_multifamiliar.shp"
tb_viviendas = path_calidad + "/tb_viviendas.shp"
tb_viviendas_ordenadas = tb_viviendas
tb_viviendas_ordenadas_dbf= path_calidad + "/tb_viviendas_ordenadas.dbf"
tb_sitios_interes= path_calidad + "/tb_sitios_interes.dbf"
tb_aeus = path_calidad + "/tb_aeus.dbf"
tb_manzanas = path_calidad + "/tb_manzanas.shp"
tb_manzanas_ordenadas = tb_manzanas
tb_manzanas_final= path_calidad + "/tb_manzanas_final.shp"
tb_zonas = path_calidad + "/tb_zonas.shp"
tb_zonas_dbf = path_calidad + "/tb_zonas.dbf"
tb_puntos_inicio = path_calidad + "/tb_puntos_inicio.shp"
tb_frentes = path_calidad + "/tb_frentes.shp"
tb_frentes_dissolve= path_calidad + "/tb_frentes_dissolve.shp"
tb_frentes_puntos= path_calidad + "/tb_frentes_puntos.shp"
tb_ejes_viales= path_calidad + "/tb_ejes_viales.shp"
tb_mzs_condominios= path_calidad + "/tb_mzs_condominios.dbf"
tb_viviendas_cortes = path_calidad + "/tb_viviendas_cortes.shp"
tb_puertas_viv_multi= path_calidad + "/tb_puertas_viv_multi.shp"
tb_rutas_puntos = path_calidad + "/tb_rutas_puntos.shp"
tb_puntos_corte = path_calidad + "/tb_puntos_corte.shp"
tb_rutas_puntos_min= path_calidad + "/tb_rutas_puntos_min.shp"
tb_puntos_seleccionados_copy= path_calidad + "/tb_puntos_seleccionados_copy.shp"
tb_edificios_copy= path_calidad + "/tb_edificios_copy.shp"
tb_puntos_opciones= path_calidad + "/tb_puntos_opciones.shp"
tb_multifamiliar_poligonos= path_calidad + "/multifamiliar_poligonos.shp"
tb_vertice_final_manzana= path_calidad + "/tb_vertice_final_manzana.shp"
tb_distrito_ope= path_calidad + "/tb_distrito_ope.dbf"
tb_copia_multifamiliar= path_calidad + "/tb_copia_multifamiliar.shp"

error_1= path_calidad + "/error_1_puerta_multifamiliar.shp"
error_2= path_calidad + "/error_2_manzanas_sin_vias.shp"
error_3= path_calidad + "/error_3_manzanas_vias_dentro.shp"
error_4= path_calidad + "/error_4_puntos_inicio_error.shp"
error_5= path_calidad + "/error_5_viviendas_afuera_mz.shp"
error_6= path_calidad + "/error_6_frentes_manzanas_cantidad.dbf"
error_7= path_calidad + "/error_7_viviendas_error_frente.shp"
error_8= path_calidad + "/error_8_frentes_manzanas_forma.shp"
error_9= path_calidad + "/error_9_enumeracion_viviendas_frentes.shp"
error_10= path_calidad + "/error_10_viv_error_nombre_vias.shp"
error_11= path_calidad + "/error_11_puertas_hijos_multi_en_frente_mz.shp"
error_12= path_calidad + "/error_12_viv_hijos_sin_padre.shp"
error_13= path_calidad + "/error_13_doble_puerta_multifamiliar.shp"


tb_viv_no_enlazadas= path_calidad + "/tb_viv_no_enlazadas"


multifamiliar_id_padre = path_calidad + "/multifamiliar_id_reg"
tb_rutas_preparacion = path_calidad + "/tb_rutas_preparacion.shp"
tb_aeus_lineas = path_calidad + "/tb_aeus_lineas.shp"
tb_aeus_puntos = path_calidad + "/tb_aeus_puntos.shp"
tb_secciones = path_calidad + "/tb_secciones.shp"
ip_server="172.18.1.41"
VIVIENDAS_AEU_OR_MAX = path_calidad + "/VIVIENDAS_AEU_OR_MAX.shp"
VIVIENDAS_MZS_OR_MAX = path_calidad + "/VIVIENDAS_MZS_OR_MAX.shp"

ip="us_arcgis_seg_2"
clave="MBs0p0rt301"
db="CPV_SEGMENTACION"
arcpy.env.workspace = path_calidad




def importar_tablas_trabajo (data,campos):
    arcpy.env.overwriteOutput = True
    if arcpy.Exists("CPV_SEGMENTACION.sde") == False:
        arcpy.CreateDatabaseConnection_management("Database Connections",
                                                  "CPV_SEGMENTACION.sde",
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
    arcpy.env.workspace = "Database Connections/CPV_SEGMENTACION.sde"
    path_conexion="Database Connections/CPV_SEGMENTACION.sde"
    where_expression=expresiones_consulta_arcpy.Expresion(data, campos)
    temp_ubigeos = ""
    i=0
    for x in data:
        i=i+1
        if (i==1):
            temp_ubigeos="'{}'".format(x[0])
        else:
            temp_ubigeos = "{},'{}'".format(temp_ubigeos,x[0])


    print temp_ubigeos
    sql=expresiones_consulta_arcpy.Expresion(data, campos)
    print sql
    manzanas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_MZS',"SELECT geom,UBIGEO,CODCCPP,ZONA,MANZANA,VIV_MZ,FALSO_COD,MZS_COND,CANT_BLOCK FROM TB_MANZANA WHERE  ({}) ".format(sql))
    print 'importo manzanas'
    sitios_interes_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_SITIOS_INT',"SELECT * FROM TB_SITIO_INTERES WHERE UBIGEO IN ({}) AND (CODIGO<91 AND CODIGO<>26) ".format(temp_ubigeos))
    print 'importo sitios interes'
    puntos_inicio_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'TB_PUNTO_INICIO', "SELECT * FROM TB_PUNTO_INICIO WHERE {} ".format(sql))
    print 'importo puntos inicio'
    zonas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.VW_ZONA_CENSAL',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_ZONA_CENSAL WHERE {} ".format(sql))
    print 'importo zonas'
    eje_vial_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.TB_EJE_VIAL',
                                                     "SELECT * FROM CPV_SEGMENTACION.dbo.TB_EJE_VIAL where UBIGEO IN  ({}) ".format(temp_ubigeos))

    print 'importo ejes viales'
    viviendas_Layer = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.VW_VIVIENDAS_U',"SELECT * FROM CPV_SEGMENTACION.dbo.VW_VIVIENDAS_U WHERE {} ".format(sql))

    print 'importo viviendas'
    frentes_mfl = arcpy.MakeQueryLayer_management(path_conexion, 'CPV_SEGMENTACION.dbo.TB_FRENTES',"SELECT * FROM CPV_SEGMENTACION.dbo.TB_FRENTES WHERE {} ".format(sql))

    print 'importo frentes'

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
    arcpy.TableToTable_conversion(path_conexion + '/CPV_SEGMENTACION.dbo.VW_MZS_CONDOMINIOS', path_calidad, "tb_mzs_condominios.dbf")
    arcpy.env.workspace = path_calidad + ""
    arcpy.DeleteField_management(tb_manzanas, ['AEU','IDMANZANA'])
    arcpy.AddField_management(tb_manzanas, "IDMANZANA", "TEXT")
    expression = "(!UBIGEO!)+(!ZONA!)+(!MANZANA!)"
    arcpy.CalculateField_management(tb_manzanas, "IDMANZANA", expression, "PYTHON_9.3")
    arcpy.AddField_management(tb_manzanas, "AEU", "SHORT")
    arcpy.AddField_management(tb_manzanas, "AEU_2", "SHORT")
    arcpy.AddField_management(tb_manzanas, "FLG_MZ", "SHORT")
    arcpy.Dissolve_management(tb_frentes, tb_frentes_dissolve,['UBIGEO', 'ZONA', 'MANZANA', 'FRENTE_ORD'])



#def crear_carpetas_calidad():
#    #arcpy.env.workspace = path_proyecto
#
#    if os.path.exists(path_proyecto) == False:
#        os.mkdir(path_proyecto)
#    if os.path.exists(path_calidad) == False:
#        os.mkdir(path_calidad)#


def nombre_ejes_viales():
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
    lineas_viv_no_en=arcpy.PointsToLine_management(viviendas_no_enlazadas_select, path_calidad + '/lineas_viv_no_en.shp', 'p21', 'ID_FRENTE')


    ejes_viales_buffer=arcpy.Buffer_analysis(tb_ejes_viales,"in_memory/ejes_viales_buffer","40 meters","","","LIST",["CAT_VIA","NOMBRE_CAT","NOMBRE_VIA","NOMBRE_ALT","CAT_NOM","UBIGEO"])
    where_eje="NOMBRE_VIA<>'{}'".format("SN")

    ejes_viales_buffer_select=arcpy.Select_analysis(ejes_viales_buffer,'in_memory/ejes_viales_buffer_select',where_eje)
    ejes_viales_buffer_select_mfl=arcpy.MakeFeatureLayer_management(ejes_viales_buffer_select)
    temp=arcpy.SpatialJoin_analysis(ejes_viales_buffer_select_mfl, lineas_viv_no_en, path_calidad + '/temp.shp', 'JOIN_ONE_TO_MANY', '', '', 'CONTAINS')

    arcpy.AddField_management(temp,'AREA','DOUBLE')
    exp = "!SHAPE.AREA@METERS!"
    arcpy.CalculateField_management(temp,'AREA',exp,'PYTHON_9.3')
    temp_sort=arcpy.Sort_management(temp,'in_memory/temp_sort',[["JOIN_FID","ASCENDING"], ["AREA","ASCENDING"] ])
    temp_sort_select=arcpy.Select_analysis(temp_sort, path_calidad + '/temp_sort_select.shp', "JOIN_FID<>-1")
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

def procesar_calidad(cant_zonas=0,data=[],campos=['UBIGEO','ZONA']):

    if len(data) == 0:
        data = conex.obtener_lista_zonas_calidad(cant_zonas)[:]

    importar_tablas_trabajo(data, campos)
    where = expresiones_consulta_arcpy.Expresion(data, campos)
    arcpy.AddField_management(tb_viviendas_ordenadas, 'IDMANZANA', 'TEXT')
    arcpy.CalculateField_management(tb_viviendas_ordenadas, 'IDMANZANA', '!UBIGEO!+!ZONA!+!MANZANA!', 'PYTHON_9.3')

    # print "Importar"

    list_zonas = [(x[0], x[1]) for x in arcpy.da.SearchCursor(tb_zonas, ["UBIGEO", "ZONA"])]
    ######################################################CALIDAD PUERTAS MULTIFAMILIAR AFUERA DEL FRENTE DE MANZANA############################################################

    arcpy.AddField_management(tb_viviendas_ordenadas, 'IDMANZANA', 'TEXT')

    arcpy.CalculateField_management(tb_viviendas_ordenadas, 'IDMANZANA', '!UBIGEO!+!ZONA!+!MANZANA!', 'PYTHON_9.3')
    manzanas_mfl = arcpy.MakeFeatureLayer_management(tb_manzanas, "manzanas_mfl", where)
    viviendas_mfl = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendas_mfl", where)
    frentes_mfl = arcpy.MakeFeatureLayer_management(tb_frentes, "frentes_mfl", where)
    mzs_line = arcpy.FeatureToLine_management(manzanas_mfl, "in_memory/mzs_line")
    puertas_multifamiliar = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "puertas_multifamiliar", "p29=6")
    puertas_multifamiliar_afuera = arcpy.SelectLayerByLocation_management(puertas_multifamiliar, "INTERSECT", mzs_line,
                                                                          '', "NEW_SELECTION", "INVERT")
    viviendas_selecc_frentes_mfl = arcpy.SelectLayerByLocation_management(viviendas_mfl, "INTERSECT", mzs_line)
    viviendas_selecc_frentes = arcpy.CopyFeatures_management(viviendas_selecc_frentes_mfl,
                                                             "in_memory/viv_selecc_frentes")
    arcpy.CopyFeatures_management(puertas_multifamiliar_afuera, error_1)

    ########################################LISTA ZONAS CON ERROR PUERTA MULTIFAMILIAR###############################
    list_1 = list(set([(x[0], x[1]) for x in arcpy.da.SearchCursor(error_1, ["UBIGEO", "ZONA"])]))
    zonas_error_puertas_multi = list(set([(x[0], x[1]) for x in arcpy.da.SearchCursor(error_1, ["UBIGEO", "ZONA"])]))

    # print zonas_error_puertas_multi

    #####################################################CALIDAD EXISTENCIA DE EJES VIALES POR ZONA#######################################################################
    # tb_ejes_viales

    ejes_viales_mfl = arcpy.MakeFeatureLayer_management(tb_ejes_viales, "ejes_viales_mfl")
    manzanas_sin_vias = arcpy.SelectLayerByLocation_management(manzanas_mfl, "INTERSECT", ejes_viales_mfl, "20 METERS",
                                                               "NEW_SELECTION", "INVERT")
    arcpy.CopyFeatures_management(manzanas_sin_vias, error_2)

    ######################################LISTA DE ZONAS SIN EJES VIALES#############################################
    #list_2 = []
    #for x in arcpy.da.SearchCursor(tb_zonas, ["UBIGEO", "ZONA"]):
    #    where = " UBIGEO='{}' AND ZONA='{}'".format(x[0], x[1])
    #    manzanas_mfl = arcpy.MakeFeatureLayer_management(tb_manzanas, "manzanas_mfl", where)
    #    manzanas_sin_vias_mfl = arcpy.MakeFeatureLayer_management(error_2, "manzanas_sin_vias_mfl", where)
    #    a = int(arcpy.GetCount_management(manzanas_mfl).getOutput(0))
    #    b = int(arcpy.GetCount_management(manzanas_sin_vias_mfl).getOutput(0))
    #    if a != 0:
    #        porcentaje = b / float(a) * 100
#
    #    else:
    #        porcentaje = 100
#
    #    if porcentaje > 10:
    #        list_2.append((x[0], x[1]))

    ##################################################CALIDAD  MANZANAS INTERSECTADO CON VIAS########################################


    line_mzs=arcpy.FeatureToLine_management(tb_manzanas_ordenadas,"in_memory/line_mzs")
    buffer_line=arcpy.Buffer_analysis(line_mzs,"in_memory/buffer_line","0.50 meters")
    mzs_cortadas=arcpy.Erase_analysis(tb_manzanas_ordenadas,buffer_line,"in_memory/erase_mzs")

    #manzanas_ordenadas_mfl = arcpy.MakeFeatureLayer_management(tb_manzanas_ordenadas, "manzanas_ordenadas_mfl")
    manzanas_cortadas_mfl = arcpy.MakeFeatureLayer_management(mzs_cortadas, "mzs_cortadas_mfl")


    #vias_dentro_manzana = arcpy.SelectLayerByLocation_management(manzanas_ordenadas_mfl, "INTERSECT", tb_ejes_viales,'', "NEW_SELECTION")
    vias_dentro_manzana = arcpy.SelectLayerByLocation_management(manzanas_cortadas_mfl, "INTERSECT", tb_ejes_viales,'', "NEW_SELECTION")
    arcpy.CopyFeatures_management(vias_dentro_manzana, error_3)
    #########################################LISTA DE ZONAS CON VIAS DENTRO DE MANZANAS###################################

    list_3 = []

    if (int(arcpy.GetCount_management(error_3).getOutput(0)) > 0):
        list_3 = list(set([(x[0], x[1]) for x in arcpy.da.SearchCursor(error_3, ["UBIGEO", "ZONA"])]))
    #################Calidad Viviendas afuera de la manzana#################################################


    viviendas_mfl = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "viviendas_mfl", where)

    viviendas_afuera_manzana = arcpy.SelectLayerByLocation_management(viviendas_mfl, "INTERSECT", tb_manzanas_ordenadas,
                                                                      '0.2 meters', "NEW_SELECTION", "INVERT")


    arcpy.CopyFeatures_management(viviendas_afuera_manzana, error_5)

    ##########################################LISTA DE ZONAS CON VIVIENDAS FUERA DE MANZANA#################

    list_4 = []
    if (int(arcpy.GetCount_management(error_5).getOutput(0)) > 0):
        list_4 = list(set([(x[0], x[1]) for x in arcpy.da.SearchCursor(error_5, ["UBIGEO", "ZONA"])]))

    #################################################CALIDAD PUNTOS DE INICIO#######################################################################
    lineas_viviendas = arcpy.PointsToLine_management(viviendas_selecc_frentes, 'in_memory/lineas_viviendas',
                                                     "IDMANZANA", "ID_REG_OR")
    puntos_extremos = arcpy.FeatureVerticesToPoints_management(lineas_viviendas, 'in_memory/puntos_extremos',
                                                               "BOTH_ENDS")
    puntos_extremos_buffer = arcpy.Buffer_analysis(puntos_extremos,  'in_memory/puntos_extremos_buffer',
                                                   "0.2 meters")
    erase_lineas = arcpy.Erase_analysis(mzs_line, puntos_extremos_buffer, 'in_memory/erase_lineas')
    split = arcpy.SplitLine_management(erase_lineas, "in_memory/split")
    dissolve = arcpy.Dissolve_management(split, "in_memory/dissolve", "UBIGEO;CODCCPP;ZONA;MANZANA", "", "MULTI_PART",
                                         "DISSOLVE_LINES")
    dissolve_multi = arcpy.MultipartToSinglepart_management(dissolve, "in_memory/dissolve_multi")
    dissolve_mfl = arcpy.MakeFeatureLayer_management(dissolve_multi, 'dissolve_mfl')
    puntos_inicio_mfl = arcpy.MakeFeatureLayer_management(tb_puntos_inicio, 'puntos_inicio_mfl')

    segmentos_selec = arcpy.SelectLayerByLocation_management(dissolve_mfl, "INTERSECT", tb_viviendas_ordenadas, '',
                                                             "NEW_SELECTION", "INVERT")

    tb_segmentos_selec = arcpy.CopyFeatures_management(segmentos_selec, "{}/tb_segmentos_selec.shp".format(path_ini))

    puntos_inici_selec = arcpy.SelectLayerByLocation_management(puntos_inicio_mfl, "INTERSECT", tb_segmentos_selec, '',
                                                                "NEW_SELECTION", "INVERT")
    arcpy.CopyFeatures_management(puntos_inici_selec, error_4)

    ################################################LISTA DE ZONAS CON PROBLEMAS DE PUNTO DE INICIO##################################################

    list_5 = []

    if (int(arcpy.GetCount_management(error_4).getOutput(0)) > 0):
        list_5 = list(set([(x[0], x[1]) for x in arcpy.da.SearchCursor(error_4, ["UBIGEO", "ZONA"])]))

    ############################ Cantidad de frentes############################################################
    '''
    resumen_frentes_viv=arcpy.Statistics_analysis(tb_viviendas_ordenadas,'in_memory/resumen_frentes_viv',[["FRENTE_ORD","MAX"]],["UBIGEO","ZONA","MANZANA"])
    arcpy.AddField_management(resumen_frentes_viv,"ID_MANZANA","text")

    with arcpy.da.UpdateCursor(resumen_frentes_viv, ["UBIGEO","ZONA","MANZANA","ID_MANZANA"]) as cursor:
        for x in cursor:
            x[4]=u'{}{}{}'.format(x[0],x[1],x[2])
            cursor.updateRow(x)

    #arcpy.CalculateField_management(resumen_frentes_viv,"ID_MANZANA","!UBIGEO!+!ZONA!+!MANZANA!","PYTHON_9.3")



    resumen_frentes = arcpy.Statistics_analysis(tb_frentes_dissolve, 'in_memory/resumen_frentes',[["FRENTE_ORD", "MAX"],["FRENTE_ORD", "COUNT"]], ["UBIGEO", "ZONA", "MANZANA"])

    arcpy.AddField_management(resumen_frentes, "ID_MANZANA", "text")

    with arcpy.da.UpdateCursor(resumen_frentes, ["UBIGEO","ZONA","MANZANA","ID_MANZANA"]) as cursor:
        for x in cursor:
            x[4]=u'{}{}{}'.format(x[0],x[1],x[2])
            cursor.updateRow(x)

    arcpy.CalculateField_management(resumen_frentes, "ID_MANZANA", "!UBIGEO!+!ZONA!+!MANZANA!", "PYTHON_9.3")

    arcpy.JoinField_management(resumen_frentes,"ID_MANZANA",resumen_frentes_viv,"ID_MANZANA",["MAX_FRENTE_ORD"])
    mzs_dif_cant_frent=arcpy.TableSelect_analysis(resumen_frentes, error_6, " (MAX_FRENTE_ORD<>MAX_FRENTE_ORD_1)")

    arcpy.AddField_management(error_6, "CANT_FR_V", "SHORT")
    arcpy.CalculateField_management(error_6, "CANT_FR_V", "!MAX_FRENTE_ORD!")

    arcpy.AddField_management(error_6, "CANT_FR_F", "text")
    arcpy.CalculateField_management(error_6, "CANT_FR_F", "!MAX_FRENTE_ORD_1!")
    arcpy.DeleteField_management(error_6,["MAX_FRENTE_ORD","MAX_FRENTE_ORD_1"])

    list_6=[]


    if (int(arcpy.GetCount_management(error_6).getOutput(0)) > 0):
        list_6 = list(set([(x[0], x[1]) for x in arcpy.da.SearchCursor(error_6, ["UBIGEO", "ZONA"])]))

    #mzs_dif_cant_frent_1 = arcpy.TableSelect_analysis(resumen_frentes, error_7_cant_frentes_dif, " CapVivNFr<>COUNT_FRENTE_ORD")
    #list_7 = []
    #if (int(arcpy.GetCount_management(error_7_cant_frentes_dif).getOutput(0)) > 0):
    #    list_7 = list(set([(x[0], x[1]) for x in arcpy.da.SearchCursor(error_7_cant_frentes_dif, ["UBIGEO", "ZONA"])]))
    #arcpy.SelectLayerByLocation_management
    '''

    #####################################################ERROR DE FRENTE DE VIVIENDAS#########################################################

    resultado = arcpy.Intersect_analysis([tb_viviendas_ordenadas, tb_frentes], 'in_memory/results')

    arcpy.Select_analysis(resultado, error_7, 'FRENTE_ORD<>FRENTE_ORD_1')
    fields = arcpy.ListFields(error_7)

    list_campos_validos = ['FID', 'Shape', 'UBIGEO', 'CODCCPP', 'ZONA', 'MANZANA', 'ID_REG_OR','FRENTE_ORD']
    delete_fields = []
    for el in fields:
        if el.name not in list_campos_validos:
            delete_fields.append(el.name)

    arcpy.DeleteField_management(error_7, delete_fields)




    #####################################################ERROR FRENTES DE MANZANAS NO COINCIDEN CON LA MANZANA EN FORMA#################################
    temp_frentes = arcpy.SelectLayerByLocation_management(frentes_mfl, "WITHIN", mzs_line, '', "NEW_SELECTION",
                                                          "INVERT")
    arcpy.CopyFeatures_management(temp_frentes, error_8)

    list_8 = []

    if (int(arcpy.GetCount_management(error_8).getOutput(0)) > 0):
        list_8 = list(set([(x[0], x[1]) for x in arcpy.da.SearchCursor(error_8, ["UBIGEO", "ZONA"])]))

    ####################################################ERROR NUMERACION DE VIVIENDAS#############################################################

    lineas_viviendas = arcpy.PointsToLine_management(viviendas_selecc_frentes, 'in_memory/lineas_viviendas',
                                                     "IDMANZANA", "ID_REG_OR")
    viviendas_selecc_frentes_buffer = arcpy.Buffer_analysis(viviendas_selecc_frentes,
                                                            "in_memory/puntos_extremos_buffer", "0.2 meters")

    erase_lineas = arcpy.Erase_analysis(lineas_viviendas, viviendas_selecc_frentes_buffer, 'in_memory/erase_lineas')
    split = arcpy.SplitLine_management(erase_lineas, path_ini + "/split.shp")

    mz_line_erase = arcpy.Erase_analysis(mzs_line, viviendas_selecc_frentes_buffer, "in_memory\mz_line_erase")
    mz_line_erase_multi = arcpy.MultipartToSinglepart_management(mz_line_erase, 'in_memory\m_l_e_m')
    result = arcpy.Statistics_analysis(mz_line_erase_multi, 'in_memory/result', [['FID', "MAX"]], ["Shape"])
    maxids = [[x[0]] for x in arcpy.da.SearchCursor(result, ["MAX_FID"], 'FREQUENCY>1')]

    if len(maxids) == 0:
        where_ids = expresiones_consulta_arcpy.Expresion_2([["-1"]], [["FID", "SHORT"]])

    else:
        where_ids = expresiones_consulta_arcpy.Expresion_2(maxids, [["FID", "SHORT"]])

    arcpy.Select_analysis(mz_line_erase_multi, error_9, where_ids)




    '''
    intersect=arcpy.Intersect_analysis([mz_line_erase_multi, split], path_ini+"/intersect.shp", "ALL", "", "")
    list_id_buffer_mzs_line_erase_multi=list(set( [x[0] for x in arcpy.da.SearchCursor(intersect,["FID_m_l_e_"])]))
    list_intersect= [x[0]  for x  in arcpy.da.SearchCursor(intersect,["FID_m_l_e_"])]


    errores_numeracion=[]
    print list_id_buffer_mzs_line_erase_multi
    print list_intersect

    for x in list_id_buffer_mzs_line_erase_multi:
        cont = 0
        for y in list_intersect:
            if (x==y):
                cont=cont+1


        #print cont
        if (cont>1):
           errores_numeracion.append([x])
    print errores_numeracion
    where_exp=UBIGEO.Expresion_2(errores_numeracion,[["FID","SHORT"]])
    b_m_l_e_m_selecc = arcpy.Select_analysis(mz_line_erase_multi, error_9, where_exp)

    list_9=[]
    if (int(arcpy.GetCount_management(error_9).getOutput(0)) > 0):
        list_9 = list(set([(x[0], x[1]) for x in arcpy.da.SearchCursor(error_9, ["UBIGEO", "ZONA"])]))


    #dissolve = arcpy.Dissolve_management(split, "in_memory/dissolve", "UBIGEO;CODCCPP;ZONA;MANZANA", "","MULTI_PART","DISSOLVE_LINES")
    #dissolve_multi=arcpy.MultipartToSinglepart_management(dissolve, "in_memory/dissolve_multi")
    #arcpy.SelectLayerByLocation_management (dissolve_multi, "INTERSECT",dissolve_multi)
    #arcpy.MultipartToSinglepart_management("intersect", "in_memory/intersect2")

    '''

    #################################################VIVIENDAS Y VIAS#####################################################


    # list_zonas_error=list(set(list_1+list_2+list_3+list_4+list_5+list_6+list_8+list_9))
    # print  list_zonas_error
    #nombre_ejes_viales()

    ################################puertas  hijos multifamiliar en el frente##########################
    puertas_hijos_multifamilar = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "puertas_multifamiliar",
                                                                   "(p29=1  or p29=3) and ID_REG_PAD<>0 ")
    error_11_mfl = arcpy.SelectLayerByLocation_management(puertas_hijos_multifamilar, "INTERSECT", mzs_line, '',
                                                          "NEW_SELECTION")
    arcpy.CopyFeatures_management(error_11_mfl, error_11)



    # puertas_hijos_multifamilar=arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "puertas_multifamiliar", "(p29=1  or p29=3) and ID_REG_PAD<>0 ")
    # error_11_mfl=arcpy.SelectLayerByLocation_management(puertas_hijos_multifamilar, "INTERSECT",mzs_line ,'' , "NEW_SELECTION")
    # arcpy.CopyFeatures_management(error_11_mfl, error_11)



    ###############################################ERROR HIJOS SIN PADRES#########################################################################
    '''
    puertas_hijos_multifamilar = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "puertas_multifamiliar",
                                                                   "(p29=1  or p29=3) and (ID_REG_PAD<>0)" )

    list_puertas_hijos_multifamilar=[[x[0],x[1],x[2],x[3],x[4]] for x in  arcpy.da.SearchCursor(puertas_hijos_multifamilar,["UBIGEO","ZONA","MANZANA","ID_REG_OR","ID_REG_PAD"])]

    list_puertas_multifamiliar=[ '{}{}{}{}'.format(x[0],x[1],x[2],x[3]) for x in arcpy.da.SearchCursor(puertas_multifamiliar,["UBIGEO","ZONA","MANZANA","ID_REG"])]




    where_error_12=""

    i=0

    for el in list_puertas_hijos_multifamilar:
        i=i+1
        id_padre='{}{}{}{}'.format(el[0],el[1],el[2],el[4])
        if  id_padre not in list_puertas_multifamiliar:
            if i==1:
                where_error_12=" (UBIGEO='{}' AND ZONA='{}' AND MANZANA='{}' AND ID_REG_OR={})".format(el[0],el[1],el[2],el[3])
            else:
                where_error_12 = "{} OR (UBIGEO='{}' AND ZONA='{}' AND MANZANA='{}' AND ID_REG_OR={})".format(where_error_12,el[0], el[1],
                                                                                                         el[2], el[3])
    error_12_mfl=arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "error_12",where_error_12 )

    arcpy.CopyFeatures_management(error_12_mfl, error_12)






    #############################ERROR   PUERTAS MULTIFAMILIAR CON MAS DE 2 GEOMETRIAS#########################################
    set_puertas_multi=set(list_puertas_multifamiliar)

    where_error_13=""
    j=0

    for el in set_puertas_multi:
        i=0

        if el in list_puertas_multifamiliar:
           i=i+1

        if i>1:
            j=j+1
            if (j==1):
                where_error_13 = " (UBIGEO='{}' AND ZONA='{}' AND MANZANA='{}' AND ID_REG_OR={})".format(el[0], el[1],el[2], el[3])
            else:
                where_error_13 = "{} OR (UBIGEO='{}' AND ZONA='{}' AND MANZANA='{}' AND ID_REG_OR={})".format(where_error_13,el[0], el[1],
                                                                                                         el[2], el[3])

    error_13_mfl = arcpy.MakeFeatureLayer_management(tb_viviendas_ordenadas, "error_13", where_error_13)

    arcpy.CopyFeatures_management(error_13_mfl, error_13)

   '''

    ################################Insercion de data###########################################


    arcpy.env.workspace = "Database Connections/PruebaSegmentacion.sde"
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
    if arcpy.Exists("GEODATABASE.sde") == False:
        arcpy.CreateDatabaseConnection_management("Database Connections",
                                                  "GEODATABASE.sde",
                                                  "SQL_SERVER",
                                                  ip_server,
                                                  "DATABASE_AUTH",
                                                  "sde",
                                                  "$deDEs4Rr0lLo",
                                                  "#",
                                                  "GEODB_CPV_SEGM",
                                                  "#",
                                                  "#",
                                                  "#",
                                                  "#")
    arcpy.env.workspace = "Database Connections/GEODATABASE.sde"
    path_conexion2 = "Database Connections/GEODATABASE.sde"
    path_calidad = path_conexion2 + "/GEODB_CPV_SEGM.SDE.CALIDAD_URBANO"
    calidad_error_1_input = path_calidad + '/GEODB_CPV_SEGM.SDE.ERROR_1_INPUT_PUERTA_MULTIFAMILIAR_DENTRO_MZ'
    calidad_error_2_input = path_calidad + '/GEODB_CPV_SEGM.SDE.ERROR_2_INPUT_MANZANAS_SIN_VIAS'
    calidad_error_3_input = path_calidad + '/GEODB_CPV_SEGM.SDE.ERROR_3_INPUT_MANZANAS_VIAS_DENTRO'
    calidad_error_4_input = path_calidad + '/GEODB_CPV_SEGM.SDE.ERROR_4_INPUT_PUNTOS_INICIO'
    calidad_error_5_input = path_calidad + '/GEODB_CPV_SEGM.SDE.ERROR_5_INPUT_VIVIENDAS_AFUERA_MZ'
    calidad_error_7_input = path_calidad + '/GEODB_CPV_SEGM.SDE.ERROR_7_INPUT_VIVIENDAS_ERROR_FRENTE'
    calidad_error_8_input = path_calidad + '/GEODB_CPV_SEGM.SDE.ERROR_8_INPUT_FRENTES_MANZANAS_FORMA'
    calidad_error_9_input = path_calidad + '/GEODB_CPV_SEGM.SDE.ERROR_9_INPUT_ENUMERACION_VIV_POR_FRENTE'
    calidad_error_10_input = path_calidad + '/GEODB_CPV_SEGM.SDE.ERROR_10_INPUT_VIV_ERROR_NOMBRE_VIA'
    calidad_error_11_input = path_calidad + '/GEODB_CPV_SEGM.SDE.ERROR_11_INPUT_PUERTAS_HIJOS_MULTI_EN_FRENTE_MZ'
    #error_7 = path_calidad + "/error_7_viviendas_error_frente.shp"
    list_errores = [[error_1, calidad_error_1_input, 1],
                    [error_2, calidad_error_2_input, 1],
                    [error_3, calidad_error_3_input, 1],
                    [error_4, calidad_error_4_input, 1],
                    [error_5, calidad_error_5_input, 1],
                    [error_8, calidad_error_8_input, 1],
                    [error_9, calidad_error_9_input, 1],
                   # [error_10, calidad_error_10_input, 1],
                    [error_11, calidad_error_11_input, 1],
                    [error_7, calidad_error_7_input, 1],
                    ]


    conn = conex.Conexion2()
    cursor = conn.cursor()
    for el in data:
        ubigeo = el[0]
        zona = el[1]
        sql_query = """
                DELETE GEODB_CPV_SEGM.SDE.ERROR_1_INPUT_PUERTA_MULTIFAMILIAR_DENTRO_MZ where ubigeo='{ubigeo}' and zona='{zona}'
                DELETE GEODB_CPV_SEGM.SDE.ERROR_2_INPUT_MANZANAS_SIN_VIAS where ubigeo='{ubigeo}' and zona='{zona}'
                delete GEODB_CPV_SEGM.SDE.ERROR_3_INPUT_MANZANAS_VIAS_DENTRO where ubigeo='{ubigeo}' and zona='{zona}'
                delete GEODB_CPV_SEGM.SDE.ERROR_4_INPUT_PUNTOS_INICIO  where ubigeo='{ubigeo}' and zona='{zona}'
                delete GEODB_CPV_SEGM.SDE.ERROR_5_INPUT_VIVIENDAS_AFUERA_MZ  where ubigeo='{ubigeo}' and zona='{zona}'
                delete GEODB_CPV_SEGM.SDE.ERROR_7_INPUT_VIVIENDAS_ERROR_FRENTE  where ubigeo='{ubigeo}' and zona='{zona}'
                delete GEODB_CPV_SEGM.SDE.ERROR_8_INPUT_FRENTES_MANZANAS_FORMA  where ubigeo='{ubigeo}' and zona='{zona}'
                delete GEODB_CPV_SEGM.SDE.ERROR_9_INPUT_ENUMERACION_VIV_POR_FRENTE where ubigeo='{ubigeo}' and zona='{zona}'
                delete GEODB_CPV_SEGM.SDE.ERROR_10_INPUT_VIV_ERROR_NOMBRE_VIA where ubigeo='{ubigeo}' and zona='{zona}'
                delete GEODB_CPV_SEGM.SDE.ERROR_11_INPUT_PUERTAS_HIJOS_MULTI_EN_FRENTE_MZ where ubigeo='{ubigeo}' and zona='{zona}'
                """.format(ubigeo=ubigeo, zona=zona)
        cursor.execute(sql_query)
        conn.commit()
    conn.close()




    i = 0
    for el in list_errores:
        i = i + 1
        print el[0]
        if (int(el[2])>1):
            a = arcpy.MakeTableView_management(el[0], "a{}".format(i), )



        else:
            a = arcpy.MakeFeatureLayer_management(el[0], "a{}".format(i))


        arcpy.Append_management(a, el[1], "NO_TEST")

    #for el in list_errores:
    #    i = i + 1
#
    #    print where
#
    #    if el[2] == 1:
    #        a = arcpy.arcpy.MakeFeatureLayer_management(el[1], "a{}".format(i), where)
    #    else:
    #        a = arcpy.MakeTableView_management(el[1], "a{}".format(i), where)
#
    #    if (int(arcpy.GetCount_management(a).getOutput(0)) > 0):
    #        arcpy.DeleteRows_management(a)
#
    #    print 'borro'
    #    if el[2] == 1:
    #        b = arcpy.arcpy.MakeFeatureLayer_management(el[0], "b{}".format(i), where)
    #    else:
    #        b = arcpy.MakeTableView_management(el[0], "b{}".format(i), where)
#
    #    if (int(arcpy.GetCount_management(b).getOutput(0)) > 0):
    #        arcpy.Append_management(b, el[1], "NO_TEST")
    #    print 'inserto'

    for el in data:
        conex.actualizar_errores_input_adicionales(ubigeo=el[0], zona=el[1])


data=[[ubigeo,zona]]

procesar_calidad(data=data)




#for el in range(2700):
#    lista = conex.obtener_lista_zonas_calidad(cant_zonas=1)
#    if len(lista)>0:
#        for el in lista:
#            ubigeo=el[0]
#            zona=el[1]
#
#            try:
#                procesar_calidad(data=[[ubigeo,zona]])
#                conex.actualizar_flag_calidad_input_zonas(ubigeo, zona)
#            except:
#                break
#    else:
#        break




