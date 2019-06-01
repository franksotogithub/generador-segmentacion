import sys
import conection_sql as conx
import expresiones_consulta_arcpy as expresion
import os
import utilidades
import arcpy
import utilidades as u

path_out_final="\\\\192.168.201.115\\cpv2017\\croquis-listado\\urbano"
path_emp_esp= path_out_final+"\\EMPADRONAMIENTO-ESPECIAL"
path_ini = "D:/proyecto-segmentacion-urbana/segmentacion"
path_proyecto="D:/Dropbox"
path_plantillas_croquis=path_proyecto+"/plantillas-croquis"
tb_manzanas = path_ini + "/tb_manzanas.shp"
tb_zonas = path_ini + "/tb_zonas.shp"
tb_sitios_interes = path_ini+"/tb_sitios_interes.shp"
tb_ejes_viales = path_ini+"/tb_ejes_viales.shp"
tb_manzanas_ordenadas = path_ini + "/tb_manzanas_ordenadas.shp"
path_pdf_1=os.path.join(path_emp_esp, "viv-colec-inst_dist")
path_pdf_2=os.path.join(path_emp_esp, "viv-colec-no-inst_dist")
path_pdf_3=os.path.join(path_emp_esp, "viv-colec-inst_zona")
path_pdf_4=os.path.join(path_emp_esp, "viv-colec-no-inst_zona")
fase='CPV2017'
ubigeo='{}'.format(sys.argv[1])



def crear_carpetas():

    list_paths=[path_emp_esp,
                path_pdf_1,
                path_pdf_2,
                path_pdf_3,
                path_pdf_4,
        ]


    for el in  list_paths:
        if os.path.exists(el) == False:
            os.mkdir(el)

def exportar_emp_especial_zona(ubigeo, zona,subzona,fase='CPV2017'):


    list_paths_zona=[]
    for i in range(3):
        out=os.path.join(path_emp_esp,"{}_{}{}_{}.pdf".format(ubigeo,zona,subzona,i+1))
        informacion = conx.obtener_informacion_reporte_emp_especial(ubigeo=ubigeo, zona=zona, subzona=subzona, tipo=i+1)
        if len(informacion[1])>0:
            informacion[0].append(len(informacion[1]))
            exportar_croquis_emp_especial(tipo=i+1, informacion=informacion,out_croquis=out)
            list_paths_zona.append(out)

    return list_paths_zona


def importar_tablas(data,campos):
    arcpy.env.overwriteOutput = True
    database='CPV_SEGMENTACION_GDB'
    ip = '172.18.1.93'
    usuario='sde'
    password='$deDEs4Rr0lLo'
    path_conexion=conx.conexion_arcgis(database,ip,usuario,password)
    arcpy.env.workspace =path_conexion

    #temp_ubigeos = ""
    #i=0

    #for x in data:
    #    i=i+1
    #    if (i==1):
    #        temp_ubigeos="'{}'".format(x[0])
    #    else:
    #        temp_ubigeos = "{},'{}'".format(temp_ubigeos,x[0])

    sql=expresion.Expresion_2(data, campos)

    list_capas=[
                ["{}.sde.TB_EJE_VIAL".format(database), tb_ejes_viales, 1],
                ["{}.sde.VW_ZONA_CENSAL".format(database), tb_zonas, 1],
                ["{}.sde.TB_MANZANA".format(database), tb_manzanas, 1],
                ["{}.sde.SEGM_SITIOS_INTERES".format(database), tb_sitios_interes, 1],

                ]



    for i,capa in enumerate(list_capas):
        print capa
        if capa[2] == 1:
            if capa[1] in [tb_manzanas,tb_sitios_interes,tb_ejes_viales]:

                if capa[1] == tb_sitios_interes:
                    x = arcpy.MakeQueryLayer_management(path_conexion, 'capa{}'.format(i),
                                                        "select * from {} where ({}) AND (CODIGO<91 AND CODIGO<>26) ".format(
                                                            capa[0],
                                                            sql))
                else:
                    x = arcpy.MakeQueryLayer_management(path_conexion, 'capa{}'.format(i),
                                                        "select * from {} where  {}  ".format(capa[0],
                                                                                                         sql))

            else:
                x = arcpy.MakeQueryLayer_management(path_conexion, 'capa{}'.format(i),
                                                    "select * from {} where {} ".format(capa[0], sql ))

        else:
            x = arcpy.MakeQueryTable_management(capa[0], "capa{}".format(i), "USE_KEY_FIELDS", "objectid", "", sql)



        if capa[1] in [tb_manzanas]:
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

def ordenar_manzanas_faso_cod():
    manzanas_selecc= arcpy.Select_analysis(tb_manzanas, "in_memory//manzanas_selecc")
    manzanas_ordenadas=arcpy.Sort_management(manzanas_selecc, tb_manzanas_ordenadas, ["UBIGEO", "ZONA", "FALSO_COD"])
    expression = "flg_manzana(!VIV_MZ!)"

    arcpy.AddField_management(manzanas_ordenadas, "FLG_MZ", "SHORT")

def exportar_croquis_emp_especial(tipo,informacion,out_croquis):
    list_manzanas = []
    cab = informacion[0]
    ccdd = cab[0]
    dep = cab[1]
    ccpp = cab[2]
    prov = cab[3]
    ccdi = cab[4]
    dist = cab[5]
    codccpp = cab[6]
    nomccpp = cab[7]
    etiq_zona = cab[8]
    subzona=cab[9]
    zona = cab[10]
    cant_viv=cab[11]

    ubigeo=ccdd+ccpp+ccdi


    for fila in informacion[1]:
        manzana=fila[2]
        list_manzanas.append([ubigeo,zona,manzana])

    where_zona=expresion.Expresion_2([[ubigeo,zona] ],[ ["UBIGEO","TEXT"],["ZONA","TEXT"]])
    where_manzanas=expresion.Expresion_2(list_manzanas,[["UBIGEO","TEXT"],["ZONA","TEXT"],['MANZANA','TEXT']])

    #########################################Listamos los layers del mxd #####################################
    mxd = arcpy.mapping.MapDocument(path_plantillas_croquis + "/CroquisUrbanoZonaEmpEspecial.mxd")
    df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
    zona_mfl = arcpy.mapping.ListLayers(mxd, "TB_ZONAS")[0]
    mzs_mfl = arcpy.mapping.ListLayers(mxd, "TB_MZS_ORD")[0]

    print where_zona
    print where_manzanas
    mzs_mfl.definitionQuery = where_manzanas
    zona_mfl.definitionQuery=where_zona



    #########################################Asignado los valores de las variables de los croquis ################################################
    if tipo==1:
        titulo_croquis=u"CROQUIS DE VIVIENDAS COLECTIVAS INSTITUCIONALES DE LA ZONA CENSAL"
        cod_documento=u"Doc. CPV.03.34"
        etiq_total_viv=u"TOTAL DE VIVIENDAS COLECTIVAS"

    elif tipo==2:
        titulo_croquis = u"CROQUIS DE VIVIENDAS COLECTIVAS NO INSTITUCIONALES DE LA ZONA CENSAL"
        cod_documento=u"Doc. CPV.03.34A"
        etiq_total_viv = u"TOTAL DE VIVIENDAS COLECTIVAS"

    else:

        titulo_croquis = u"CROQUIS DE EMPRESAS E INSTITUCIONES DE LA ZONA CENSAL"
        cod_documento = u"Doc. CPV.03.34B"
        etiq_total_viv = u"TOTAL DE EMPRESAS E INSTITUCIONES"



    list_text_el = [["titulo_croquis",titulo_croquis],["cod_documento",cod_documento],
                    ["CCDD", ccdd],["CCPP", ccpp], ["CCDI", ccdi], ["CODCCPP", codccpp],
                    ["DEPARTAMENTO", dep], ["PROVINCIA", prov], ["DISTRITO", dist], ["NOMCCPP", nomccpp],
                    ["ZONA", etiq_zona], ["SUBZONA", subzona],["CANT_VIV", cant_viv],["etiq_total_viv",etiq_total_viv] ]

    for text_el in list_text_el:
        el = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", text_el[0])[0]
        el.text = text_el[1]
    #######################################Asignamos la escala del dibujo#################################################
    df.extent = zona_mfl.getSelectedExtent()

    arcpy.mapping.ExportToPDF(mxd, out_croquis, "PAGE_LAYOUT")
    u.error_export_pdf(out_croquis, mxd)


    del mxd
    del df


def exportar_emp_especial_dist(ubigeo,fase='CPV2017'):
    c = conx.Conexion()
    cursor = c.cursor()


    list_paths_dist=[]



    sql_zona = """
                    begin

           	        select ubigeo,zona,subzona from CPV_SEGMENTACION.dbo.SUBZONA
                       where  fase='{fase}' and ubigeo='{ubigeo}'
                       order by 1,2,3
           	        end
                       """.format(fase=fase, ubigeo=ubigeo)


    cursor.execute(sql_zona)
    list_paths_zonas=[]
    for row in cursor:
        ubigeo = row[0]
        zona = row[1]
        subzona = row[2]
        list_paths_zonas=exportar_emp_especial_zona(ubigeo=ubigeo,zona=zona,subzona=subzona)[:]

        list_paths_dist.extend(list_paths_zonas)

    path_out_final=os.path.join(path_emp_esp,'{}_emp_especial.pdf'.format(ubigeo))

    if len(list_paths_dist)>0:
        utilidades.mergePDF(list_paths_dist,path_out_final)
    print 'listado de pdfs', list_paths_dist
    for path in list_paths_dist:
        os.remove(path)

    c.close()

def procesar_emp_especial(ubigeo):
    importar_tablas(data=[[ubigeo]], campos=[["UBIGEO","TEXT"]])
    ordenar_manzanas_faso_cod()
    exportar_emp_especial_dist(ubigeo)


procesar_emp_especial(ubigeo)