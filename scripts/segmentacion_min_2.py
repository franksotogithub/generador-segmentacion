# -*- coding: utf-8 -*-
import math
import os
import arcpy
import expresiones_consulta_arcpy
import  datetime
import shutil
import sys
from datetime import *
import pymssql


arcpy.env.overwriteOutput = True


class Segmentacion:

    path_ini="D:/proyecto-segmentacion-urbana"
    path_segmentacion = "D:/proyecto-segmentacion-urbana/segmentacion-prueba"

    path_proyecto="D:/Dropbox"
    path_plantillas_croquis=path_proyecto+"/plantillas-croquis"
    path_plantillas_layers=path_proyecto+"/plantillas-layers"
    path_imagenes=path_proyecto+"/imagenes"
    tb_rutas = path_segmentacion + "/tb_rutas.dbf"
    tb_viviendas = path_segmentacion + "/tb_viviendas.shp"
    tb_viviendas_ordenadas = path_segmentacion + "/tb_viviendas_ordenadas.shp"
    tb_aeus = path_segmentacion + "/tb_aeus.dbf"
    tb_manzanas = path_segmentacion + "/tb_manzanas.dbf"
    tb_manzanas_ordenadas = path_segmentacion + "/tb_manzanas_ordenadas.dbf"
    tb_zonas = path_segmentacion + "/tb_zonas.dbf"
    tb_mzs_condominios= path_segmentacion + "/tb_mzs_condominios.dbf"
    list_zonas=[]
    list_manzanas=[]

    cant_viv_techo=16
    cant_viv_techo_gra_ciud=16
    cant_viv_techo_peq_ciud = 16
    data=[]
    cant_zonas=10
    where_expression=""
    techo_segunda_pasada_gra_ciud=16
    techo_segunda_pasada_peq_ciud=16
    techo_segunda_pasada=16
    uso_falso_cod=1
    campos=["UBIGEO","ZONA"]
    server = "172.18.1.41"
    database="CPV_SEGMENTACION"
    user = "us_arcgis_seg_2"
    password = "b8an!hUse8P-"
    cant_viv_eleg_segund_pasada=9




    def __init__(self,cant_viv_techo_gra_ciud=16, cant_viv_techo_peq_ciud=16,techo_segunda_pasada_gra_ciud=16,techo_segunda_pasada_peq_ciud=16,cant_zonas_x=10,data_x=[],uso_falso_cod=1 ,campos_x=["UBIGEO","ZONA"],cant_viv_eleg_segund_pasada=9):
        #self.cant_viv_techo=cant_viv_techo_x
        #print sys.path
        self.data=data_x[:]
        self.cant_zonas=cant_zonas_x
        self.cant_viv_techo_gra_ciud=cant_viv_techo_gra_ciud
        self.cant_viv_techo_peq_ciud=cant_viv_techo_peq_ciud
        self.techo_segunda_pasada_gra_ciud=techo_segunda_pasada_gra_ciud
        self.techo_segunda_pasada_peq_ciud=techo_segunda_pasada_peq_ciud
        self.campos=campos_x
        self.uso_falso_cod=uso_falso_cod
        #############conexion##############################
        self.connx = pymssql.connect(self.server, self.user, self.password, self.database)
        #################borra####################
        #shutil.rmtree(path=self.path_segmentacion)
        self.CrearCarpetasSegmentacion()
        self.cant_viv_eleg_segund_pasada=cant_viv_eleg_segund_pasada
        if len(data_x)>0:
            self.where_expression=expresiones_consulta_arcpy.Expresion(data_x, campos_x)


    def Conexion(self):
        self.connx = pymssql.connect(self.server, self.user, self.password, self.database)

    def ObtenerZonasPrueba(self):
        self.Conexion()
        cursor = self.connx.cursor()

        if self.uso_falso_cod==1:
            print "segmentacion con falso cod"
            sql_query = """
                    begin

                    declare @table TABLE(
                    ubigeo varchar(6),
                    zona varchar(6)
                    );

                    select top {} a.ubigeo,a.zona from zona_prueba_segm a
                    inner join (select distinct ubigeo,zona from manzana_prueba_segm where isnull(falso_cod,0)<>0 ) b on a.ubigeo=b.ubigeo and a.zona=b.zona
                    where a.flag_proc=0 --and isnull(a.id_estrato,0)<>0
                    order by 1,2

                    insert @table
                    select top {} a.ubigeo,a.zona from zona_prueba_segm a
                    inner join (select distinct ubigeo,zona from manzana_prueba_segm where isnull(falso_cod,0)<>0 ) b on a.ubigeo=b.ubigeo and a.zona=b.zona
                    where a.flag_proc=0 --and isnull(a.id_estrato,0)<>0
                    order by 1,2

                    update  a
                    set a.flag_proc=1
                    from zona_prueba_segm a
                    where ubigeo+zona in (select ubigeo+zona from @table)

                    end

                    """.format(self.cant_zonas,self.cant_zonas)
        else:

            print "segmentacion sin falso cod"
            sql_query = """
                begin

                declare @table TABLE(
                ubigeo varchar(6),
                zona varchar(6)
                );

                select top {} ubigeo,zona from zona_prueba_segm
                where flag_proc=0
                order by 1,2

                insert @table
                select top {} ubigeo,zona from zona_prueba_segm
                where flag_proc=0
                order by 1,2

                update  a
                set a.flag_proc=1
                from zona_prueba_segm a
                where ubigeo+zona in (select ubigeo+zona from @table)

                end

                """.format(self.cant_zonas, self.cant_zonas)
        print sql_query
        cursor.execute(sql_query)
        self.data = []
        for row in cursor:
            self.data.append(["{}".format(row[0]), "{}".format(row[1])])
        self.connx.commit()
        self.connx.close()



    def ObtenerInformacionPrueba(self):
        self.Conexion()
        cursor = self.connx.cursor()

        #for el in self.data:
        where_zonas=""
        for i,el in enumerate(self.data):
            if i==0:
                where_zonas="'{}{}'".format(el[0],el[1])
            else:
                where_zonas = "{},'{}{}'".format(where_zonas,el[0], el[1])

        sql_query = """
                    select ubigeo,codccpp,zona,isnull(id_estrato,0) id_estrato from zona_prueba_segm WHERE ubigeo+zona in ({})
                    order by 1,2,3
                    """.format(where_zonas)
        print sql_query
        cursor.execute(sql_query)
        self.list_zonas = []

        for row in cursor:
            self.list_zonas.append(row)

        sql_query = """
                    select ubigeo,codccpp,zona,manzana,cant_viv,isnull(falso_cod,0) falso_cod,0 flag_cond,cant_pob from manzana_prueba_segm WHERE ubigeo+zona in ({})
                    order by 1,2,3,4
                    """.format(where_zonas)
        print sql_query
        cursor.execute(sql_query)
        self.list_manzanas = []

        for row in cursor:
            self.list_manzanas.append(row)
        self.connx.close()


        arcpy.env.overwriteOutput = True



        arcpy.CreateTable_management(self.path_segmentacion, 'tb_zonas.dbf')
        list_fields = [['UBIGEO', 'TEXT'],['CODCCPP','TEXT'] ,['ZONA', 'TEXT'],['ID_ESTRATO', 'SHORT'] ,['CANT_POB','SHORT']]
        for field in list_fields:
            arcpy.AddField_management(self.tb_zonas,field[0],field[1])



        arcpy.CreateTable_management(self.path_segmentacion, 'tb_manzanas.dbf')
        list_fields = [['UBIGEO', 'TEXT'],['CODCCPP','TEXT'], ['ZONA', 'TEXT'],['MANZANA', 'TEXT'] ,['CANT_VIV','SHORT'],['FALSO_COD','SHORT'],['FLAG_COND','SHORT'],['CANT_POB','SHORT']]

        for field in list_fields:
            arcpy.AddField_management(self.tb_manzanas,field[0],field[1])


        cursor_insert = arcpy.da.InsertCursor(self.tb_zonas,
                                          ['UBIGEO','CODCCPP' ,'ZONA', 'ID_ESTRATO'])

        for x in self.list_zonas:
            cursor_insert.insertRow((x[0],x[1],x[2],x[3]))



        cursor_insert = arcpy.da.InsertCursor(self.tb_manzanas,
                                              ['UBIGEO', 'CODCCPP','ZONA', 'MANZANA', 'CANT_VIV','FALSO_COD','FLAG_COND','CANT_POB'])

        for x in self.list_manzanas:

            cursor_insert.insertRow((x[0],x[1],x[2],x[3],x[4],int(x[5]) if x[5]!=None else 0 ,x[6],x[7]))




    def CrearCarpetasSegmentacion(self):
        arcpy.env.workspace = self.path_segmentacion

        if os.path.exists(self.path_ini) == False:
            os.mkdir(self.path_ini)

        if os.path.exists(self.path_segmentacion) == False:
            os.mkdir(self.path_segmentacion)



        #else:
        #    shutil.rmtree(path=self.path_segmentacion)
        #    os.mkdir(self.path_segmentacion)
        #if os.path.exists(self.path_segmentacion+ '/tb_secciones') == False:
        #    os.mkdir(self.path_segmentacion + '/tb_secciones')

    #def ImportarTablasTrabajo(self):
    #    arcpy.env.overwriteOutput = True
    #    if arcpy.Exists("CPV_SEGMENTACION.sde") == False:
    #        arcpy.CreateDatabaseConnection_management("Database Connections",
    #                                                  "{}.sde".format(self.database),
    #                                                  "SQL_SERVER",
    #                                                  self.server,
    #                                                  "DATABASE_AUTH",
    #                                                  self.user,
    #                                                  self.password,
    #                                                  "#",
    #                                                  self.database,
    #                                                  "#",
    #                                                  "#",
    #                                                  "#",
    #                                                  "#")
    #    arcpy.env.workspace = "Database Connections/{}.sde".format(self.database)
    #    path_conexion="Database Connections/{}.sde".format(self.database)
#
    #    manzanas_Layer = arcpy.MakeQueryTable_management(path_conexion, 'manzanas',
    #                                                     "SELECT ID,UBIGEO,CODCCPP,ZONA,MANZANA,CANT_VIV, 0  FLAG_COND,FALSO_COD FROM manzana_prueba_segm WHERE {} ".format(
    #                                                         self.where_expression))
    #    manzanas_Layer=arcpy.MakeQueryTable_management(path_conexion,'manzanas',"SELECT ID,UBIGEO,CODCCPP,ZONA,MANZANA,CANT_VIV, 0  FLAG_COND,FALSO_COD FROM manzana_prueba_segm WHERE {} ".format(self.where_expression))








        #zonas_Layer=arcpy.MakeQueryTable_management(path_conexion,'zonas',"select * from CPV_SEGMENTACION.dbo.zona_prueba_segm where {}".format(self.where_expression))


        #manzanas_mfl = arcpy.MakeFeatureLayer_management(manzanas_Layer,"manzanas_mfl")
        #zonas_mfl=arcpy.MakeFeatureLayer_management(zonas_Layer, "zonas_mfl")
#
#
        #arcpy.CopyFeatures_management(manzanas_mfl,self.tb_manzanas)
        #arcpy.CopyFeatures_management(zonas_mfl, self.tb_zonas)
#
#
        #arcpy.TableToTable_conversion(path_conexion + '/CPV_SEGMENTACION.dbo.VW_MZS_CONDOMINIOS', self.path_segmentacion, "tb_mzs_condominios.dbf")
#
        #arcpy.env.workspace = self.path_segmentacion + ""
        #arcpy.DeleteField_management(self.tb_manzanas, ['AEU','IDMANZANA'])
        #arcpy.AddField_management(self.tb_manzanas, "IDMANZANA", "TEXT")
        #expression = "str(!UBIGEO!)+str(!ZONA!)+str(!MANZANA!)"
        #arcpy.CalculateField_management(self.tb_manzanas, "IDMANZANA", expression, "PYTHON_9.3")
        #arcpy.AddField_management(self.tb_manzanas, "AEU", "SHORT")
        #arcpy.AddField_management(self.tb_manzanas, "FLG_MZ", "SHORT")




    def OrdenarManzanasFalsoCod(self):
        if self.uso_falso_cod==1:
            manzanas_ordenadas=arcpy.Sort_management(self.tb_manzanas, self.tb_manzanas_ordenadas, ["UBIGEO", "ZONA", "FALSO_COD"])
        else:
            manzanas_ordenadas = arcpy.Sort_management(self.tb_manzanas, self.tb_manzanas_ordenadas,["UBIGEO", "ZONA", "MANZANA"])


        expression = "flg_manzana(!CANT_VIV!)"
        codeblock = """def flg_manzana(CANT_VIV):\n  if (CANT_VIV>18):\n    return 1\n  else:\n    return 0"""
            #.format(self.cant_viv_techo)
        arcpy.AddField_management(manzanas_ordenadas, "FLG_MZ", "SHORT")
        arcpy.CalculateField_management(manzanas_ordenadas, "FLG_MZ", expression, "PYTHON_9.3", codeblock)
        #########################EL FLG_MZ INDICA SI LA EXPRESION ES MAYOR O MENOR QUE LA CANT DE VIV TECHO




    #def OrdenarViviendas(self):
    #    arcpy.Sort_management(self.tb_viviendas, self.tb_viviendas_ordenadas, ["UBIGEO", "ZONA", "FALSO_COD", "ID_REG_OR"])
    #    arcpy.AddField_management(self.tb_viviendas_ordenadas, "AEU", "SHORT")
    #    arcpy.AddField_management(self.tb_viviendas_ordenadas, "OR_VIV_AEU", "SHORT")
    #    arcpy.AddField_management(self.tb_viviendas_ordenadas, "FLG_CORTE", "SHORT")
    #    arcpy.AddField_management(self.tb_viviendas_ordenadas, "FLG_MZ", "SHORT")


    def ConjuntosConexosCantViviendas(self,ubigeo,zona):
        arcpy.env.workspace = self.path_segmentacion
        conjunto_conexo_cero_final=[]

        if self.uso_falso_cod == 1:
            where="UBIGEO='{}' and ZONA='{}' AND CANT_VIV<={}".format(ubigeo,zona,self.cant_viv_techo)
            list_manzanas = [(x[0],x[1],x[2],x[3],x[4]) for x in arcpy.da.SearchCursor(self.tb_manzanas_ordenadas, ["UBIGEO","ZONA","MANZANA","CANT_VIV","FALSO_COD"],where)]
            conjuntos_conexos_ceros=[]
            conjunto_conexo=[]
            cant_viv_acu = 0
            cod_falso_aux=0



            for el in list_manzanas:

                cod_falso = int(el[4])
                if (cod_falso_aux==cod_falso):

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



            if len(conjunto_conexo) > 0 and (cant_viv_acu==0):
                conjuntos_conexos_ceros.append(conjunto_conexo)

            where = "UBIGEO='{}' and ZONA='{}'".format(ubigeo,zona)




            list_falso_cod=[ x[0] for x in  arcpy.da.SearchCursor(self.tb_manzanas_ordenadas,["FALSO_COD"],where)]


            max_falso_cod=max(list_falso_cod)


            if len(conjuntos_conexos_ceros)>0:
                if max_falso_cod in conjuntos_conexos_ceros[-1]:
                    conjunto_conexo_cero_final=conjuntos_conexos_ceros[-1][:]


            return conjunto_conexo_cero_final

        else:
            where = "UBIGEO='{}' and ZONA='{}' ".format(ubigeo, zona)
            #where = "UBIGEO='{}' and ZONA='{}' AND CANT_VIV<={}".format(ubigeo, zona, self.cant_viv_techo)
            list_manzanas = [[x[0], x[1], x[2], x[3], x[4]] for x in arcpy.da.SearchCursor(self.tb_manzanas_ordenadas,
                                                                                           ["UBIGEO", "ZONA", "MANZANA",
                                                                                            "CANT_VIV", "FALSO_COD"],
                                                                                           where)]
            conjuntos_conexos=[]
            conjuntos_conexos_ceros = []
            conjunto_conexo = []
            #cant_viv_acu = 0
            cod_falso_aux = ''

            list_manzanas_aux=list_manzanas[:]



            conjunto_conexo_cant_viv_men=[]

            while len(list_manzanas_aux)>0:
                el=list_manzanas_aux[0]

                cant_viv=int(el[3])
                #print self.cant_viv_techo

                if (cant_viv<=self.cant_viv_techo):
                    conjunto_conexo_cant_viv_men.append(el)
                    #print conjunto_conexo_cant_viv_men
                else:

                    #print conjunto_conexo_cant_viv_men
                    if len(conjunto_conexo_cant_viv_men)>0:
                        conjuntos_conexos.append(conjunto_conexo_cant_viv_men)


                    conjunto_conexo_cant_viv_men=[]



                list_manzanas_aux.remove(el)
            ##########obtienes todos los conjuntos conexos cuya canti de viv totales suman 0


            if len(conjunto_conexo_cant_viv_men)>0:
                conjuntos_conexos.append(conjunto_conexo_cant_viv_men)

            #print 'conjuntos conexos',conjuntos_conexos


            for conjunto in conjuntos_conexos:
                cant_viv_acu=0
                for el in conjunto:
                    cant_viv_acu=cant_viv_acu+int(el[3])

                if cant_viv_acu==0:
                    conjuntos_conexos_ceros.append(conjunto)



            #print 'conjuntos conexos ceros' ,conjuntos_conexos_ceros

            #where = "UBIGEO='{}' and ZONA='{}'".format(ubigeo, zona)

            #list_falso_cod = [x[0] for x in arcpy.da.SearchCursor(self.tb_manzanas_ordenadas, ["MANZANA"], where)]

            ultima_manzana = list_manzanas[-1]

            if len(conjuntos_conexos_ceros) > 0:
                if ultima_manzana in conjuntos_conexos_ceros[-1]:

                    for el in conjuntos_conexos_ceros[-1][:]:
                        conjunto_conexo_cero_final.append(el[2])
                    #conjunto_conexo_cero_final = conjuntos_conexos_ceros[-1][:]

            print conjunto_conexo_cero_final

            if len(conjunto_conexo_cero_final)==len(list_manzanas):
                conjunto_conexo_cero_final=[]

            return conjunto_conexo_cero_final




    def EnumerarAEUEnViviendasDeManzanas(self):
        arcpy.env.overwriteOutput = True
        arcpy.CreateTable_management(self.path_segmentacion,'tb_rutas.dbf')
        list_fields=[['UBIGEO','TEXT'],['ZONA','TEXT'],['MANZANA','TEXT'],['AEU','SHORT'],['CANT_VIV','SHORT'],['TECHO','SHORT'],['FLG_MZ','SHORT'],['TECHO_S_P','SHORT'],['FALSO_COD','SHORT'],['CANT_POB','SHORT']]
        cursor_insert = arcpy.da.InsertCursor(self.tb_rutas, ['UBIGEO','ZONA','MANZANA','AEU','CANT_VIV','TECHO','FLG_MZ','FALSO_COD','CANT_POB'],)



        for field in list_fields:
            arcpy.AddField_management(self.tb_rutas,field[0],field[1])

        lista_zonas=[ (x[0],x[1],x[2])for x   in arcpy.da.SearchCursor(self.tb_zonas, ["UBIGEO", "ZONA","ID_ESTRATO"], self.where_expression)]

        #lista_manzanas=[ (x[0],x[1])for x   in arcpy.da.SearchCursor(tb_manzanas_ordenadas, ["UBIGEO", "ZONA", "MANZANA", "FALSO_COD", "VIV_MZ", "MZS_COND"], where_expression)]

        lista_rutas=[]


        for zona in lista_zonas:
            conjunto_conexo_final_cero=self.ConjuntosConexosCantViviendas(ubigeo=zona[0],zona=zona[1])[:]
            #print conjunto_conexo_final_cero
            where_expression1 = "UBIGEO='{}' and ZONA='{}'".format(str(zona[0]),str(zona[1]))
            numero_aeu = 1
            cant_vivi_agrupadas = 0
            anterior_manzana = 0

            if int(zona[2]==1):
                self.cant_viv_techo=self.cant_viv_techo_gra_ciud
            else:
                self.cant_viv_techo = self.cant_viv_techo_peq_ciud
            #self.cant_viv_techo=18
            #fields = arcpy.ListFields(self.tb_manzanas_ordenadas)

            #for field in fields:
            #    print "{}".format(field.name)

            for row1 in arcpy.da.SearchCursor(self.tb_manzanas_ordenadas,["UBIGEO", "ZONA", "MANZANA", "FALSO_COD", "CANT_VIV", "FLAG_COND","CANT_POB"],where_expression1) :

                cant_viv = int(row1[4])
                ubigeo=row1[0]
                zona=row1[1]
                manzana=row1[2]
                cant_pob = int(row1[6])
                if (cant_viv > 18):
                    mzs_cond = int(row1[5])
                    # Aqui se hace referencia a la manzana anterior

                    if anterior_manzana == 1:  # la anterior manzana es una menor o igual a 16 viviendas
                        if cant_vivi_agrupadas != 0:
                            numero_aeu = numero_aeu + 1

                    cant_vivi_agrupadas = 0
                    anterior_manzana = 2  ##si la manzana es con mas de 16 viv entonces 2

                    if (mzs_cond >= 0):
                        division = float(cant_viv) / self.cant_viv_techo
                        cant_aeus = int(math.ceil(division))
                        residuo = cant_viv % cant_aeus
                        viv_aeu = cant_viv / cant_aeus
                        cant_pob_ruta = math.ceil(float(cant_pob) / cant_aeus)
                        i = 0
                        cant_aeus_aux = 0

                        for i in range(cant_aeus):
                            if residuo > 0:

                                lista_rutas.append([ubigeo,zona,manzana,numero_aeu,viv_aeu+1,self.cant_viv_techo,1,self.uso_falso_cod,cant_pob_ruta])
                                cursor_insert.insertRow((ubigeo,zona,manzana,numero_aeu,viv_aeu+1,self.cant_viv_techo,1,self.uso_falso_cod,cant_pob_ruta))

                                numero_aeu_anterior = numero_aeu
                                numero_aeu = numero_aeu + 1
                                residuo=residuo-1
                                cant_aeus_aux = cant_aeus_aux + 1

                            else:
                                lista_rutas.append([ubigeo, zona, manzana, numero_aeu, viv_aeu,self.cant_viv_techo,1,self.uso_falso_cod,cant_pob_ruta])
                                cursor_insert.insertRow((ubigeo, zona, manzana, numero_aeu, viv_aeu,self.cant_viv_techo,1,self.uso_falso_cod,cant_pob_ruta))
                                numero_aeu_anterior = numero_aeu
                                numero_aeu = numero_aeu + 1
                                cant_aeus_aux = cant_aeus_aux + 1

    #                else:
    #
    #                    condominio_anterior = 0
    #                    numero_aeu_anterior = 0
    #
    #                    for condominios in [[str(x[0]), str(x[1]), str(x[2]), int(x[3]), int(x[4])] for x in
    #                                        arcpy.da.SearchCursor(tb_mzs_condominios,
    #                                                              ["UBIGEO", "ZONA", "MANZANA", "CONDOMINIO",
    #                                                               "VIV_COND"], where_expression_viv)]:
    #                        cant_viv_cond = condominios[4]
    #
    #                        if (cant_viv_cond == 0):
    #                            cant_aeu_condominio = 1
    #                            viv_aeu_condominio = 0
    #                            res_viv_condominio = 0
    #                        else:
    #                            ##########cant aeu_condominio es la cantidad de aeus por block ########################
    #                            cant_aeu_condominio = int(math.ceil(float(condominios[4]) / cant_viv_techo))
    #                            ##########viv_aeu_block es la cantidad de viviendas por block#####################
    #                            viv_aeu_condominio = int(condominios[4]) / int(cant_aeu_condominio)
    #                            ##########res_viv_block es el residuo de viviendas por block######################
    #
    #                            res_viv_condominio = int(condominios[4]) % int(cant_aeu_condominio)
    #
    #                        or_viv_aeu = 1
    #
    #                        where_expression_viv_cond = " UBIGEO=\'" + condominios[0] + "\'  AND  ZONA=\'" + \
    #                                                    condominios[1] + "\' AND MANZANA=\'" + condominios[
    #                                                        2] + "\' AND  P19A=" + str(condominios[3])
    #
    #                        print  where_expression_viv_cond
    #
    #                        with arcpy.da.UpdateCursor(tb_viviendas,
    #                                                   ["UBIGEO", "ZONA", "MANZANA", "ID_REG_OR", "AEU", "OR_VIV_AEU",
    #                                                    "P19A",
    #                                                    "P29", "FLG_MZ", "P23"], where_expression_viv_cond) as cursor2:
    #                            for row2 in cursor2:
    #                                # flg manzana en 1
    #                                row2[8] = 1
    #                                idmanzana = str(row2[0]) + str(row2[1]) + str(row2[2])
    #                                usolocal = int(row2[7])
    #
    #                                condominio = int(row2[6])
    #
    #                                # condominio=int(row2[9])
    #                                # print  bloque
    #                                if (usolocal in [1, 3]):
    #                                    row2[4] = numero_aeu
    #                                    row2[5] = or_viv_aeu
    #                                    or_viv_aeu = or_viv_aeu + 1
    #                                elif (usolocal == 6):
    #                                    row2[4] = numero_aeu
    #
    #                                else:
    #                                    if or_viv_aeu != 1:
    #                                        row2[4] = numero_aeu
    #
    #                                    else:
    #
    #                                        if condominio == condominio_anterior:
    #                                            row2[4] = numero_aeu_anterior
    #                                        else:
    #                                            row2[4] = numero_aeu
    #
    #                                if res_viv_condominio > 0:
    #
    #                                    if or_viv_aeu > (viv_aeu_condominio + 1):
    #                                        i = 1
    #                                        condominio_anterior = condominio
    #                                        numero_aeu_anterior = numero_aeu
    #                                        idmanzana_anterior = idmanzana
    #                                        numero_aeu = numero_aeu + 1
    #                                        res_viv_condominio = res_viv_condominio - 1
    #                                        or_viv_aeu = 1
    #
    #                                else:
    #
    #                                    if or_viv_aeu > (viv_aeu_condominio):
    #                                        condominio_anterior = condominio
    #                                        numero_aeu_anterior = numero_aeu
    #                                        numero_aeu = numero_aeu + 1
    #                                        idmanzana_anterior = idmanzana
    #                                        or_viv_aeu = 1
    #
    #                                cursor2.updateRow(row2)
    #                        del cursor2

                else:
                    cant_vivi_agrupadas = cant_vivi_agrupadas + cant_viv
                    cod_falso = int(row1[3])

                    if (anterior_manzana == 2 or anterior_manzana == 0):  #
                        cant_vivi_agrupadas = cant_viv  # cantidad  de viviendas del grupo regrewsa a 0 y se almacena la cantidad de viviendas
                        or_viv_aeu = 1

                        if len(conjunto_conexo_final_cero) > 0:
                            if self.uso_falso_cod == 1:
                                if cod_falso in conjunto_conexo_final_cero:  ## si la manzana esta en el ultimo conjunto conexo
                                    numero_aeu = numero_aeu - 1
                            else:
                                print conjunto_conexo_final_cero
                                print 'manzana', manzana
                                print numero_aeu

                                if manzana in conjunto_conexo_final_cero:  ## si la manzana esta en el ultimo conjunto conexo
                                    print 'manzana' ,manzana


                                    numero_aeu = numero_aeu - 1
                                    print 'manzana', numero_aeu

                    else:
                        if (cant_vivi_agrupadas <= self.cant_viv_techo) or (cant_viv==0):
                            numero_aeu = numero_aeu

                            # or_viv_aeu

                        else:
                            cant_vivi_agrupadas = cant_viv
                            numero_aeu = numero_aeu + 1
                            or_viv_aeu = 1

                    anterior_manzana = 1  ##si la manzana es menor igual a 16 viv entonces la anterior manzana tiene valor 1


                    lista_rutas.append([ubigeo,zona,manzana,numero_aeu,cant_viv,self.cant_viv_techo,0,self.uso_falso_cod,cant_pob])
                    cursor_insert.insertRow((ubigeo, zona, manzana, numero_aeu, cant_viv,self.cant_viv_techo,0,self.uso_falso_cod,cant_pob))

    def CrearAEUS(self):

        arcpy.Statistics_analysis(self.tb_rutas, self.tb_aeus, [["CANT_VIV", "SUM"],["CANT_POB","SUM"]], ["UBIGEO", "ZONA",  "AEU","TECHO","TECHO_S_P","FALSO_COD"])

        arcpy.AddField_management(self.tb_aeus, "CANT_VIV", "SHORT")
        arcpy.CalculateField_management(self.tb_aeus, "CANT_VIV",
                                        "[SUM_CANT_V]", "VB", "")
        arcpy.DeleteField_management(self.tb_aeus,["SUM_CANT_V"])

        arcpy.AddField_management(self.tb_aeus, "CANT_POB", "SHORT")
        arcpy.CalculateField_management(self.tb_aeus, "CANT_POB",
                                        "[SUM_CANT_P]", "VB", "")

        arcpy.Copy_management(self.tb_aeus,self.path_ini+'/tb_aeus_temp.dbf'  )


    def SegundaPasada(self):
        #arcpy.AddField_management(self.tb_rutas, "TECHO_S_P", "SHORT")
        resumen_zona=arcpy.Statistics_analysis(self.tb_aeus, 'in_memory/zonas_aeu', [["AEU", "MAX"]],
                                  ["UBIGEO", "ZONA"])
        zonas=[ (x[0],x[1],x[2]) for x in arcpy.da.SearchCursor(resumen_zona,["UBIGEO","ZONA","MAX_AEU"])]



        for zona in zonas:
            where_zona=" UBIGEO='{}' AND ZONA='{}' ".format(zona[0],zona[1])



            id_estrato=[ x[0] for x in arcpy.da.SearchCursor(self.tb_zonas,['ID_ESTRATO'],where_zona)][0]

            cant_aeus_zona=int(zona[2])
            #id_estrato=int(zona[3])
            if id_estrato==1:
                self.techo_segunda_pasada=self.techo_segunda_pasada_gra_ciud
            else:
                self.techo_segunda_pasada=self.techo_segunda_pasada_peq_ciud


            #where_aeus_elegidos=" UBIGEO='{}' AND ZONA='{}' AND CANT_VIV<=9 AND FLG_MZ=0".format(zona[0],zona[1])
            where_aeus_elegidos = " UBIGEO='{}' AND ZONA='{}' AND CANT_VIV<={} ".format(zona[0], zona[1],self.cant_viv_eleg_segund_pasada)
            where_aeus = " UBIGEO='{}' AND ZONA='{}' ".format(zona[0], zona[1])

            ############################diccionario de datos#############################

            #temp_aeus_elegidos = [(x[0], x[1], x[2], x[3], x[4], x[5]) for x in
            #             arcpy.SearchCursor(self.tb_aeus, ["UBIGEO", "ZONA", "AEU", "CANT_VIV", "TECHO", "FLG_MZ"],where_aeus_elegidos)]



            dic_aeus_elegidos = dict ( ( x[2], [x[3], x[4]] ) for x in arcpy.da.SearchCursor(self.tb_aeus,["UBIGEO", "ZONA", "AEU", "CANT_VIV", "TECHO"],where_aeus_elegidos))
            aeus_eleg_l=dic_aeus_elegidos.keys()



            aeus_eleg=sorted(aeus_eleg_l)[:]
            #print aeus_eleg

            #temp_aeus=[(x[0], x[1], x[2], x[3], x[4], x[5]) for x in arcpy.SearchCursor(self.tb_aeus, ["UBIGEO", "ZONA", "AEU", "CANT_VIV", "TECHO", "FLG_MZ"],where_aeus)]

            dic_aeus = dict((x[2], [x[3], x[4]]) for x in arcpy.da.SearchCursor(self.tb_aeus, ["UBIGEO", "ZONA", "AEU", "CANT_VIV", "TECHO"],where_aeus))

            aeus=dic_aeus.keys()

            aeus_temp=aeus[:]
            aeus_opciones=aeus[:]
            #print aeus
            #dict((key, value) for key, value in miLista)

            if len(aeus)>1:
                if int(len(aeus_eleg)>0):
                    while int(len(aeus_eleg)>0):
                        aeu_eleg=aeus_eleg[0]
                        aeus_eleg.remove(aeu_eleg)
                    #for aeu_eleg in aeus_eleg:
                        datos_aeu_eleg=dic_aeus_elegidos[aeu_eleg]
                        aeu=int(aeu_eleg)
                        cant_viv=int(datos_aeu_eleg[0])
                        #techo = int(datos_aeu_eleg[1])


                        if (aeu > 1 and aeu < cant_aeus_zona):
                            aeu_op_anterior=aeu-1
                            aeu_op_posterior=aeu+1

                            temp_aeu_anterior=dic_aeus[aeu_op_anterior]
                            temp_aeu_posterior = dic_aeus[aeu_op_posterior]

                            cant_viv_anterior=int(temp_aeu_anterior[0])+cant_viv
                            cant_viv_posterior=int(temp_aeu_posterior[0])+cant_viv


                            if (  (cant_viv_anterior <=self.techo_segunda_pasada) and (cant_viv_posterior<=self.techo_segunda_pasada) ):
                                if( cant_viv_anterior<=cant_viv_posterior ):
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
                                        aeu_selec =-1
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
                                        aeu_selec =-1
                            elif ( (cant_viv_anterior <=self.techo_segunda_pasada)):
                                if aeu_op_anterior in aeus_opciones:
                                    aeu_selec = aeu_op_anterior
                                    aeus_opciones.remove(aeu_selec)
                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)
                                else:
                                    aeu_selec = -1

                            elif ( (cant_viv_posterior <=self.techo_segunda_pasada)):
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

                        elif (aeu==1):
                            aeu_op_posterior =aeu+1
                            temp_aeu_posterior = dic_aeus[aeu_op_posterior]
                            cant_viv_posterior = int(temp_aeu_posterior[0]) + cant_viv
                            #print cant_viv_posterior

                            if(cant_viv_posterior<=self.techo_segunda_pasada):
                                if aeu_op_posterior in aeus_opciones:
                                    aeu_selec = aeu_op_posterior
                                    aeus_opciones.remove(aeu_selec)
                                    #print aeu_selec

                                    aeus_temp[(aeus_temp.index(aeu))] = aeu_selec
                                    #print aeus_temp

                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)


                        else :
                            aeu_op_anterior = aeu - 1
                            temp_aeu_anterior = dic_aeus[aeu_op_anterior]
                            cant_viv_anterior = int(temp_aeu_anterior[0]) + cant_viv
                            if (cant_viv_anterior <= self.techo_segunda_pasada):
                                if aeu_op_anterior in aeus_opciones:
                                    aeu_selec = aeu_op_anterior
                                    aeus_opciones.remove(aeu_selec)
                                    aeus_temp[(aeus_temp.index(aeu))] = aeu_selec
                                    if aeu_selec in aeus_eleg:
                                        aeus_eleg.remove(aeu_selec)


            #print aeus_temp
            aeus_renumerados=[]

            i=0
            temp_anterior=0

            for x in aeus_temp:
                if temp_anterior!=x:
                    i=i+1
                aeus_renumerados.append(i)
                temp_anterior=x

            with arcpy.da.UpdateCursor(self.tb_rutas,["AEU","TECHO_S_P"],where_aeus) as cursor:
                for x in cursor:
                    indice=int(x[0])-1
                    x[0]=aeus_renumerados[indice]
                    x[1]=self.techo_segunda_pasada
                    cursor.updateRow(x)

            #print aeus_renumerados



    def CrearAEUSSegundaPasada(self):
        arcpy.Statistics_analysis(self.tb_rutas, self.tb_aeus, [["CANT_VIV", "SUM"],["CANT_POB","SUM"]], ["UBIGEO", "ZONA", "AEU","TECHO","TECHO_S_P","FALSO_COD"])
        arcpy.AddField_management(self.tb_aeus, "CANT_VIV", "SHORT")
        arcpy.CalculateField_management(self.tb_aeus, "CANT_VIV",
                                        "[SUM_CANT_V]", "VB", "")
        arcpy.AddField_management(self.tb_aeus, "CANT_POB", "SHORT")
        arcpy.CalculateField_management(self.tb_aeus, "CANT_POB",
                                        "[SUM_CANT_P]", "VB", "")

        arcpy.DeleteField_management(self.tb_aeus,["SUM_CANT_V"])



    def InsertarRegistros(self):
        arcpy.env.workspace = "Database Connections/PruebaSegmentacion.sde"
        arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)
        if arcpy.Exists("GEODATABASE.sde") == False:
            arcpy.CreateDatabaseConnection_management("Database Connections",
                                                      "GEODATABASE.sde",
                                                      "SQL_SERVER",
                                                      self.server,
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
        segm_ruta_prueba = path_conexion2 + "/GEODB_CPV_SEGM.SDE.SEGM_RUTA_PRUEBA"
        segm_aeu_prueba = path_conexion2 + "/GEODB_CPV_SEGM.SDE.SEGM_AEU_PRUEBA"

        list_capas=[[self.tb_rutas,segm_ruta_prueba],[self.tb_aeus,segm_aeu_prueba]]
        i=0

        data=[]
        for x in arcpy.da.SearchCursor(self.tb_zonas,["UBIGEO","ZONA","ID_ESTRATO"]):
            if int(x[2])==1:
                data.append([x[0],x[1],self.cant_viv_techo_gra_ciud,self.techo_segunda_pasada_gra_ciud,self.uso_falso_cod])

            else:
                data.append([x[0], x[1], self.cant_viv_techo_peq_ciud, self.techo_segunda_pasada_peq_ciud,self.uso_falso_cod])

        where_final=expresiones_consulta_arcpy.Expresion_2(data, [['UBIGEO', 'TEXT'], ['ZONA', 'TEXT'], ['TECHO', 'SHORT'], ['TECHO_S_P', 'SHORT'], ['FALSO_COD', 'SHORT']])

        print  where_final
        for el in list_capas:
            i=i+1
            #a = arcpy.MakeTableView_management(el[1], "a{}".format(i),  "({}) and TECHO={}".format(self.where_expression,self.cant_viv_techo_gra_ciud))
            a = arcpy.MakeTableView_management(el[1], "a{}".format(i),where_final)

            #print int(arcpy.GetCount_management(a).getOutput(0))
            if (int(arcpy.GetCount_management(a).getOutput(0)) > 0):
                arcpy.DeleteRows_management(a)

            #b = arcpy.MakeTableView_management(el[0], "b{}".format(i),  "({}) and TECHO={}".format(self.where_expression,self.cant_viv_techo_gra_ciud) )
            b = arcpy.MakeTableView_management(el[0], "b{}".format(i),where_final)
            arcpy.Append_management(b, el[1], "NO_TEST")



    def Segmentar(self):

        if (len(self.data)==0):
            self.ObtenerZonasPrueba()

        if (len(self.data)>0):
            self.where_expression = expresiones_consulta_arcpy.Expresion(self.data, self.campos)
            print datetime.today()

            self.ObtenerInformacionPrueba()

            print datetime.today()
            self.OrdenarManzanasFalsoCod()
            print datetime.today()
            self.EnumerarAEUEnViviendasDeManzanas()
            print datetime.today()
            self.CrearAEUS()
            print datetime.today()
            if (self.techo_segunda_pasada_gra_ciud>0 and self.techo_segunda_pasada_peq_ciud>0):
                print 'segunda pasada'
                self.SegundaPasada()
                print datetime.today()
                self.CrearAEUSSegundaPasada()
                print datetime.today()

            self.InsertarRegistros()
            print datetime.today()

        return len(self.data)


data=[
    #['040122','01600']
]



def ResetearZonasPrueba():
    server = "172.18.1.41"
    database="CPV_SEGMENTACION"
    user = "us_arcgis_seg_2"
    password = "b8an!hUse8P-"
    connx = pymssql.connect(server, user, password, database)
    cursor = connx.cursor()
    sql_query = """


begin

  update  a
  set a.flag_proc=0
  from zona_prueba_segm a


end

        """
    cursor.execute(sql_query)

    connx.commit()
    connx.close()


#s = Segmentacion(cant_viv_techo_gra_ciud=16, cant_viv_techo_peq_ciud=16, techo_segunda_pasada_gra_ciud=0,
#                 techo_segunda_pasada_peq_ciud=0, cant_zonas_x=100, data_x=data, uso_falso_cod=0)
#s.Segmentar()





#ResetearZonasPrueba()
#
#for x in range(70):
#    s=Segmentacion(cant_viv_techo_gra_ciud=16,cant_viv_techo_peq_ciud=16,techo_segunda_pasada_gra_ciud=17,techo_segunda_pasada_peq_ciud=17,cant_zonas_x=100,data_x=data,uso_falso_cod=0,cant_viv_eleg_segund_pasada=9)
#    s.Segmentar()
#
#ResetearZonasPrueba()
#
#for x in range(70):
#    s = Segmentacion(cant_viv_techo_gra_ciud=16, cant_viv_techo_peq_ciud=16, techo_segunda_pasada_gra_ciud=18,
#                         techo_segunda_pasada_peq_ciud=18, cant_zonas_x=100, data_x=data, uso_falso_cod=0,
#                         cant_viv_eleg_segund_pasada=9)
#    s.Segmentar()




ResetearZonasPrueba()
for x in range(70):
    s = Segmentacion(cant_viv_techo_gra_ciud=17, cant_viv_techo_peq_ciud=17, techo_segunda_pasada_gra_ciud=17,
                         techo_segunda_pasada_peq_ciud=17, cant_zonas_x=100, data_x=data, uso_falso_cod=0,
                         cant_viv_eleg_segund_pasada=9)
    s.Segmentar()
ResetearZonasPrueba()
for x in range(70):
    s = Segmentacion(cant_viv_techo_gra_ciud=17, cant_viv_techo_peq_ciud=17, techo_segunda_pasada_gra_ciud=18,
                         techo_segunda_pasada_peq_ciud=18, cant_zonas_x=100, data_x=data, uso_falso_cod=0,
                         cant_viv_eleg_segund_pasada=9)
    s.Segmentar()
ResetearZonasPrueba()
for x in range(70):
        s = Segmentacion(cant_viv_techo_gra_ciud=18, cant_viv_techo_peq_ciud=18, techo_segunda_pasada_gra_ciud=18,
                         techo_segunda_pasada_peq_ciud=18, cant_zonas_x=100, data_x=data, uso_falso_cod=0,
                         cant_viv_eleg_segund_pasada=9)
        s.Segmentar()



#for x in range(60):
#    s=Segmentacion(cant_viv_techo_gra_ciud=17,cant_viv_techo_peq_ciud=17,techo_segunda_pasada_gra_ciud=16,techo_segunda_pasada_peq_ciud=16,cant_zonas_x=100,data_x=data,uso_falso_cod=0,cant_viv_eleg_segund_pasada=9)
#    s.Segmentar()



#ResetearZonasPrueba()
#for x in range(60):
#    s=Segmentacion(cant_viv_techo_gra_ciud=17,cant_viv_techo_peq_ciud=17,techo_segunda_pasada_gra_ciud=0,techo_segunda_pasada_peq_ciud=0,cant_zonas_x=100,data_x=data,uso_falso_cod=0)
#    s.Segmentar()
#
#ResetearZonasPrueba()
#
#for x in range(60):
#    s=Segmentacion(cant_viv_techo_gra_ciud=18,cant_viv_techo_peq_ciud=18,techo_segunda_pasada_gra_ciud=0,techo_segunda_pasada_peq_ciud=0,cant_zonas_x=100,data_x=data,uso_falso_cod=0)
#    s.Segmentar()



#ResetearZonasPrueba()
#for x in range(1):
#    s=Segmentacion(cant_viv_techo_gra_ciud=14,cant_viv_techo_peq_ciud=14,techo_segunda_pasada_gra_ciud=0,techo_segunda_pasada_peq_ciud=0,cant_zonas_x=1,data_x=data,uso_falso_cod=1)
#    s.Segmentar()



#ResetearZonasPrueba()
#for x in range(40):
#    s=Segmentacion(cant_viv_techo_gra_ciud=15,cant_viv_techo_peq_ciud=15,techo_segunda_pasada_gra_ciud=0,techo_segunda_pasada_peq_ciud=0,cant_zonas_x=100,data_x=data,uso_falso_cod=1)
#    s.Segmentar()
#
#
#ResetearZonasPrueba()
#for x in range(60):
#    s=Segmentacion(cant_viv_techo_gra_ciud=14,cant_viv_techo_peq_ciud=14,techo_segunda_pasada_gra_ciud=0,techo_segunda_pasada_peq_ciud=0,cant_zonas_x=100,data_x=data,uso_falso_cod=0)
#    s.Segmentar()
#
#
#
#ResetearZonasPrueba()
#for x in range(60):
#    s=Segmentacion(cant_viv_techo_gra_ciud=15,cant_viv_techo_peq_ciud=15,techo_segunda_pasada_gra_ciud=0,techo_segunda_pasada_peq_ciud=0,cant_zonas_x=100,data_x=data,uso_falso_cod=0)
#    s.Segmentar()
#