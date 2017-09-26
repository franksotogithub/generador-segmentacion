# -*- coding: utf-8 -*-
import sys
import listado_urbano as listado
import conection_sql as conx
import math
import pyPdf
import os

import pymssql
#server = "172.18.1.41"

path_proyecto_segm = "D:/proyecto-segmentacion-urbana/"
path_ini = "D:/proyecto-segmentacion-urbana/segmentacion"
path_out = "D:"
path_croquis=path_out + "\\croquis"
path_listados=path_out + "\\listados"
path_croquis_listado=path_out + "\\croquis-listado"
path_etiquetas=path_out + "\\etiquetas"
path_out_final="\\\\192.168.201.115\\cpv2017\\croquis-listado\\urbano"
#path_urbano_listados_prov= path_out_final+"\\reportes-provinciales-urbanos"
#path_urbano_listados_dep= path_out_final+"\\reportes-departamentales-urbanos"

path_urbano_listados_prov= path_out_final+"\\REPORTES-PROVINCIALES"
path_urbano_listados_dep= path_out_final+"\\REPORTES-DEPARTAMENTALES"





def obtener_informacion_reporte(ccdd,ccpp,fase='CPV2017'):
    conn = conx.Conexion()
    cursor = conn.cursor()
    data=[]
    cabecera=[]


    #if idsubprov == '':
    #    sql_query_cab = """ select nombdep_censal,nombsubdep_censal  from dbo.SUBDEPARTAMENTO_CENSAL  where  idsubdep_censal='{idsubdep}'  """.format(idsubdep=idsubdep)
    #else:
    #    sql_query_cab = """ select nombdep_censal,nombprov_censal,nombsubprov_censal from dbo.SUBPROVINCIA_CENSAL  where idsubdep_censal='{idsubdep}' and idsubprov_censal='{idsubprov}' """.format(
    #        idsubdep=idsubdep,idsubprov=idsubprov)

    if ccpp == '':
        sql_query_cab = """ select  nombdep,'' nombsubdep_censal  from dbo.MARCO_DEPARTAMENTO  where  ccdd='{ccdd}' and fase='{fase}'  """.format(ccdd=ccdd,fase=fase)
        where = """ where a.fase='{fase}' and a.ccdd='{ccdd}' """.format(ccdd=ccdd, fase=fase)
    else:
        sql_query_cab = """ select nombdep,nombprov,'' nombsubprov_censal from dbo.MARCO_PROVINCIA  where ccdd='{ccdd}'  and ccpp='{ccpp}'  and fase='{fase}' """.format(ccdd=ccdd ,ccpp=ccpp,fase=fase)
        where = """ where a.fase='{fase}' and a.ccdd='{ccdd}' and a.ccpp='{ccpp}' """.format(ccdd=ccdd, ccpp=ccpp,
                                                                                             fase=fase)



    cursor.execute(sql_query_cab)

    for row in cursor:
        cabecera=row

    #if idsubprov == '':
    #    where=""" where a.fase='CPV2017' and a.idsubdep_censal='{idsubdep}' """.format(idsubdep=idsubdep)
    #else:
    #    where = """ where a.fase='CPV2017' and a.idsubdep_censal='{idsubdep}' and a.idsubprov_censal='{idsubprov}' """.format(idsubdep=idsubdep,idsubprov=idsubprov)






    sql_query_data = """


        SELECT '','','','','','',SUM(a.n_zonas) n_zonas,SUM(a.n_subzonas) n_subzonas,SUM(a.n_secc)n_secc,SUM(a.n_aeu)n_aeu ,SUM(a.n_mzs)n_mzs,SUM(a.n_viv)n_viv,'000000' ubigeo
        from
        (
        select A.CCDD,A.CCPP,A.NOMBPROV provincia,'' subprovincia,a.nombdist distrito,b.nombdistope subdistrito,count(distinct c.idzona) n_zonas,
        sum(c.cant_subzonas)n_subzonas,sum(c.cant_secc_u) n_secc,sum(c.cant_ae_u)n_aeu, sum(c.cant_mzs_marco) n_mzs,sum(c.cant_viv_marco) n_viv,A.UBIGEO
        from marco_distrito a
        inner join distrito_ope b on a.ubigeo=b.ubigeo  and a.fase=b.fase
        inner join marco_zona c on c.ubigeo=b.ubigeo and c.distope=b.distope and b.fase=c.fase
        {where}
        group by A.CCDD,A.CCPP,A.NOMBPROV, a.nombdist ,b.nombdistope,a.ubigeo
        ) a

        union all
        select A.CCDD,A.CCPP,A.NOMBPROV provincia,'' subprovincia,a.nombdist distrito,b.nombdistope subdistrito,count(distinct c.idzona) n_zonas,
        sum(c.cant_subzonas)n_subzonas,sum(c.cant_secc_u) n_secc,sum(c.cant_ae_u)n_aeu, sum(c.cant_mzs_marco) n_mzs,sum(c.cant_viv_marco) n_viv,A.UBIGEO
        from marco_distrito a
        inner join distrito_ope b on a.ubigeo=b.ubigeo  and a.fase=b.fase
        inner join marco_zona c on c.ubigeo=b.ubigeo and c.distope=b.distope and b.fase=c.fase
        {where}
        group by A.CCDD,A.CCPP,A.NOMBPROV, a.nombdist ,b.nombdistope,a.ubigeo
        order by 13

    """.format(where=where)
    cursor.execute(sql_query_data)

    for row in cursor:
        data.append(row)

    conn.commit()
    conn.close()


    return [cabecera,data]


def listar_subprovincias(ccdd,fase='CPV2017'):
    conn=conx.Conexion()
    cursor = conn.cursor()
    #sql="""select idsubprov_censal  from dbo.SUBPROVINCIA_CENSAL b
    #            where b.idsubdep_censal='{}'    """.format(idsubdep,fase)

    sql = """select ccpp  from dbo.marco_provincia b
                where ccdd='{ccdd}'  and fase='{fase}'   """.format(ccdd=ccdd, fase=fase)


    cursor.execute(sql)
    list_sub_prov = []

    for row in cursor:
         list_sub_prov.append(row[0])
    return list_sub_prov
    conn.close()



def exportar_listados( fase='CPV2017'):
    conn=conx.Conexion()
    cursor = conn.cursor()
    sql = """
        select ccdd   from dbo.marco_departamento b where fase='{fase}'
         """.format(fase=fase)
    cursor.execute(sql)

    for row in cursor:
        idsubdep = row[0]
        list_subprovincias = listar_subprovincias(idsubdep)
        informacion=obtener_informacion_reporte(ccdd=idsubdep, ccpp='', fase='CPV2017')
        numsubdep=informacion[0][0]

        if len(informacion[1]) > 0:

            out_final = os.path.join(path_urbano_listados_dep, u'{}-{}.pdf'.format(idsubdep,numsubdep))


            listado.listado_depart_prov(informacion=informacion,nivel=1,output=out_final)


            for idsubprov in list_subprovincias:
                informacion_2=obtener_informacion_reporte(ccdd=idsubdep,ccpp=idsubprov, fase='CPV2017')
                if informacion_2[1]>0:
                    numsubprov = informacion_2[0][1]
                    out_final = os.path.join(path_urbano_listados_prov, u'{}{}-{}.pdf'.format(idsubdep,idsubprov,numsubprov))
                    listado.listado_depart_prov(informacion=informacion_2, nivel=2, output=out_final)


exportar_listados()