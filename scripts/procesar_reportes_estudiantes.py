import sys
import listado_urbano as listado
import conection_sql as conx
import math
import pyPdf

import pymssql
#server = "172.18.1.41"



path_out_final="\\\\192.168.201.115\\cpv2017"
#path_urbano_croquis_listado = path_out_final + "\\croquis-listado\\urbano"
path_urbano_croquis_listado= path_out_final+"\\reportes-estudiantes-urbano"




##############################obeteniedo reportes distritales

fase='CPV2017'

def exportar_listado_estudiantes(ubigeo, zona,subzona,fase='CPV2017'):
    path_pdf = "{}\\{}_{}{}_estudiantes.pdf".format(path_urbano_croquis_listado,ubigeo,zona,subzona)
    print path_pdf
    informacion=conx.obtener_informacion_reporte_estudiantes(ubigeo, zona, subzona)
    listado.ListadoDeEstudiantes(informacion,path_pdf)


def procesar_listado_estudiantes():

    c = conx.Conexion()
    cursor = c.cursor()
    sql = """
            begin

	        update subzona
	        set flag_selec_estudiantes=0

	        update a
	        set a.flag_selec_estudiantes=1
	        from subzona a inner join

	        (
            SELECT   distinct  c.ubigeo,c.zona,c.subzona
            FROM    CPV_SEGMENTACION_GDB.sde.TB_CPV0301_VIVIENDA_U A
            inner join CPV_SEGMENTACION.DBO.SEGM_U_VIV  B ON A.UBIGEO=B.UBIGEO COLLATE DATABASE_DEFAULT AND A.ZONA =B.ZONA COLLATE DATABASE_DEFAULT AND A.MANZANA=B.MANZANA COLLATE DATABASE_DEFAULT AND A.ID_REG_OR=B.ID_REG_OR   AND B.FASE='CPV2017'
	        inner join CPV_SEGMENTACION.DBO.SEGM_U_AEU C ON B.UBIGEO=C.UBIGEO COLLATE DATABASE_DEFAULT AND B.ZONA=C.ZONA COLLATE DATABASE_DEFAULT AND B.AEU=C.AEU  COLLATE DATABASE_DEFAULT AND C.FASE='CPV2017'
            WHERE A.P29 IN (1,3) AND ((isnull(A.P33_1A_N,0)<>0 ) or ( isnull(A.P33_1B_N,0)<> 0 ) or ( isnull(A.P33_1C_N,0)<> 0 ) )


	        ) b on a.ubigeo=b.ubigeo and a.zona=b.zona and a.subzona=b.subzona

	        update a
	        set flag_selec_estudiantes=1
	        from  (select * from subzona ) a
	        inner join
	        (select distinct ubigeo,zona from subzona where subzona<>0 and flag_selec_estudiantes=1	) b on a.ubigeo=b.ubigeo and a.zona=b.zona
	        where a.subzona=0


	        select ubigeo,zona,subzona from CPV_SEGMENTACION.DBO.SUBZONA
            where flag_selec_estudiantes=1 AND fase='{}'
            order by 1,2

	        end

            """.format(fase)
    print sql
    cursor.execute(sql)

    for row in cursor:

       ubigeo=row[0]
       zona=row[1]
       subzona = row[2]
       print ubigeo,zona,subzona

       exportar_listado_estudiantes(ubigeo, zona, subzona)
    c.close()

procesar_listado_estudiantes()

