import conection_sql as conx
import os
import pyPdf
from PyPDF2 import PdfFileMerger,PdfFileReader
import  sys

path_inicial="\\\\192.168.201.115\\cpv2017\\croquis-listado\\urbano\\CPV2017"




ubigeo="{}".format(sys.argv[1])
zona="{}".format(sys.argv[2])

def mergePDF(pdfs,out):
    #temp = tempfile.NamedTemporaryFile(delete=False)

    merger = PdfFileMerger()
    for pdf in pdfs:
        fin = file(pdf, 'rb')
        merger.append(PdfFileReader(fin))
        fin.close()

    #merger.write('{}.pdf'.format(temp.name))
    merger.write('{}'.format(out))

    #print '{}.pdf'.format(temp.name), "_"*20
    return out



def crear_merge(ubigeo,zona,fase='CPV2017'):
    conn = conx.Conexion()
    cursor = conn.cursor()

    sql_subzona = """
            select a.ruta_croq
            from
            subzona a
            where ubigeo='{}' and zona='{}' and fase='{}' and cant_pea=1
            """.format(ubigeo,zona,fase)
    cursor.execute(sql_subzona)
    list_subzona=[x[0] for x in cursor]
    sql_seccion = """
        select a.ruta_croq
        from
        segm_u_seccion a
        where ubigeo='{}' and zona='{}' and fase='{}'
        """.format(ubigeo, zona, fase)
    cursor.execute(sql_seccion)
    list_seccion = [x[0] for x in cursor]
    sql_aeu = """
        select a.ruta_croq
        from
        segm_u_aeu a
        where ubigeo='{}' and zona='{}' and fase='{}'
        """.format(ubigeo, zona, fase)
    cursor.execute(sql_aeu)
    list_aeu = [x[0] for x in cursor]
    legajo_aeu=list_aeu
    legajo_aeu.sort()
    legajo_seccion=[]
    legajo_seccion.extend(list_seccion)
    legajo_seccion.extend(list_aeu)
    legajo_seccion.sort()
    legajo_zona = []
    legajo_zona.extend(list_subzona)
    legajo_zona.extend(list_seccion)
    legajo_zona.sort()
    out_aeu="{path_ini}\\{ubigeo}\\{zona}\\{ubigeo}_{zona}_aeu.pdf".format(path_ini=path_inicial,ubigeo=ubigeo,zona=zona)
    out_secc = "{path_ini}\\{ubigeo}\\{zona}\\{ubigeo}_{zona}_seccion.pdf".format(path_ini=path_inicial, ubigeo=ubigeo,
                                                                             zona=zona)
    out_zona = "{path_ini}\\{ubigeo}\\{zona}\\{ubigeo}_{zona}_zona.pdf".format(path_ini=path_inicial, ubigeo=ubigeo,
                                                                                  zona=zona)
    list_legajos=[[legajo_aeu,out_aeu ],[legajo_seccion,out_secc],[legajo_zona,out_zona]]

    print out_aeu

    print out_secc
    print out_zona
    for list in list_legajos:
        mergePDF(list[0],list[1])
    conn.close()




crear_merge(ubigeo,zona)


