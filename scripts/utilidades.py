import os
import subprocess
import arcpy

import conection_sql as conx
import os
import pyPdf
from PyPDF2 import PdfFileMerger,PdfFileReader
import  sys

def error_export_pdf(path_urbano_croquis, ubigeo, zona,codigo,out_croquis,mxd):
    error=1
    while (error == 1):
        error = pdf_to_png(dir=os.path.join(path_urbano_croquis, ubigeo, zona), archivo="{}.pdf".format(codigo))
        if (error == 1):
            print 'hay error'
            print codigo
            arcpy.mapping.ExportToPDF(mxd, out_croquis, data_frame="PAGE_LAYOUT", resolution=300)
        else:
            break

def pdf_to_png(dir, archivo):

    proceso = subprocess.Popen("convert -density 72 {} {}".format(os.path.join(dir, archivo),
                                                                  os.path.join(dir,
                                                                               '{}.png'.format(archivo.split(".")[0]))),
                               shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    errores = proceso.stderr.read()
    errores_print='{}'.format(errores)
    #errores_print = errores.decode(sys.getdefaultencoding())

    print errores_print
    if len(errores_print) > 0:
        return 1
    else:
        return 0


def mergePDF(pdfs,out):


    merger = PdfFileMerger()
    for pdf in pdfs:
        fin = file(pdf, 'rb')
        merger.append(PdfFileReader(fin))
        fin.close()


    merger.write('{}'.format(out))


    return out