import os
import subprocess
import arcpy
import conection_sql as conx
import os
import pyPdf
from PyPDF2 import PdfFileMerger,PdfFileReader
import  sys


def error_export_pdf(xpath,mxd):
    error=1
    while (error == 1):
        error = pdf_to_png(xpath)
        if (error == 1):
            arcpy.mapping.ExportToPDF(mxd, xpath, data_frame="PAGE_LAYOUT", resolution=300)
        else:
            break



def pdf_to_png(xpath):

    dir=os.path.dirname(xpath)
    archivo=os.path.basename(xpath)
    file_png=os.path.join(dir,'{}.png'.format(archivo.split(".")[0]))
    proceso = subprocess.Popen("convert -density 72 {} {}".format(
                                                                  xpath,
                                                                  file_png
                                                                  ),
                               shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    errores = proceso.stderr.read()
    errores_print='{}'.format(errores)

    print errores_print
    if len(errores_print) > 0:
        return 1
    else:
        os.remove(file_png)
        return 0



def mergePDF(pdfs,out):

    merger = PdfFileMerger()
    for pdf in pdfs:
        fin = file(pdf, 'rb')
        merger.append(PdfFileReader(fin))
        fin.close()
    merger.write('{}'.format(out))
    return out