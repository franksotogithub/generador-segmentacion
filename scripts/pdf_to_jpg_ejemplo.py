#from wand.image import Image
#from wand.display import display
#import os




#dir_zona='D:/croquis/urbano/030220/00100'
#for archivo in os.listdir(dir_zona):
#    nom_archivo_pdf= '{}/{}[0]'.format(dir_zona, archivo)
#
#
#    with Image(filename=nom_archivo_pdf, resolution=300) as img:
#        img.save(filename='{}/{}.png'.format(dir_zona,archivo.split(".")[0] ))

import subprocess
import os
import sys
from  datetime import *

dir_zona='D:/croquis/urbano/030220/00100'

print datetime.today()

for archivo in os.listdir(dir_zona):

    try:
        proceso = subprocess.Popen("convert -density 72 {} {}".format(os.path.join(dir_zona,archivo),os.path.join(dir_zona,'{}.png'.format(archivo.split(".")[0]) ) ) , shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        errores=proceso.stderr.read()
        errores_print=errores.decode(sys.getdefaultencoding())
        if len(errores_print)>0:
            print 'Hay un error'
    except:
        print 'algo salio mal'

print datetime.today()
