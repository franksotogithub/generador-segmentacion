from wand.image import Image
from wand.display import display
import os





dir_zona='D:/croquis/urbano/030220/00100'
for archivo in os.listdir(dir_zona):
    nom_archivo_pdf= '{}/{}[0]'.format(dir_zona, archivo)


    with Image(filename=nom_archivo_pdf, resolution=300) as img:
        img.save(filename='{}/{}.png'.format(dir_zona,archivo.split(".")[0] ))