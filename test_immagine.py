#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi


import os, sys, re  ,shutil #,glob
import inspect, os.path


# per gestire le immagini
from PIL import Image


filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))





path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/test_immagine.log'.format(path)
errorfile='{}/log/error_test_immagine.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

import fnmatch # per filtrare i tipi file


import logging




# Create a custom logger
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
    ]
)

logger = logging.getLogger()

# Create handlers
c_handler = logging.FileHandler(filename=errorfile, encoding='utf-8', mode='w')
f_handler = logging.StreamHandler()
#f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.ERROR)
f_handler.setLevel(logging.DEBUG)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)






def main():
    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db')
    
    file_originale= "{}/img_sit/markers/2-{}.png".format(path, 'ecopunto_g')
    filename="{0}/img_sit/markers/{1}-{2}.png".format(path, 16, 'ecopunto_g')
    
    #logger.debug(filename)
    #logger.info(os.path.exists(filename))
    # a questo punto verifico
    #if (os.path.exists(filename)):
    #    logger.debug('''Il file c'è non devo fare nulla''')
    #else: # il file non esiste
    #''' 
    
    logger.debug('''Devo creare file {0} per rifiuto {1} di colore {2}'''.format(filename, 16, '#f4b311'))
        
        
    shutil.copy(file_originale, filename)
    picture = Image.open(filename)

    
    
    logger.debug('Sono arrivato qua')
    # Get the size of the image
    width, height = picture.size


    img = picture.convert("RGBA")
    
    datas = img.getdata()

    new_image_data = []
    for item in datas:
        # change all white (also shades of whites) pixels to yellow
        if item[0] in list(range(190, 256)):
            new_image_data.append((255, 204, 100))
        else:
            new_image_data.append(item)
            
    # update image data
    img.putdata(new_image_data)

    # save new image
    img.save("test_image_altered_background.png")

    # show image in preview
    img.show()
        
        
       
    '''
    logger.debug(width)
    logger.debug(height)
    # Process every pixel
    x=0
    y=0
    while x < width:
        while y < height:
            #logger.debug(x)
            #logger.debug(y)
            current_color = picture.getpixel( (x,y) )
            ####################################################################
            # Do your logic here and create a new (R,G,B) tuple called new_color
            ####################################################################
            if (current_color!=(128, 128, 128, 0) and current_color!=(0, 0, 0, 0) and current_color!=(255, 255, 255, 0)):
                logger.debug(current_color)
                logger.debug('Devo cambiare colore')
                color_rgb=tuple(int('f4b311'[i:i+2], 16) for i in (0, 2, 4))
                logger.debug(color_rgb)
                picture.putpixel( (x,y), color_rgb)
            y+=1
        x+=1
    '''

    
     
if __name__ == "__main__":
    main()   