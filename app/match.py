from .functions import add_metadata
import pandas as pd
import fastdup 
import shutil
import os


def image(path_imgs: str = None, ) -> pd.DataFrame:
    
    fastdup_dir = os.path.dirname(path_imgs)
    fd = fastdup.create(fastdup_dir)
    
    # revisamos las imagenes con fastdup
    fd.run(path_imgs, threshold= 0.5, overwrite= True, high_accuracy= True)
    # pedimos las imagenes que son invalidas
    invalid_img_s3: list = fd.invalid_instances()["filename"].to_list()
    
    if len(invalid_img_s3):
        for damaged_file in invalid_img_s3:
            add_metadata(damaged_file)
        
        # analizo las imagenes de nuevo
        fd.run(path_imgs, threshold= 0.5, overwrite= True, high_accuracy= True)

    similarity = fd.similarity()
    
    # borramos los archivos que se generaron
    shutil.rmtree(fastdup_dir)
    
    return similarity