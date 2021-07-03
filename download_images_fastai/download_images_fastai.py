
import os

import fastai.vision as fv

urls_filepath = "/home/sai/Downloads/tomato.csv"
destpath = "/home/sai/Downloads/tomato"

os.mkdir(destpath)

fv.download_image(urls_filepath, destpath
