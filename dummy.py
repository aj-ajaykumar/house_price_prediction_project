from housing.pipeline.pipeline import Piplile
from housing.exception import HousingException
import os,sys


def main():
   try:
       piplile = Piplile().run_pipeline()
       #piplile.run_pipeline()
   except Exception as e:
       raise HousingException(e,sys) from e

if __name__ == "__main__":
    main()

