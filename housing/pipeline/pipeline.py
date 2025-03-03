from housing.component.data_ingestion import DataIngestion
from housing.config.configuration import Configuartion
from housing.entity.artifact_entity import DataIngestionArtifact
from housing.exception import HousingException
from housing.logger import logging
import os,sys


class Piplile:
    def __init__(self,config:Configuartion = Configuartion())->None:
        try:
            self.config = config
        except Exception as e:
            raise HousingException(e,sys) from e
        
    def start_data_ingestion(self)->DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        except Exception as e:
            raise HousingException(e,sys) from e
        

    def run_pipeline(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as e:
            raise HousingException(e,sys) from e
    