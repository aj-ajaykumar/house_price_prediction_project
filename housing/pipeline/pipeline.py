from housing.component.data_ingestion import DataIngestion
from housing.component.data_validation import DataValidation
from housing.component.data_transformation import DataTransformation
from housing.component.model_trainer import ModelTrainer
from housing.component.model_evaluation import ModelEvaluation
from housing.component.model_pusher import ModelPusher
from housing.config.configuration import Configuartion
from housing.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact,DataTransformationArtifact,ModelTrainerArtifact,ModelEvaluationArtifact,ModelPusherArtifact
from housing.exception import HousingException
from housing.logger import logging,get_log_file_name
import os,sys
from housing.constant import *
import pandas as pd
from collections import namedtuple


from threading import Thread
import uuid
from multiprocessing import process
from datetime import datetime



from housing.constant import EXPERIMENT_DIR_NAME, EXPERIMENT_FILE_NAME

Experiment = namedtuple("Experiment", ["experiment_id", "initialization_timestamp", "artifact_time_stamp",
                                       "running_status", "start_time", "stop_time", "execution_time", "message",
                                       "experiment_file_path", "accuracy", "is_model_accepted"])


class Piplile(Thread):
    experiment: Experiment = Experiment(*([None] * 11))
    experiment_file_path = str

    def __init__(self,config:Configuartion)->None:
        try:
            #self.config = config
            os.makedirs(config.training_pipeline_config.artifact_dir,exist_ok=True)
            Piplile.experiment_file_path = os.path.join(config.training_pipeline_config.artifact_dir,EXPERIMENT_DIR_NAME,EXPERIMENT_FILE_NAME)
            super().__init__(daemon=False,name="pipeline")

            self.config = config
        except Exception as e:
            raise HousingException(e,sys) from e
        
    def start_data_ingestion(self)->DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        except Exception as e:
            raise HousingException(e,sys) from e
        
    def start_data_validation(self,data_ingestion_atifact:DataIngestionArtifact)-> DataValidationArtifact:
        try:
            data_validation = DataValidation(data_validation_config=self.config.get_data_validation_config()
                                             ,data_ingestion_artifact=data_ingestion_atifact)
            return data_validation.initiate_data_validation()

        except Exception as e:
            raise HousingException(e,sys) from e
        
    def start_data_transformation(self,data_ingestion_artifact:DataIngestionArtifact,
                                  data_validation_artifact:DataValidationArtifact)->DataTransformationArtifact:
        try:
            data_transformation = DataTransformation(data_ingestion_artifact=data_ingestion_artifact,
                                                 data_validation_artifact=data_validation_artifact,
                                                 data_transformation_config=self.config.get_data_transformation_config()
                                                 )
            return data_transformation.initiate_data_transformation()
        except Exception as e:
            raise HousingException(e,sys) from e
    def start_model_trainer(self,data_transformation_artifact:DataTransformationArtifact)->ModelTrainerArtifact:
        try:
            model_trainer = ModelTrainer(model_trainer_config=self.config.get_model_trainer_config(),
                                         data_transformation_artifact=data_transformation_artifact)
            return model_trainer.initiate_model_trainer()
        except Exception as e:
            raise HousingException(e,sys) from e
        
    def start_model_evaluation(self,data_ingestion_artifact:DataIngestionArtifact,
                               data_validation_artifact:DataValidationArtifact,
                               model_trainer_artifact: ModelTrainerArtifact)->ModelEvaluationArtifact:
        try:
            model_evaluation = ModelEvaluation(model_evaluation_config=self.config.get_model_evaluation_config(),
                                               data_ingestion_artifact=data_ingestion_artifact,
                                               data_validation_artifact=data_validation_artifact,
                                               model_trainer_artifact=model_trainer_artifact)
            return model_evaluation.initiate_model_evaluation()
        except Exception  as e:
            raise HousingException(e,sys) from e
        
    def start_model_pusher(self,model_evaluation_artifact:ModelEvaluationArtifact)->ModelPusherArtifact:
        try:
            model_pusher = ModelPusher(model_pusher_config=self.config.get_model_pusher_config(),
                                       model_evaluation_artifact=model_evaluation_artifact)
            return model_pusher.initiate_model_pusher()
        except Exception as e:
            raise HousingException(e,sys) from e
        
        

    def run_pipeline(self):
        try:

            if Piplile.experiment.running_status:
                logging.info(f"pipeline is already running..")
                return Piplile.experiment
            
            #data ingestion
            logging.info(f"pipeline starting..")

            experiment_id = str(uuid.uuid4())

            Piplile.experiment = Experiment(experiment_id=experiment_id,
                                             initialization_timestamp=self.config.time_stamp,
                                             artifact_time_stamp=self.config.time_stamp,
                                             running_status=True,
                                             start_time=datetime.now(),
                                             stop_time=None,
                                             execution_time=None,
                                             experiment_file_path=Piplile.experiment_file_path,
                                             is_model_accepted=None,
                                             message="Pipeline has been started.",
                                             accuracy=None,
                                             )
            logging.info(f"Pipeline experiment: {Piplile.experiment}")

            self.save_experiment()



            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation(data_ingestion_atifact=data_ingestion_artifact)
            data_transformation_artifact = self.start_data_transformation(data_ingestion_artifact=data_ingestion_artifact,
                                                                 data_validation_artifact=data_validation_artifact,
                                                                 )
            model_trainer_artifact = self.start_model_trainer(data_transformation_artifact=data_transformation_artifact)
            model_evaluation_artifact = self.start_model_evaluation(data_ingestion_artifact=data_ingestion_artifact,
                                                           data_validation_artifact=data_validation_artifact,
                                                           model_trainer_artifact=model_trainer_artifact)
            if model_evaluation_artifact.is_model_accepted:
                model_pusher_artifact = self.start_model_pusher(model_evaluation_artifact=model_evaluation_artifact)
                logging.info(f"model pusher artifact: {model_pusher_artifact}")

            else:
                logging.info(f"trained model rejected..!")
            
            logging.info(f"pipeline compleated...")

            stop_time = datetime.now()
            Piplile.experiment = Experiment(experiment_id=Piplile.experiment.experiment_id,
                                             initialization_timestamp=self.config.time_stamp,
                                             artifact_time_stamp=self.config.time_stamp,
                                             running_status=False,
                                             start_time=Piplile.experiment.start_time,
                                             stop_time=stop_time,
                                             execution_time=stop_time - Piplile.experiment.start_time,
                                             message="Pipeline has been completed.",
                                             experiment_file_path=Piplile.experiment_file_path,
                                             is_model_accepted=model_evaluation_artifact.is_model_accepted,
                                             accuracy=model_trainer_artifact.model_accuracy
                                             )
            logging.info(f"Pipeline experiment: {Piplile.experiment}")
            self.save_experiment()
    
        except Exception as e:
            raise HousingException(e,sys) from e

    def run(self):
        try:
            self.run_pipeline()
        except Exception as e:
            raise e
        
    def save_experiment(self):
        try:
            if Piplile.experiment.experiment_id is not None:
                experiment = Piplile.experiment
                experiment_dict = experiment._asdict()
                experiment_dict: dict = {key: [value] for key, value in experiment_dict.items()}

                experiment_dict.update({
                    "created_time_stamp": [datetime.now()],
                    "experiment_file_path": [os.path.basename(Piplile.experiment.experiment_file_path)]})

                experiment_report = pd.DataFrame(experiment_dict)

                os.makedirs(os.path.dirname(Piplile.experiment_file_path), exist_ok=True)
                if os.path.exists(Piplile.experiment_file_path):
                    experiment_report.to_csv(Piplile.experiment_file_path, index=False, header=False, mode="a")
                else:
                    experiment_report.to_csv(Piplile.experiment_file_path, mode="w", index=False, header=True)
            else:
                print("First start experiment")
        except Exception as e:
            raise HousingException(e, sys) from e
        
    @classmethod
    def get_experiments_status(cls, limit: int = 5) -> pd.DataFrame:
        try:
            experiment_file_path = r"housing/artifact/experiment/experiment.csv"
            if os.path.exists(experiment_file_path):
                df = pd.read_csv(experiment_file_path)
                limit = -1 * int(limit)
                return df[limit:].drop(columns=["experiment_file_path", "initialization_timestamp"], axis=1)
            else:
                return pd.DataFrame()
        except Exception as e:
            raise HousingException(e, sys) from e
    