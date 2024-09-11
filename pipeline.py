#from google.cloud import aiplatform
import kfp
#from google.cloud import aiplatform
from google_cloud_pipeline_components.v1.bigquery import (BigqueryQueryJobOp) 

from google_cloud_pipeline_components.v1.automl.training_job import AutoMLTabularTrainingJobRunOp
from google_cloud_pipeline_components.v1.dataset import TabularDatasetCreateOp 
from google_cloud_pipeline_components.v1.endpoint import EndpointCreateOp , ModelDeployOp

 
project_id = "vlba-2024-mpd-group-6"
pipeline_root_path = "gs://vlba-g6-ml-pipeline-bucket" 
 
# [START aiplatform_sdk_create_and_import_dataset_tabular_bigquery_sample]
def create_and_import_dataset_tabular_bigquery_sample(
   display_name: str,
   project: str,
   bigquery_source: str,
):
 
   ds_op = TabularDatasetCreateOp(
       display_name=display_name,
       bq_source=bigquery_source,
       project=project,
   )
   print(ds_op.outputs['dataset'])
      
   return ds_op
 
# Define the workflow of the pipeline.
@kfp.dsl.pipeline(
   name="g6_regr_model_pipeline",
   pipeline_root=pipeline_root_path)
def pipeline(project_id: str):
   # The first step of your workflow is a dataset generator.
   ds_op = create_and_import_dataset_tabular_bigquery_sample("product_sales", project_id, 
            "bq://vlba-2024-mpd-group-6.mpd_g6_data.XXMPD_PastSalesQty_Model")
   # The second step is a model training component. It takes the dataset
   # outputted from the first step, supplies it as an input argument to the
   # component   
   training_job_run_op = AutoMLTabularTrainingJobRunOp(
       project=project_id,
       display_name="train_regression_model",
       optimization_prediction_type="regression",
       dataset=ds_op.outputs["dataset"],
       model_display_name="LinearRegression_model",
       target_column="Forcasted_sales",
       budget_milli_node_hours=1000,
   )
 
   # The third and fourth step are for deploying the model.
   create_endpoint_op = EndpointCreateOp(
       project=project_id,
       display_name = "sales_regression_endpoint",
   )
 
   model_deploy_op = ModelDeployOp(
       model=training_job_run_op.outputs["model"],
       endpoint=create_endpoint_op.outputs['endpoint'],
       dedicated_resources_machine_type="n1-standard-16",
       dedicated_resources_min_replica_count=1,
       dedicated_resources_max_replica_count=1,
 
   )
 
from kfp import compiler
compiler.Compiler().compile(
    pipeline_func=pipeline,
    package_path='g6_regr_model_pipeline.yaml'
)