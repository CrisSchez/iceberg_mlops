import os
import pandas as pd
from cmlbootstrap import CMLBootstrap
# Set the setup variables needed by CMLBootstrap
HOST = os.getenv("CDSW_API_URL").split(
    ":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split(
    "/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT")  

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

import cmlapi
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT")  

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)
project_id = cml.get_project()['public_identifier']

print(project_id)
user_details = cml.get_user({})
user_obj = {"id": user_details["id"], 
            "username": user_details["username"],
            "name": user_details["name"],
            "type": user_details["type"],
            "html_url": user_details["html_url"],
            "url": user_details["url"]
            }

#client = cmlapi.default_client()
api_url= cml.host
# No arguments are required when the default_client method is used inside a session.
variables=cml.get_environment_variables()
api_key='ed65813c617f98ba699d29e93e9fbcab807ccc9eb4381b689c2b6649e1ef1755.5cea061bf084ae218ffe2aaf91b5e78c9c41f96cdfdf8e6b826b453dcd6729cc'
api_client=cmlapi.default_client(url=api_url,cml_api_key=api_key)
api_client.list_projects()

#client = cmlapi.default_client()
api_instance=api_client

runtime_identifier="docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-workbench-python3.7-standard:2023.08.2-b8"
spark_addon='spark323-19-hf2'

create_jobs_params = {"name": "Check Model",
          "type": "manual",
          #"arguments": str([identificador,choicemetric, threshold,target_columns,drop_input_columns]),
          #"arguments": identificador,
          "project_id": project_id,
          "runtime_identifier": runtime_identifier,
          "script": "6_check_model.py",
          "timezone": "Europe/Madrid",
          "runtime_addon_identifiers": [spark_addon],
          "kernel": "python3",
          "cpu" : 2,
          "memory" : 4,
          "recipients": [
                          {"email": user_details["email"],
                           "success":True,"notify_on_success": False, "notify_on_failure": False, "notify_on_timeout": False, "notify_on_stop": False
                           }
          ]
          }
api_instance.create_job(create_jobs_params, project_id)

create_jobs_params = {"name": "avisoPerformance",
          "type": "manual",
          #"arguments": str([identificador,choicemetric, threshold,target_columns,drop_input_columns]),
          #"arguments": identificador,
          "project_id": project_id,
          "runtime_identifier": runtime_identifier,
          "script": "7_crearReportes.py",
          "timezone": "Europe/Madrid",
          "runtime_addon_identifiers": [spark_addon],
          "kernel": "python3",
          "cpu" : 2,
          "memory" : 4,
          "recipients": [
                          {"email": user_details["email"],
                           "success":True,"notify_on_success": False, "notify_on_failure": False, "notify_on_timeout": False, "notify_on_stop": False
                           }
          ]
          }
api_instance.create_job(create_jobs_params, project_id)
          
          
create_jobs_params = {"name": "retrain",
          "type": "manual",
          #"arguments": str([identificador,choicemetric, threshold,target_columns,drop_input_columns]),
          #"arguments": identificador,
          "project_id": project_id,
          "runtime_identifier": runtime_identifier,
          "script": "3_trainStrategy_job.py",
          "timezone": "Europe/Madrid",
          "runtime_addon_identifiers": [spark_addon],
          "kernel": "python3",
          "cpu" : 2,
          "memory" : 4,
          "recipients": [
                          {"email": user_details["email"],
                           "success":True,"notify_on_success": False, "notify_on_failure": False, "notify_on_timeout": False, "notify_on_stop": False
                           }
          ]
          }
api_instance.create_job(create_jobs_params, project_id)
params = {"projectId":project_id,"latestModelDeployment":True,"latestModelBuild":True}
jobsInfo=pd.DataFrame(cml.get_jobs(params))
job_id = jobsInfo.loc[jobsInfo['name'] == 'retrain']['public_identifier'].min()
          
create_jobs_params = {"name": "deploy_best_model",
          "type": "dependent",
          "parent_job_id":job_id,
          #"arguments": str([identificador,choicemetric, threshold,target_columns,drop_input_columns]),
          #"arguments": identificador,
          "project_id": project_id,
          "runtime_identifier": runtime_identifier,
          "script": "4_get_champion.py",
          "timezone": "Europe/Madrid",
          "runtime_addon_identifiers": [spark_addon],
          "kernel": "python3",
          "cpu" : 2,
          "memory" : 4,
          "recipients": [
                          {"email": user_details["email"],
                           "success":True,"notify_on_success": False, "notify_on_failure": False, "notify_on_timeout": False, "notify_on_stop": False
                           }
          ]
          }
api_instance.create_job(create_jobs_params, project_id)
