import dagster as dg
from dags.defs import feature_engineering as feature_engg_defs
from dags.defs import train_model as train_defs
from dagster_aws.pipes import PipesEMRServerlessClient


@dg.definitions
def defs():
    return dg.Definitions(
        assets=dg.load_assets_from_modules([feature_engg_defs, train_defs]),
        resources={
            'pipes_emr_serverless_client': PipesEMRServerlessClient()
        },
    )
