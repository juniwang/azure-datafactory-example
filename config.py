# export AZURE_TENANT_ID={your tenant id}
# export AZURE_CLIENT_ID={your client id}
# export AZURE_CLIENT_SECRET={your client secret}
# export AZURE_SUBSCRIPTION_ID={your subscription id}

import os
from azure.common.credentials import ServicePrincipalCredentials

subscription_id = os.environ.get('AZURE_SUBSCRIPTION_ID', '11111111-1111-1111-1111-111111111111') # your Azure Subscription Id
credentials = ServicePrincipalCredentials(
    client_id=os.environ['AZURE_CLIENT_ID'],
    secret=os.environ['AZURE_CLIENT_SECRET'],
    tenant=os.environ['AZURE_TENANT_ID']
)

dataFactory_location="southeastasia"
dataFactory_group_name="somegroup"
dataFactory_name="somefactory"
dataFactory_storage_name="somestorage"

