from config import *

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.storage.models import *
from azure.mgmt.datafactory.models import *
from datetime import datetime
import time

resource_client = ResourceManagementClient(credentials, subscription_id)
storage_client = StorageManagementClient(credentials, subscription_id)
adf_client = DataFactoryManagementClient(credentials, subscription_id)


def get_storage_conn_string(storage_account_group, storage_account_name):
    storage_keys = storage_client.storage_accounts.list_keys(storage_account_group, storage_account_name)
    storage_keys = {v.key_name: v.value for v in storage_keys.keys}
    return "DefaultEndpointsProtocol=https;AccountName={};AccountKey={}".format(storage_account_name,
                                                                                storage_keys['key1'])


def setup_linked_service(storage_account_group, storage_account_name):
    print("Creating linked service for {} in group {}".format(storage_account_name, storage_account_group))
    conn = SecureString(get_storage_conn_string(storage_account_group, storage_account_name))
    ls_azure_storage = AzureStorageLinkedService(connection_string=conn)
    ls_name = linked_service_name(storage_account_name)
    ls = adf_client.linked_services.create_or_update(dataFactory_group_name, dataFactory_name, ls_name,
                                                     ls_azure_storage)
    print_item(ls)
    return ls


def linked_service_name(storage_account_name):
    return "ls{}".format(storage_account_name)


def dataset_name(storage_account_name):
    return "ds_{}".format(storage_account_name)


def setup_dataset(storage_account_name, table_name, is_sink=False):
    prefix = "out" if is_sink else "in"
    ds_name = "{}_ds_{}_{}".format(prefix, storage_account_name, table_name)
    ds_ls = LinkedServiceReference(linked_service_name(storage_account_name))
    ds_table = AzureTableDataset(ds_ls, table_name)
    print("create dataset for {}.{}".format(storage_account_name, table_name))
    adf_client.datasets.create_or_update(dataFactory_group_name, dataFactory_name, ds_name, ds_table)
    return ds_name


def setup_activity(storage_account_name, in_dataset_name, out_dataset_name, azure_table_source_query=None):
    # define Activity
    table_source = AzureTableSource(azure_table_source_query=azure_table_source_query)
    table_sink = AzureTableSink(azure_table_partition_key_name="PartitionKey", azure_table_row_key_name="RowKey")
    dsIn_ref = DatasetReference(in_dataset_name)
    dsOut_ref = DatasetReference(out_dataset_name)
    copy_activity = CopyActivity(storage_account_name,
                                 description=storage_account_name,
                                 inputs=[dsIn_ref],
                                 outputs=[dsOut_ref],
                                 source=table_source,
                                 sink=table_sink,
                                 cloud_data_movement_units=2)
    return copy_activity


def setup_pipeline(pipeline_name, new_activities):
    params_for_pipeline = {}
    p_obj = None
    try:
        print("Query pipeline: " + pipeline_name)
        p_obj = adf_client.pipelines.get(dataFactory_group_name, dataFactory_name, pipeline_name)
        activities = p_obj.activities
        for new_a in new_activities:
            activities = [a for a in activities if not a.name == new_a.name]
            activities.append(new_a)
        p_obj.activities = activities
    except:
        print("Pipeline doesn't exist, will create new pipeline: " + pipeline_name)
        p_obj = PipelineResource(activities=new_activities, parameters=params_for_pipeline)

    print("Create or update pipeline {}, activities:{}".format(pipeline_name, [a.name for a in p_obj.activities]))
    p = adf_client.pipelines.create_or_update(dataFactory_group_name, dataFactory_name, pipeline_name, p_obj)
    print_item(p)


def run_pipeline(pipeline_name):
    # create a run
    run_resp = adf_client.pipelines.create_run(dataFactory_group_name, dataFactory_name, pipeline_name)
    run = adf_client.pipeline_runs.get(dataFactory_group_name, dataFactory_name, run_resp.run_id)
    print("\tPipeline run status: {}".format(run.status))
    while run.status != 'Succeeded':
        time.sleep(5)
        run = adf_client.pipeline_runs.get(dataFactory_group_name, dataFactory_name, run_resp.run_id)
        print("\tPipeline run status: {}".format(run.status))


def setup_trigger(pipeline_name, interval_minutes=5):
    trigger_name = "trigger" + pipeline_name
    try:
        print("query trigger: " + trigger_name)
        triggerResource=adf_client.triggers.get(dataFactory_group_name, dataFactory_name, trigger_name)
        if triggerResource.properties.recurrence.interval == interval_minutes:
            print("trigger {} exists. Skip creating.".format(trigger_name))
            return
        else:
            print("trigger {} exists.Stopping.".format(trigger_name))
            adf_client.triggers.stop(dataFactory_group_name, dataFactory_name, trigger_name)
    except:
        print("Trigger not found, will create new:" + trigger_name)

    print("creating or updating trigger {}".format(trigger_name))
    trigger_schedule = ScheduleTriggerRecurrence(start_time=datetime.utcnow(),
                                                 frequency="Minute",
                                                 interval=interval_minutes)
    pipeline_reference = PipelineReference(pipeline_name)
    trigger = ScheduleTrigger(trigger_schedule, pipelines=[TriggerPipelineReference(pipeline_reference)])
    adf_client.triggers.create_or_update(dataFactory_group_name, dataFactory_name, trigger_name, trigger)
    adf_client.triggers.start(dataFactory_group_name, dataFactory_name, trigger_name)
    print("Trigger {} started".format(trigger_name))


def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, 'location'):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, 'tags'):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, 'properties'):
        print_properties(group.properties)


def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n\n")


def print_activity_run_details(activity_run):
    """Print activity run details."""
    print("\n\tActivity run details\n")
    print("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        print("\tNumber of bytes read: {}".format(activity_run.output['dataRead']))
        print("\tNumber of bytes written: {}".format(activity_run.output['dataWritten']))
        print("\tCopy duration: {}".format(activity_run.output['copyDuration']))
    else:
        print("\tErrors: {}".format(activity_run.error['message']))
