from common import *


# create the resource group
def create_resource_group():
    rg_params = {'location': dataFactory_location}
    resource_client.resource_groups.create_or_update(dataFactory_group_name, rg_params)


# Create a data factory
def create_df():
    df_resource = Factory(location=dataFactory_location)
    df = adf_client.factories.create_or_update(dataFactory_group_name, dataFactory_name, df_resource)
    print_item(df)
    while df.provisioning_state != 'Succeeded':
        df = adf_client.factories.get(dataFactory_group_name, dataFactory_name)
        time.sleep(1)


# create sink storage account
def create_sink_storage_account():
    storage_async_operation = storage_client.storage_accounts.create(
        dataFactory_group_name,
        dataFactory_storage_name,
        StorageAccountCreateParameters(
            sku=Sku(SkuName.standard_ragrs),
            kind=Kind.storage,
            location=dataFactory_location
        )
    )
    storage_account = storage_async_operation.result()
    print_item(storage_account)


def initialize_df():
    # comment out if the resource group already exits
    create_resource_group()
    create_df()
    create_sink_storage_account()
    # setup linked service for sink storage account
    setup_linked_service(dataFactory_group_name, dataFactory_storage_name)


if __name__ == '__main__':
    initialize_df()
