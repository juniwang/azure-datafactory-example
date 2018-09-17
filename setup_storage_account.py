from common import *
import argparse
from azure.cosmosdb.table import *


def initialize_storage_account(group, name, init_copy=False):
    # create linked service for this storage account
    setup_linked_service(group, name)

    excluded_tables=["billing", "log"]
    table_service = TableService(account_name=name, connection_string=get_storage_conn_string(group, name))
    azs = (t for t in table_service.list_tables() if all(ex not in t.name.lower() for ex in excluded_tables))
    for table in azs:
        # source data set
        source_ds = setup_dataset(name, table.name, False)

        # sink data set
        sink_ds = setup_dataset(dataFactory_storage_name, table.name, True)

        # create pipeline for full copy and trigger once. Run it manually if needed
        full_copy = setup_activity(name, source_ds, sink_ds)
        pipeline_full = "{}_full_copy".format(table.name)
        setup_pipeline(pipeline_full, [full_copy])
        if init_copy:
            run_pipeline(pipeline_full)

        # create pipeline with incremental copy and its triggers
        query = "Timestamp ge datetime'@{addminutes(pipeline().TriggerTime, -6)}'"
        incremental_copy = setup_activity(name, source_ds, sink_ds, query)
        pipeline_incremental = "{}_incremental".format(table.name)
        setup_pipeline(pipeline_incremental, [incremental_copy])
        setup_trigger(pipeline_incremental)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", "-g", required=True, help="The resource group of the source storage account")
    parser.add_argument("--name", "-n", required=True, help="The name of the source storage account")
    parser.add_argument("--full", "-f", required=False, default=False, help="The name of the source storage account")
    args = parser.parse_args()

    initialize_storage_account(args.group, args.name, args.full)
