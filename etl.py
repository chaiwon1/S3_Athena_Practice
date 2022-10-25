import awswrangler as wr
import pandas as pd

def csv_to_parquet (bucket_name, file_name) :
    
    # define variables
    raw_s3_bucket = bucket_name
    raw_path_dir = f"original_data/{file_name}"
    raw_path = f"s3://{raw_s3_bucket}/{raw_path_dir}"

    standardized_s3_bucket = bucket_name
    standard_path_dir = f"converted_data/{file_name}/"
    standardized_path = f"s3://{standardized_s3_bucket}/{standard_path_dir}"
    
    event_data_header = ["identity_adid", "os", "model", "country", "event_name", "log_id", "server_datetime", "quantity", "price"]
    attribution_data_header = ["partner", "campaign", "server_datetime", "tracker_id", "log_id", "attribution_type", "identity_adid"]
    
    
    # etl process
    if file_name == "event_data" :
        # extract csv file from s3
        df = wr.s3.read_csv(raw_path, names=event_data_header)
        
        # transform data
        df = df.astype({"identity_adid" : "str", 
                        "os" : "str",
                        "model" : "str",
                        "country" : "str",
                        "event_name" : "str",
                        "quantity" : "Int64",
                        "price" : "float"
                        })
        df["server_datetime"] = pd.to_datetime(df["server_datetime"], errors = "coerce")
        df["server_datetime"] = df["server_datetime"].dt.strftime("%Y-%m-%d")
        
                
    elif file_name == "attribution_data" :
        # extract csv file from s3
        df = wr.s3.read_csv(raw_path, names=attribution_data_header)
        
        # transform data
        df = df.astype({"partner" : "str", 
                        "campaign" : "str",
                        "tracker_id" : "str",
                        "log_id" : "str",
                        "attribution_type" : "Int64",
                        "identity_adid" : "str"
                        })
        df["server_datetime"] = pd.to_datetime(df["server_datetime"], errors = "coerce")
        df["server_datetime"] = df["server_datetime"].dt.strftime("%Y-%m-%d")

              
    # load parquet file to s3    
    partition = ["server_datetime"]
    wr.s3.to_parquet(df, path=standardized_path, dataset=True, partition_cols=partition)
        
        
    # setting catalog for gule/athena
    if file_name == "event_data" :
        wr.catalog.create_parquet_table(
                                        database = "dfncodetestdb",
                                        table = f"{file_name}_table",
                                        path=standardized_path,
                                        columns_types = {"identity_adid" : "string",
                                                        "os" : "string", 
                                                        "model" : "string",
                                                        "country" : "string",
                                                        "event_name" : "string",
                                                        "quantity" : "int",
                                                        "price" : "double"
                                                        },
                                        partitions_types = {"server_datetime" : "string"},
                                        compression = "snappy",
                                        description = "test",
                                        columns_comments = {"identity_adid" : "id",
                                                            "os" : "os type", 
                                                            "model" : "model type",
                                                            "country" : "name of country",
                                                            "event_name" : "event name",
                                                            "quantity" : "counts of purchase",
                                                            "price" : "amount of purchase"
                                                            }
                                        )
        
    elif file_name == "attribution_data" :
        wr.catalog.create_parquet_table(
                                        database = "dfncodetestdb",
                                        table = f"{file_name}_table",
                                        path=standardized_path,
                                        columns_types = {"partner" : "string", 
                                                        "campaign" : "string",
                                                        "tracker_id" : "string",
                                                        "log_id" : "string",
                                                        "attribution_type" : "int",
                                                        "identity_adid" : "string"
                                                        },
                                        partitions_types = {"server_datetime" : "string"},
                                        compression = "snappy",
                                        description = "test",
                                        columns_comments = {"partner" : "partner name", 
                                                            "campaign" : "campaign name",
                                                            "server_datetime" : "server datetime",
                                                            "tracker_id" : "tracker ids",
                                                            "log_id" : "log ids",
                                                            "attribution_type" : "types of attribution",
                                                            "identity_adid" : "id"
                                                            }
                                        )
    # repair table                                
    wr.athena.repair_table(database = "dfncodetestdb",
                           table = f"{file_name}_table",
                           s3_output = standardized_path)
    
    
    
bucket_name = "dfncodetestbucket"
csv_to_parquet(bucket_name, "event_data")
csv_to_parquet(bucket_name, "attribution_data")