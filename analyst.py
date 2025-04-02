from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from glob import glob
import polars as pl


FILE_TYPE = "csv"
FILE_LOCATION = "data/*.csv"

mcp = FastMCP("analyst", dependencies=["polars"])


@mcp.tool()
def get_files_list() -> str:
    """
    Get the list of files that are source of data
    """
    files_list = glob(FILE_LOCATION)
    filtered_files_list = [f for f in files_list if f.endswith(f".{FILE_TYPE}")]
    return filtered_files_list


def read_file(file_location: str) -> pl.DataFrame:
    """
    Read the data from the given file location
    """
    if FILE_TYPE == "csv":
        return pl.read_csv(file_location)
    elif FILE_TYPE == "parquet":
        return pl.read_parquet(file_location)
    else:
        raise ValueError(f"Unsupported file type: {FILE_TYPE}")


def read_file_list(file_locations: List[str]) -> pl.DataFrame:
    """
    Read the data from the given file locations
    """
    if FILE_TYPE == "csv":
        dfs = []
        for file_location in file_locations:
            dfs.append(pl.read_csv(file_location))
        return pl.concat(dfs)
    elif FILE_TYPE == "parquet":
        dfs = pl.read_parquet(file_locations)
    else:
        raise ValueError(f"Unsupported file type: {FILE_TYPE}")


@mcp.tool()
def get_schema(file_location: str) -> List[Dict[str, Any]]:
    """
    Get the schema of the data file from the given file location
    """
    df = read_file(file_location)

    schema = df.schema
    schema_dict = {}
    for key, value in schema.items():
        schema_dict[key] = value
    return schema_dict


@mcp.tool()
def execute_polars_sql(file_locations: List[str], query: str) -> List[Dict[str, Any]]:
    """
    Reads the data from the given file locations. Note that file_locations
    can be a list of multiple files. However, all files must have the same schema
    and the same columns. Executes the given polars sql query and returns the result.
    The polars sql query must use the table name as `self` to refer to the source data.
    """
    print(file_locations)
    df = read_file_list(file_locations)
    op_df = df.sql(query)
    output_records = op_df.to_dicts()
    return output_records
