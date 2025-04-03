import argparse
from typing import List, Dict, Any, Optional
from pydantic import Field
from mcp.server.fastmcp import FastMCP
from glob import glob
import polars as pl


parser = argparse.ArgumentParser()
parser.add_argument("--file_location", type=str, default="data/*.csv")
args = parser.parse_args()


mcp = FastMCP("analyst", dependencies=["polars"])


@mcp.tool()
def get_files_list() -> str:
    """
    Get the list of files that are source of data
    """
    files_list = glob(args.file_location)
    return files_list


def read_file(
    file_location: str,
    file_type: str = Field(
        description="The type of the file to be read. Supported types are csv and parquet",
        default="csv",
    ),
) -> pl.DataFrame:
    """
    Read the data from the given file location
    """
    if file_type == "csv":
        return pl.read_csv(file_location)
    elif file_type == "parquet":
        return pl.read_parquet(file_location)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def read_file_list(
    file_locations: List[str],
    file_type: str = Field(
        description="The type of the file to be read. Supported types are csv and parquet",
        default="csv",
    ),
) -> pl.DataFrame:
    """
    Read the data from the given file locations
    """
    if file_type == "csv":
        dfs = []
        for file_location in file_locations:
            dfs.append(pl.read_csv(file_location))
        return pl.concat(dfs)
    elif file_type == "parquet":
        dfs = pl.read_parquet(file_locations)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


@mcp.tool()
def get_schema(
    file_location: str,
    file_type: str = Field(
        description="The type of the file to be read. Supported types are csv and parquet",
        default="csv",
    ),
) -> List[Dict[str, Any]]:
    """
    Get the schema of a single data file from the given file location
    """
    df = read_file(file_location, file_type)

    schema = df.schema
    schema_dict = {}
    for key, value in schema.items():
        schema_dict[key] = value
    return schema_dict


@mcp.tool()
def execute_polars_sql(
    file_locations: List[str],
    query: str,
    file_type: str = Field(
        description="The type of the file to be read. Supported types are csv and parquet",
        default="csv",
    ),
) -> List[Dict[str, Any]]:
    """
    Reads the data from the given file locations. Note that file_locations
    can be a list of multiple files. However, all files must have the same schema
    and the same columns. Executes the given polars sql query and returns the result.
    Note that the polars sql query must use the table name as `self` to refer to the source data.
    """
    df = read_file_list(file_locations, file_type)
    op_df = df.sql(query)
    output_records = op_df.to_dicts()
    return output_records


if __name__ == "__main__":
    mcp.run()
