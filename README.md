# MCP Analyst

MCP Analyst is an MCP server that empowers claude to analyze local CSV or Parquet files.

Use this server when your dataset size is bigger than the context window or you don't want to upload full file to optimize the cost.

# Installation

## Install uv

[uv](https://docs.astral.sh/uv/) is required to run the MCP server.

**Mac**

```
brew install uv
```

**Windows**

```
winget install --id=astral-sh.uv  -e
```

## Add servers in MCP

To use the server in Claude you would need to update the Claude config

On MacOS: `~/Library/Application Support/Claude/claude_desktop_config.json` On Windows: `%APPDATA%/Claude/claude_desktop_config.json`

```
{
  "mcpServers": {
    "analyst": {
      "command": "uvx",
      "args": [
        "mcp-analyst",
        "--file_location",
        "<replace_this_with_path_to_csv_or_parquet_files_on_your_machine>"
      ]
    }
  }
}
```

# How do I provide more than one CSV file?

The server supports the using globs in the file location. So for example if your directory `data` has more than one csv file then you can give file-location as `<parent_path_to_data>/data/*.csv`
