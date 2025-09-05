"""Schema utilities for converting CBX1 JSON Schema to Singer SDK types."""

import requests
from typing import Dict, Any, Optional
from singer_sdk import typing as th
import logging


def convert_json_type_to_singer(json_type: str, enum_values: Optional[list] = None) -> th.JSONTypeHelper:
    """Convert JSON Schema type to Singer SDK type."""
    if json_type == "string":
        if enum_values:
            # For enum strings, we still use StringType but could add validation later
            return th.StringType()
        return th.StringType()
    elif json_type == "integer":
        return th.IntegerType()
    elif json_type == "number":
        return th.NumberType()
    elif json_type == "boolean":
        return th.BooleanType()
    elif json_type == "array":
        return th.ArrayType(th.StringType())  # Default to string array
    else:
        # Default to string for unknown types
        return th.StringType()


def parse_flattened_schema(flattened_schema: Dict[str, Any]) -> th.PropertiesList:
    """
    Convert CBX1's flattenedJsonSchemaForJsonPath to Singer SDK PropertiesList.
    
    Args:
        flattened_schema: The flattened schema from CBX1 API response
        
    Returns:
        PropertiesList with converted properties
    """
    properties = []
    
    # Define fields that should be treated as datetime regardless of API schema
    datetime_fields = {"createdAt", "updatedAt", "dataUpdatedAt"}
    
    for field_path, field_def in flattened_schema.items():
        # Skip array notation fields like "industries[*]" for now
        # We'll handle these as simple arrays on the base field
        if "[*]" in field_path:
            continue
            
        # Handle nested object fields like "hqLocation.city"
        if "." in field_path:
            # For now, we'll flatten these to simple fields
            # A more sophisticated approach would build nested objects
            field_name = field_path.replace(".", "_")
        else:
            field_name = field_path
            
        # Override datetime fields regardless of API schema type
        if field_name in datetime_fields:
            singer_type = th.DateTimeType()
        else:
            json_type = field_def.get("type", "string")
            enum_values = field_def.get("enum")
            singer_type = convert_json_type_to_singer(json_type, enum_values)
        
        properties.append(th.Property(field_name, singer_type))
    
    # Handle array fields by looking for their base names
    array_fields = {}
    for field_path, field_def in flattened_schema.items():
        if "[*]" in field_path:
            base_field = field_path.split("[*]")[0]
            if base_field not in array_fields:
                json_type = field_def.get("type", "string")
                item_type = convert_json_type_to_singer(json_type)
                array_fields[base_field] = th.ArrayType(item_type)
    
    # Add array fields to properties
    for field_name, array_type in array_fields.items():
        # Skip if we already have this field as a non-array
        existing_names = [prop.name for prop in properties]
        if field_name not in existing_names:
            properties.append(th.Property(field_name, array_type))
    
    return th.PropertiesList(*properties)


def fetch_schema_from_api(url_base: str, target: str, headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """
    Fetch schema from CBX1 API endpoint.
    
    Args:
        url_base: Base URL for the API
        target: Target type (accounts, contacts, etc.)
        headers: HTTP headers including authentication
        
    Returns:
        Schema dictionary or None if failed
    """
    logger = logging.getLogger(__name__)
    
    try:
        url = f"{url_base}/targets/{target}/debug/jsonSchema"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract the flattened schema from the response
        if (data.get("status", {}).get("code") == "CM000" and 
            data.get("data") and 
            len(data["data"]) > 1 and
            "flattenedJsonSchemaForJsonPath" in data["data"][1]):
            
            flattened_schema = data["data"][1]["flattenedJsonSchemaForJsonPath"]
            properties_list = parse_flattened_schema(flattened_schema)
            return properties_list.to_dict()
        else:
            logger.error(f"Invalid schema response format for target {target}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch schema for target {target}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing schema for target {target}: {e}")
        return None
