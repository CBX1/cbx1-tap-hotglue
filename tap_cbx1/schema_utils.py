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


def build_schema_from_multiple_contents(contents: list) -> th.PropertiesList:
    """
    Build schema from multiple CBX1 content responses (actual data records).
    
    Args:
        contents: List of content objects from CBX1 API response containing actual data
        
    Returns:
        PropertiesList with converted properties
    """
    properties = []
    field_types = {}  # Track field types across all records
    
    # Define fields that should be treated as datetime regardless of data types
    datetime_fields = {"createdAt", "updatedAt", "dataUpdatedAt"}
    
    for content in contents:
        # Content is a dict with field names and values
        for field_name, field_value in content.items():
            if field_name is None:
                continue
                
            # Infer type from the actual value
            if field_name not in field_types:
                if field_name in datetime_fields:
                    field_types[field_name] = "datetime"
                elif field_value is None:
                    field_types[field_name] = "string"  # Default to string for null values
                elif isinstance(field_value, bool):
                    field_types[field_name] = "boolean"
                elif isinstance(field_value, int):
                    field_types[field_name] = "integer"
                elif isinstance(field_value, float):
                    field_types[field_name] = "number"
                elif isinstance(field_value, list):
                    field_types[field_name] = "array"
                elif isinstance(field_value, dict):
                    field_types[field_name] = "object"
                else:
                    field_types[field_name] = "string"
            else:
                # If we find different types for the same field, default to string
                current_type = field_types[field_name]
                if field_name in datetime_fields:
                    field_types[field_name] = "datetime"
                elif field_value is None:
                    # Keep existing type for null values
                    pass
                elif isinstance(field_value, bool):
                    if current_type != "boolean":
                        field_types[field_name] = "string"
                elif isinstance(field_value, int):
                    if current_type == "boolean":
                        field_types[field_name] = "string"
                    elif current_type == "string":
                        pass
                    elif current_type == "number":
                        pass
                    else:
                        field_types[field_name] = "integer"
                elif isinstance(field_value, float):
                    if current_type in ["boolean", "integer"]:
                        field_types[field_name] = "string"
                    elif current_type == "string":
                        pass
                    else:
                        field_types[field_name] = "number"
                elif isinstance(field_value, list):
                    if current_type not in ["array", "string"]:
                        field_types[field_name] = "string"
                elif isinstance(field_value, dict):
                    if current_type not in ["object", "string"]:
                        field_types[field_name] = "string"
                else:
                    # For strings, keep the most specific type found
                    pass
    
    # Create properties from collected field types
    for field_name, data_type in field_types.items():
        # Override datetime fields regardless of inferred type
        if field_name in datetime_fields:
            singer_type = th.DateTimeType()
        else:
            singer_type = convert_json_type_to_singer(data_type)
        
        properties.append(th.Property(field_name, singer_type))
    
    return th.PropertiesList(*properties)


def fetch_schema_from_api(url_base: str, target: str, headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """
    Fetch schema from CBX1 API endpoint using content response.
    
    Args:
        url_base: Base URL for the API
        target: Target type (accounts, contacts, etc.)
        headers: HTTP headers including authentication
        
    Returns:
        Schema dictionary or None if failed
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Use the same endpoint as the client to get content response
        url = f"{url_base}/HUBSPOT/list"
        
        # Fetch multiple pages to get comprehensive schema
        all_contents = []
        page_number = 0
        max_pages = 2 # Limit to prevent infinite loops
        
        while page_number < max_pages:
            payload = {
                "pageNumber": page_number,
                "pageSize": 10,  # Get more records per page for better schema coverage
                "filters": {
                    "testMetadata": {
                        "type": "EQUALS", 
                        "value": None
                    }
                }
            }
            
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract the content from the response
            if (data.get("status", {}).get("code") == "CM000" and 
                data.get("data") and 
                data["data"].get("content")):
                
                contents = data["data"]["content"]
                all_contents.extend(contents)
                
                # Check if there are more pages
                page_data = data.get("data", {})
                if page_data.get("number") >= page_data.get("totalPages") - 1:
                    break
                    
                page_number += 1
            else:
                break
        
        if not all_contents:
            logger.error(f"No content found for target {target}")
            return None
            
        properties_list = build_schema_from_multiple_contents(all_contents)
        return properties_list.to_dict()
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch schema for target {target}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing schema for target {target}: {e}")
        return None
