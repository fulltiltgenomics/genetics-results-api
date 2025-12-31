"""
Validation helpers for API response testing.
"""

import requests
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlencode


def validate_tsv_response(response_text: str, min_data_lines: int = 0) -> Dict[str, Any]:
    """
    Validate TSV response structure.

    Args:
        response_text: The TSV response text
        min_data_lines: Minimum number of data lines expected (default 0)

    Returns:
        Dict with validation results:
        - valid: bool
        - has_header: bool
        - has_data: bool
        - num_data_lines: int
        - consistent_columns: bool
        - header: list (if valid)
        - errors: list of error messages
    """
    result = {
        "valid": False,
        "has_header": False,
        "has_data": False,
        "num_data_lines": 0,
        "consistent_columns": False,
        "header": None,
        "errors": [],
    }

    if not response_text or not response_text.strip():
        result["errors"].append("Response is empty")
        return result

    lines = response_text.strip().split("\n")

    if len(lines) == 0:
        result["errors"].append("No lines in response")
        return result

    # Check header
    result["has_header"] = True
    result["header"] = lines[0].split("\t")
    header_cols = len(result["header"])

    # Check data lines
    data_lines = lines[1:]
    result["num_data_lines"] = len(data_lines)
    result["has_data"] = len(data_lines) > 0

    if result["num_data_lines"] < min_data_lines:
        result["errors"].append(
            f"Expected at least {min_data_lines} data lines, got {result['num_data_lines']}"
        )

    # Check column consistency
    if result["has_data"]:
        column_counts = [len(line.split("\t")) for line in data_lines]
        result["consistent_columns"] = all(count == header_cols for count in column_counts)

        if not result["consistent_columns"]:
            result["errors"].append(
                f"Inconsistent column counts: header has {header_cols}, "
                f"data lines have {set(column_counts)}"
            )
    else:
        result["consistent_columns"] = True  # No data to be inconsistent

    # Overall validity
    result["valid"] = (
        result["has_header"]
        and result["consistent_columns"]
        and len(result["errors"]) == 0
    )

    return result


def validate_json_response(response_json: Any, min_items: int = 0) -> Dict[str, Any]:
    """
    Validate JSON response structure.

    Args:
        response_json: The parsed JSON response (should be a list of dicts)
        min_items: Minimum number of items expected (default 0)

    Returns:
        Dict with validation results:
        - valid: bool
        - is_list: bool
        - has_data: bool
        - num_items: int
        - consistent_keys: bool
        - keys: set (if valid)
        - errors: list of error messages
    """
    result = {
        "valid": False,
        "is_list": False,
        "has_data": False,
        "num_items": 0,
        "consistent_keys": False,
        "keys": None,
        "errors": [],
    }

    # Check if it's a list
    if not isinstance(response_json, list):
        result["errors"].append(f"Expected list, got {type(response_json).__name__}")
        return result

    result["is_list"] = True
    result["num_items"] = len(response_json)
    result["has_data"] = result["num_items"] > 0

    if result["num_items"] < min_items:
        result["errors"].append(
            f"Expected at least {min_items} items, got {result['num_items']}"
        )

    # Check key consistency
    if result["has_data"]:
        # Check all items are dicts
        if not all(isinstance(item, dict) for item in response_json):
            result["errors"].append("Not all items are dictionaries")
            return result

        # Check all items have the same keys
        first_keys = set(response_json[0].keys())
        result["keys"] = first_keys

        all_same_keys = all(set(item.keys()) == first_keys for item in response_json)
        result["consistent_keys"] = all_same_keys

        if not all_same_keys:
            result["errors"].append("Items have inconsistent keys")
    else:
        result["consistent_keys"] = True  # No data to be inconsistent

    # Overall validity
    result["valid"] = (
        result["is_list"]
        and result["consistent_keys"]
        and len(result["errors"]) == 0
    )

    return result


def validate_response_schema(data: List[Dict], schema: Dict[str, type]) -> Dict[str, Any]:
    """
    Validate that data matches the expected schema.

    Args:
        data: List of dictionaries to validate
        schema: Dictionary mapping field names to expected types

    Returns:
        Dict with validation results:
        - valid: bool
        - missing_fields: list of missing field names
        - extra_fields: list of extra field names
        - type_errors: list of (field, expected_type, actual_type) tuples
        - errors: list of error messages
    """
    result = {
        "valid": False,
        "missing_fields": [],
        "extra_fields": [],
        "type_errors": [],
        "errors": [],
    }

    if not data:
        result["valid"] = True  # Empty data is valid
        return result

    first_item = data[0]

    # Check for missing fields
    expected_fields = set(schema.keys())
    actual_fields = set(first_item.keys())

    result["missing_fields"] = list(expected_fields - actual_fields)
    result["extra_fields"] = list(actual_fields - expected_fields)

    if result["missing_fields"]:
        result["errors"].append(f"Missing fields: {result['missing_fields']}")

    # Note: extra fields might be okay, don't error on them by default

    # Check types for fields that exist
    for field, expected_type in schema.items():
        if field not in first_item:
            continue

        value = first_item[field]

        # Handle None values (nullable fields)
        if value is None:
            continue

        # Check type
        if not isinstance(value, expected_type):
            result["type_errors"].append(
                (field, expected_type.__name__, type(value).__name__)
            )
            result["errors"].append(
                f"Field '{field}' has wrong type: expected {expected_type.__name__}, "
                f"got {type(value).__name__}"
            )

    result["valid"] = len(result["missing_fields"]) == 0 and len(result["type_errors"]) == 0

    return result


def validate_content_type(content_type: str, expected: str) -> bool:
    """
    Validate content type header.

    Args:
        content_type: The content-type header value
        expected: Expected content type (will check if it's contained in the header)

    Returns:
        True if content type matches, False otherwise
    """
    return expected in content_type


def validate_variant_fields(item: Dict) -> Dict[str, Any]:
    """
    Validate that variant fields (chr, pos, ref, alt) are present and valid.

    Args:
        item: Dictionary containing variant data

    Returns:
        Dict with validation results:
        - valid: bool
        - errors: list of error messages
    """
    result = {
        "valid": False,
        "errors": [],
    }

    required_fields = ["chr", "pos", "ref", "alt"]

    for field in required_fields:
        if field not in item:
            result["errors"].append(f"Missing required field: {field}")

    if result["errors"]:
        return result

    # Validate chr (should be int or str representing valid chromosome)
    chr_val = item["chr"]
    if isinstance(chr_val, str):
        valid_chrs = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]
        if chr_val not in valid_chrs:
            result["errors"].append(f"Invalid chromosome: {chr_val}")
    elif isinstance(chr_val, int):
        if not (1 <= chr_val <= 22):
            result["errors"].append(f"Invalid chromosome: {chr_val}")

    # Validate pos (should be positive integer)
    pos_val = item["pos"]
    if not isinstance(pos_val, int) or pos_val <= 0:
        result["errors"].append(f"Invalid position: {pos_val}")

    # Validate ref and alt (should be non-empty strings)
    ref_val = item["ref"]
    if not isinstance(ref_val, str) or not ref_val:
        result["errors"].append(f"Invalid ref allele: {ref_val}")

    alt_val = item["alt"]
    if not isinstance(alt_val, str) or not alt_val:
        result["errors"].append(f"Invalid alt allele: {alt_val}")

    result["valid"] = len(result["errors"]) == 0

    return result


class RequestHelper:
    """
    Helper class for making API requests with better error reporting.

    Automatically captures and displays request URLs in test failures.
    """

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.last_url = None
        self.last_params = None
        self.last_response = None

    def get(self, endpoint: str, params: Optional[Dict] = None, timeout: int = 30) -> requests.Response:
        """
        Make a GET request and capture details for error reporting.

        Args:
            endpoint: API endpoint path (e.g., "/api/v1/credible_sets_by_region/1:1000000-1000100")
            params: Query parameters
            timeout: Request timeout in seconds

        Returns:
            requests.Response object
        """
        url = urljoin(self.base_url, endpoint)
        self.last_url = url
        self.last_params = params or {}

        # Build full URL with params for display
        if params:
            query_string = urlencode(params, doseq=True)
            full_url = f"{url}?{query_string}"
        else:
            full_url = url

        try:
            response = requests.get(url, params=params, timeout=timeout)
            self.last_response = response

            # Add URL info to response object for easy access in tests
            response.test_url = full_url
            response.test_endpoint = endpoint
            response.test_params = params

            return response
        except Exception as e:
            # Enhance exception with URL information
            raise Exception(f"Request failed for URL: {full_url}\nOriginal error: {str(e)}") from e

    def get_last_url(self) -> str:
        """Get the last request URL (useful for error messages)."""
        if self.last_url and self.last_params:
            query_string = urlencode(self.last_params, doseq=True)
            return f"{self.last_url}?{query_string}"
        return self.last_url or "No request made yet"

    def assert_status(self, response: requests.Response, expected_status: int, message: str = ""):
        """
        Assert response status with detailed error message including URL.

        Args:
            response: The response object
            expected_status: Expected status code
            message: Optional custom message
        """
        url = getattr(response, 'test_url', self.get_last_url())

        if response.status_code != expected_status:
            error_msg = f"\n{'='*80}\n"
            error_msg += f"URL: {url}\n"
            error_msg += f"Expected status: {expected_status}\n"
            error_msg += f"Actual status: {response.status_code}\n"

            if message:
                error_msg += f"Message: {message}\n"

            # Include response body if it's small enough
            if len(response.text) < 500:
                error_msg += f"Response body:\n{response.text}\n"
            else:
                error_msg += f"Response body (first 500 chars):\n{response.text[:500]}...\n"

            error_msg += f"{'='*80}"
            raise AssertionError(error_msg)


def make_request(base_url: str, endpoint: str, params: Optional[Dict] = None,
                 timeout: int = 30) -> requests.Response:
    """
    Convenience function to make a request with enhanced error reporting.

    Args:
        base_url: Base URL of the API
        endpoint: API endpoint path
        params: Query parameters
        timeout: Request timeout in seconds

    Returns:
        requests.Response object with test_url attribute

    Example:
        response = make_request(server_url, "/api/v1/credible_sets_by_region/1:1000000-1000100",
                               {"format": "json"})
    """
    helper = RequestHelper(base_url)
    return helper.get(endpoint, params, timeout)
