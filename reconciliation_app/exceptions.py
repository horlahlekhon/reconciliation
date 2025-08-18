from typing import Dict, List


class DataValidationError(Exception):
    def __init__(self, message: str, errors: List[Dict[str, str]]) -> None:
        self.message = message
        self.errors = errors
