"""
Error logging for NMOS Registry

Centralized error logging system to track and display errors from the registry.
"""

from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass


@dataclass
class ErrorEntry:
    """Represents a single error entry"""
    timestamp: datetime
    message: str
    exception_type: Optional[str] = None
    traceback: Optional[str] = None

    def __str__(self) -> str:
        """String representation of the error"""
        time_str = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        if self.exception_type:
            return f"[{time_str}] {self.exception_type}: {self.message}"
        return f"[{time_str}] {self.message}"

    def get_detailed_str(self) -> str:
        """Get detailed string with traceback if available"""
        result = str(self)
        if self.traceback:
            result += f"\n{self.traceback}"
        return result


class ErrorLog:
    """Centralized error log for tracking registry errors"""
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern to ensure only one error log exists"""
        if cls._instance is None:
            cls._instance = super(ErrorLog, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the error log"""
        if self._initialized:
            return
        self._errors: List[ErrorEntry] = []
        self._max_entries = 1000  # Maximum number of errors to keep
        self._initialized = True
    
    def add_error(self, message: str, exception: Optional[Exception] = None, traceback_str: Optional[str] = None):
        """
        Add an error to the log
        
        Args:
            message: Error message
            exception: Optional exception object
            traceback_str: Optional traceback string
        """
        exception_type = type(exception).__name__ if exception else None
        
        entry = ErrorEntry(
            timestamp=datetime.now(),
            message=message,
            exception_type=exception_type,
            traceback=traceback_str
        )
        
        self._errors.append(entry)
        
        # Keep only the most recent errors
        if len(self._errors) > self._max_entries:
            self._errors = self._errors[-self._max_entries:]
    
    def get_last_error(self) -> Optional[ErrorEntry]:
        """Get the most recent error"""
        if self._errors:
            return self._errors[-1]
        return None
    
    def get_all_errors(self) -> List[ErrorEntry]:
        """Get all errors"""
        return self._errors.copy()
    
    def clear(self):
        """Clear all errors"""
        self._errors.clear()
    
    def get_error_count(self) -> int:
        """Get the total number of errors"""
        return len(self._errors)
