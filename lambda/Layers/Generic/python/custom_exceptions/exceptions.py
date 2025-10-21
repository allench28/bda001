class ApiException(Exception):
    """Base exception for API errors"""
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(self.message)

class AuthenticationException(ApiException):
    """Exception for authentication failures"""
    def __init__(self, message="Authentication required"):
        super().__init__(401, message)

class AuthorizationException(ApiException):
    """Exception for authorization failures"""
    def __init__(self, message="You don't have permission to perform this action"):
        super().__init__(403, message)

class ResourceNotFoundException(ApiException):
    """Exception for resource not found errors"""
    def __init__(self, resource_type, resource_id):
        message = f"{resource_type} with ID {resource_id} not found"
        super().__init__(404, message)

class BadRequestException(ApiException):
    """Exception for invalid request errors"""
    def __init__(self, message="Invalid request parameters"):
        super().__init__(400, message)