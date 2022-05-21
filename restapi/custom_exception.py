from rest_framework.exceptions import APIException, status

class UnauthorizedUserException(APIException):
    status_code = status.HTTP_404_NOT_FOUND
    default_detail = "Not Found"
    default_code = "Records unavailable"