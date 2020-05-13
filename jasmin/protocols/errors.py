class ArgsValidationError(Exception):
    """"Raised when url validation fails  (jasmin.protocols.http.validation.UrlArgsValidator)"""


class CredentialValidationError(Exception):
    """Raised when user credential validation fails

    (jasmin.protocols.http.validation.HttpAPICredentialValidator)
    """

class AuthenticationError(Exception):
    """Raised on authentication error"""