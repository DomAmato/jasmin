class RouteNotFoundError(Exception):
    """Raised when no routes found for a given Routable"""


class ConnectorNotFoundError(Exception):
    """Raised when no connectors are available for a given Routable"""


class ChargingError(Exception):
    """Raised on any occuring error while charging user"""


class ThroughputExceededError(Exception):
    """Raised when throughput is exceeded"""


class InterceptorNotSetError(Exception):
    """Raised when message is about to be intercepted and no interceptorpb_client were set"""


class InterceptorNotConnectedError(Exception):
    """Raised when message is about to be intercepted and interceptorpb_client is disconnected"""


class InterceptorRunError(Exception):
    """Raised when running script returned an error"""
