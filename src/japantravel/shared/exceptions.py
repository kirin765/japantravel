"""Domain exceptions."""


class PipelineError(Exception):
    """Base pipeline exception."""


class ExternalServiceError(PipelineError):
    """Raised when external API call fails."""
