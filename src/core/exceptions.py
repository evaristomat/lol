# src/core/exceptions.py
class BetsAPIError(Exception):
    """Exceção base para erros da BetsAPI"""

    pass


class RateLimitError(BetsAPIError):
    """Exceção para erros de rate limit"""

    pass


class InvalidAPIKeyError(BetsAPIError):
    """Exceção para chave API inválida"""

    pass


class EventNotFoundError(BetsAPIError):
    """Exceção para evento não encontrado"""

    pass
