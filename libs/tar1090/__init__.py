"""tar1090 HTTP client library.

Public API::

    states = await tar1090.poll(url, timeout=5.0)

Only ``poll`` and ``AircraftState`` are part of the public API.
Internal HTTP logic lives in ``_http.py``.
"""

from tar1090._http import poll
from tar1090.models import AircraftState

__all__ = ["AircraftState", "poll"]
