"""
This module holds all of the context variables
"""

from contextvars import ContextVar
from typing import Optional

current_host = ContextVar[Optional['Host']]('host', default=None)