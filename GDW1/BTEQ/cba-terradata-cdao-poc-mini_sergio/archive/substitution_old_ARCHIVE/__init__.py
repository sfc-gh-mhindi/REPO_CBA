"""
BTEQ Variable Substitution Module

This module provides services for substituting BTEQ variables with their configured values,
particularly for database and schema name transformations during migration to Snowflake.
"""

from .substitution_service import SubstitutionService
from .pipeline import SubstitutionPipeline

__all__ = ['SubstitutionService', 'SubstitutionPipeline']
