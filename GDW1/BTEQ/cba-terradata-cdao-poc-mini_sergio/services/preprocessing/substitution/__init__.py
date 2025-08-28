"""
BTEQ Variable Substitution Service

Data preprocessing service for BTEQ variable substitution and transformation.
Handles %%VARIABLE%%, ${VARIABLE}, &VARIABLE patterns from configuration.
"""

from .service import SubstitutionService, SubstitutionMapping  # noqa: F401
