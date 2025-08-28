"""BTEQ to dbt conversion toolkit.

This package implements the agentic conversion system for migrating
Teradata BTEQ scripts to dbt models using the DCF framework.

Subpackages:
- parser: lexical + SQL parsing services
- analysis: comprehensive BTEQ analysis outputs
- tools: integration utilities and demos
- design: design documents and specifications

Main services (to be implemented):
- parser: Parse BTEQ into control statements and SQL blocks
- classifier: Classify BTEQ patterns using Snowflake Cortex
- mapper: Map patterns to DCF materializations
- generator: Generate dbt models with hooks
- validator: Validate generated models
"""
