"""
Unit tests for validate_identifier().
No database connection required — these run instantly.
"""
import pytest
from db.utils import validate_identifier


class TestValidIdentifiers:
    def test_simple_table_name(self):
        assert validate_identifier("users") == "users"

    def test_name_with_underscore(self):
        assert validate_identifier("my_table") == "my_table"

    def test_name_with_trailing_numbers(self):
        assert validate_identifier("table_1") == "table_1"

    def test_leading_underscore(self):
        assert validate_identifier("_internal") == "_internal"

    def test_mixed_case(self):
        assert validate_identifier("MyTable") == "MyTable"

    def test_all_uppercase(self):
        assert validate_identifier("USERS") == "USERS"

    def test_single_char(self):
        assert validate_identifier("x") == "x"


class TestInjectionAttempts:
    """Every one of these should raise ValueError before touching the DB."""

    def test_semicolon_drop(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("users; DROP TABLE users; --")

    def test_union_select(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("1 UNION SELECT * FROM secrets")

    def test_single_quote(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("users' OR '1'='1")

    def test_comment_suffix(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("users--")

    def test_space_in_name(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("my table")

    def test_dash_in_name(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("my-table")

    def test_dot_notation(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("public.users")

    def test_wildcard(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("users*")

    def test_starts_with_digit(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("1table")

    def test_empty_string(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("")

    def test_null_byte(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("users\x00")

    def test_newline(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("users\nDROP TABLE users")
