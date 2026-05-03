"""Unit tests for spark/jobs/silver_to_postgres.py."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_repo_root = Path(__file__).resolve().parents[2]
_candidate_paths = (
    _repo_root / "spark" / "jobs",
    Path("/opt/airflow/spark_jobs"),
)
for _path in _candidate_paths:
    if (_path / "silver_to_postgres.py").exists():
        sys.path.insert(0, str(_path))
        break

from silver_to_postgres import jdbc_url_to_psycopg2_dsn  # noqa: E402


class TestJdbcUrlToPsycopg2Dsn:
    def test_basic_url_with_port(self) -> None:
        result = jdbc_url_to_psycopg2_dsn(
            "jdbc:postgresql://postgres:5432/riskflow",
            user="riskflow", password="secret",
        )
        assert "host=postgres" in result
        assert "port=5432" in result
        assert "dbname=riskflow" in result
        assert "user=riskflow" in result
        assert "password=secret" in result

    def test_url_without_port_defaults_to_5432(self) -> None:
        result = jdbc_url_to_psycopg2_dsn(
            "jdbc:postgresql://localhost/mydb",
            user="u", password="p",
        )
        assert "host=localhost" in result
        assert "port=5432" in result
        assert "dbname=mydb" in result

    def test_url_with_alternate_port(self) -> None:
        result = jdbc_url_to_psycopg2_dsn(
            "jdbc:postgresql://db.example.com:6543/prod",
            user="u", password="p",
        )
        assert "host=db.example.com" in result
        assert "port=6543" in result
        assert "dbname=prod" in result

    def test_invalid_prefix_raises(self) -> None:
        with pytest.raises(ValueError, match="prefix"):
            jdbc_url_to_psycopg2_dsn(
                "postgresql://host/db",
                user="u", password="p",
            )

    def test_missing_database_raises(self) -> None:
        with pytest.raises(ValueError, match="database"):
            jdbc_url_to_psycopg2_dsn(
                "jdbc:postgresql://host:5432",
                user="u", password="p",
            )

    def test_special_chars_in_password_kept_intact(self) -> None:
        result = jdbc_url_to_psycopg2_dsn(
            "jdbc:postgresql://h:5432/d",
            user="u", password="p@ss w0rd!",
        )
        assert "password=p@ss w0rd!" in result
