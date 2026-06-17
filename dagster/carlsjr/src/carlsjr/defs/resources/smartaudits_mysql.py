import os
from typing import Any, Dict

from dagster import resource


class SmartAuditsMySqlResource:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        driver: str,
        fetch_size: int,
        use_ssl: str,
        allow_public_key_retrieval: str,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.driver = driver
        self.fetch_size = fetch_size
        self.use_ssl = use_ssl
        self.allow_public_key_retrieval = allow_public_key_retrieval

    def get_basic_options(self) -> Dict[str, Any]:
        url = (
            "jdbc:mysql://"
            + self.host
            + ":"
            + str(self.port)
            + "/"
            + self.database
            + "?zeroDateTimeBehavior=CONVERT_TO_NULL"
            + "&serverTimezone=UTC"
            + "&useSSL="
            + self.use_ssl
            + "&allowPublicKeyRetrieval="
            + self.allow_public_key_retrieval
        )
        return {
            "url": url,
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
            "fetchsize": str(self.fetch_size),
        }


@resource
def smartaudits_mysql_resource(_):
    host = os.getenv("SMARTAUDITS_MYSQL_HOST", "")
    port = int(os.getenv("SMARTAUDITS_MYSQL_PORT", "3306"))
    database = os.getenv("SMARTAUDITS_MYSQL_DATABASE", "")
    user = os.getenv("SMARTAUDITS_MYSQL_USER", "")
    password = os.getenv("SMARTAUDITS_MYSQL_PASSWORD", "")
    driver = os.getenv("SMARTAUDITS_MYSQL_DRIVER", "com.mysql.cj.jdbc.Driver")
    fetch_size = int(os.getenv("SMARTAUDITS_MYSQL_FETCH_SIZE", "10000"))
    use_ssl = os.getenv("SMARTAUDITS_MYSQL_USE_SSL", "false")
    allow_public_key_retrieval = os.getenv(
        "SMARTAUDITS_MYSQL_ALLOW_PUBLIC_KEY_RETRIEVAL",
        "true",
    )
    return SmartAuditsMySqlResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        driver=driver,
        fetch_size=fetch_size,
        use_ssl=use_ssl,
        allow_public_key_retrieval=allow_public_key_retrieval,
    )
