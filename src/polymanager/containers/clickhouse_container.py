from dependency_injector import containers, providers
from polymanager.containers.core_container import CoreContainer
from polymanager.core.clickchouse.clickhouse_handler import ClickhouseHandler

class ClickhouseContainer(containers.DeclarativeContainer):

    handler : ClickhouseHandler = providers.Factory(
            ClickhouseHandler,
            hostname=CoreContainer.config.clickhouse_hostname,
            port=CoreContainer.config.clickhouse_port
        )
