from dependency_injector import containers, providers
from polymanager.containers.core_container import CoreContainer
from polymanager.core.manticore.manticore_handler import ManticoreHandler

class ManticoreContainer(containers.DeclarativeContainer):

    handler : ManticoreHandler = providers.Factory(
            ManticoreHandler,
            hostname=CoreContainer.config.manticoresearch_hostname,
            port=CoreContainer.config.manticoresearch_port,
        )
