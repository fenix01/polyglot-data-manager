from dependency_injector import containers, providers
from polymanager.containers.core_container import CoreContainer
from polymanager.core.arangodb.arangodb_handler import ArangoDBHandler

class ArangoDBContainer(containers.DeclarativeContainer):

    handler : ArangoDBHandler = providers.Factory(
            ArangoDBHandler,
            hostname=CoreContainer.config.arangodb_hostname,
            port=CoreContainer.config.arangodb_port,
            user=CoreContainer.config.arangodb_user,
            password=CoreContainer.config.arangodb_password
        )
