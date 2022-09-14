from dependency_injector import containers, providers
from polymanager.core.dgraph.dgraph_conn import DGraphConn
from polymanager.containers.core_container import CoreContainer
from polymanager.core.dgraph.dgraph_handler import DGraphHandler

class DGraphContainer(containers.DeclarativeContainer):

    conn = providers.Singleton(
        DGraphConn,
        CoreContainer.config.dgraph_addresses,
        port=CoreContainer.config.dgraph_port
    )

    handler : DGraphHandler = providers.Factory(DGraphHandler, conn, dgraph_admin=CoreContainer.config.dgraph_addresses, dgraph_admin_port=CoreContainer.config.dgraph_admin_port)
