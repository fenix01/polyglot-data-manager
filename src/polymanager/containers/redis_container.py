from dependency_injector import containers, providers
from polymanager.containers.core_container import CoreContainer
import redis

class RedisContainer(containers.DeclarativeContainer):

    db : redis.Redis = providers.Factory(
            redis.Redis,
            host=CoreContainer.config.kvrocks_hostname,
            port=CoreContainer.config.kvrocks_port
    )