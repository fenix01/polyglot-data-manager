from dependency_injector import containers, providers


class CoreContainer(containers.DeclarativeContainer):
    """IoC container of core component providers."""

    config = providers.Configuration('config')
