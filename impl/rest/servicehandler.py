from core.service.service import ServiceFactory
from impl.service.serviceimpl import TipoClienteServiceImpl, UsuarioServiceImpl, ClienteServiceImpl

ClienteRestService: ClienteServiceImpl = ServiceFactory.get_service(ClienteServiceImpl)
"""Service de clientes."""
TipoClienteRestService: TipoClienteServiceImpl = ServiceFactory.get_service(TipoClienteServiceImpl)
"""Service de tipos de cliente."""
UsuarioRestService: UsuarioServiceImpl = ServiceFactory.get_service(UsuarioServiceImpl)
"""Service de usuarios."""
