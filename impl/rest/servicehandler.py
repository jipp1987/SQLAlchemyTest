from core.service.service import ServiceFactory
from impl.service.serviceimpl import TipoClienteServiceImpl, UsuarioServiceImpl, ClienteServiceImpl

ClienteService: ClienteServiceImpl = ServiceFactory.get_service(ClienteServiceImpl)
"""Service de clientes."""
TipoClienteService: TipoClienteServiceImpl = ServiceFactory.get_service(TipoClienteServiceImpl)
"""Service de tipos de cliente."""
UsuarioService: UsuarioServiceImpl = ServiceFactory.get_service(UsuarioServiceImpl)
"""Service de usuarios."""
