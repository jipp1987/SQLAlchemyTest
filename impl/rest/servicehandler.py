from core.service.service import ServiceFactory
from impl.service.serviceimpl import TipoClienteServiceImpl, UsuarioServiceImpl, ClienteServiceImpl, RolServiceImpl, \
    UsuarioRolServiceImpl

ClienteRestService: ClienteServiceImpl = ServiceFactory.get_service(ClienteServiceImpl)
"""Service de clientes."""
TipoClienteRestService: TipoClienteServiceImpl = ServiceFactory.get_service(TipoClienteServiceImpl)
"""Service de tipos de cliente."""
UsuarioRestService: UsuarioServiceImpl = ServiceFactory.get_service(UsuarioServiceImpl)
"""Service de usuarios."""
RolRestService: RolServiceImpl = ServiceFactory.get_service(RolServiceImpl)
"""Service de roles."""
UsuarioRolRestService: UsuarioRolServiceImpl = ServiceFactory.get_service(UsuarioRolServiceImpl)
"""Service de usuarios-roles."""
