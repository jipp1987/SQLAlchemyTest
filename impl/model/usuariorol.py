from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship

from core.dao.modelutils import BaseEntity


class UsuarioRol(BaseEntity):
    """Modelo de relación m a n de usuarios-roles."""

    __tablename__ = "usuariosroles"

    rolid = Column(Integer, ForeignKey("roles.id"), primary_key=True)
    usuarioid = Column(Integer, ForeignKey("usuarios.id"), primary_key=True)

    rol = relationship("Rol", foreign_keys=[rolid], lazy="raise", overlaps="usuarios_roles")
    usuario = relationship("Usuario", foreign_keys=[usuarioid], lazy="raise", overlaps="usuarios_roles")

    def __init__(self, **kwargs):
        super(UsuarioRol, self).__init__(**kwargs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.rolid == other.rolid and self.usuarioid == other.usuarioid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'[UsuarioRol] rolid = {self.rolid}, usuarioid = {self.usuarioid}'
