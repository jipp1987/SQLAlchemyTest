from sqlalchemy import inspect
from sqlalchemy.orm import declarative_base


BaseEntity = declarative_base()
"""Declaración de clase para mapeo de todas la entidades de la base de datos."""


def serialize_model(model: BaseEntity) -> dict:
    """
    Convierte a json un modelo de SQLAlchemy.
    :param model:
    :return: dicctionario de datos de la entidad.
    """
    json_dict: dict = {}

    columns = model.__table__.columns
    for column in columns:
        # Las columnas de tipo foreign_key no las quiero exportar
        if getattr(type(model), column.name).foreign_keys:
            continue

        # Añado clave-valor al diccionario
        json_dict[column.name] = getattr(model, column.name)

    # Relaciones
    relationships = model.__mapper__.relationships
    if relationships:
        # Compruebo las entidades no cargadas para evitar lazyloads
        ins = inspect(model)

        attr: any
        for rel in relationships:
            # Si no está en el set de propiedades no cargadas, la guardo en el diccionario
            if rel.key not in ins.unloaded:
                # Vigilar posibles valores null
                attr = getattr(model, rel.key)
                # Si es not null, llamo recursivamente a esta función
                json_dict[rel.key] = serialize_model(attr) if attr is not None else None

    return json_dict
