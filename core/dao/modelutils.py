from sqlalchemy import inspect
from sqlalchemy.orm import declarative_base

BaseEntity = declarative_base()
"""Declaración de clase para mapeo de todas la entidades de la base de datos."""


def find_entity_id_field_name(entity_type: type(BaseEntity)) -> str:
    """
    Devuelve el nombre del campo id de la entidad principal asociada al dao.
    :param entity_type: Tipo de la entidad, siempre y cuando herede de BaseEntity.
    :return: Nombre del campo id de la entidad.
    """
    id_field_name: str = "id"
    id_field_name_fn = getattr(entity_type, "get_id_field_name")

    if id_field_name_fn is not None:
        id_field_name = id_field_name_fn()

    return id_field_name


def serialize_model(model: BaseEntity) -> dict:
    """
    Convierte a json un modelo de SQLAlchemy.
    :return: dicctionario de datos de la entidad.
    """
    json_dict: dict = {}
    # Almaceno las foreign keys para el caso de entidades lazyload distintas de null. Devolveré un diccionario con el id
    # de la entidad al menos.
    foreign_key_names: list = []

    columns = model.__table__.columns
    for column in columns:
        # Las columnas de tipo foreign_key no las quiero exportar
        if getattr(type(model), column.name).foreign_keys:
            foreign_key_names.append(column.name)
            continue

        # Añado clave-valor al diccionario
        json_dict[column.name] = getattr(model, column.name)

    # Obtener relaciones del modelo
    relationships = model.__mapper__.relationships

    if relationships:
        # Compruebo las entidades no cargadas para evitar lazyloads
        ins = inspect(model)

        attr: any
        local_key: str
        # Este diccionario es para devolver algo en las entidades lazy, el id por lo menos aunque el resto no
        # esté definido
        lazyload_dict: dict

        for rel in relationships:
            # Si no está en el set de propiedades no cargadas, la guardo en el diccionario con todos los atributos
            # que tenga
            if rel.key not in ins.unloaded:
                # Vigilar posibles valores null
                attr = getattr(model, rel.key)
                # Si es not null, llamo recursivamente a esta función
                json_dict[rel.key] = serialize_model(attr) if attr is not None else None
            else:
                # Si no está cargada, al menos guardo un objeto de la clase que corresponda a la entidad lazyload con
                # el id cargado
                # Busco la foreing key asociada a la relación
                for lcl in rel.local_columns:
                    local_key = ins.mapper.get_property_by_column(lcl).key

                    # Compruebo por si acaso que se encuentra en la lista de foreing keys de la entidad
                    if local_key in foreign_key_names:
                        attr = getattr(model, local_key)
                        # Creo un diccionario con al menos el id de la entidad y lo añado al json_dict
                        lazyload_dict = {find_entity_id_field_name(rel.mapper.class_): attr}
                        json_dict[rel.key] = lazyload_dict if attr is not None else None
                        break

    return json_dict