from typing import Union

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


def deserialize_model(model_dict: dict, entity_type: type(BaseEntity)) -> BaseEntity:
    """
    Convierte de diccionario a modelo de SQLAlchemy.
    :param model_dict: Diccionario json con los valores del objeto.
    :param entity_type: Tipo de entidad, heredando de BaseEntity.
    :return: Nueva entidad del tipo pasado como parámetro.
    """
    # Instancio una nueva entidad del tipo pasado como parámetro
    new_entity = entity_type()
    set_model_properties_by_dict(model_dict=model_dict, entity=new_entity)

    return new_entity


def set_model_properties_by_dict(entity: BaseEntity, model_dict: dict, is_an_update: bool = False) -> None:
    """
    Establece las propiedades de una entidad a partir de un diccionario; las propiedades que no formen parte del
    diccionario se dejarán tal cual estén en la entidad.
    :param entity: Entidad a modificar.
    :param model_dict: Diccinoario de valores.
    :param is_an_update: Si se están estableciendo los valores para un update, respecto a las entidades anidadas sólo
    se fijará el valor de la foreign key, no de la relación completa. OJO!!! En caso de True, sólo está probado de
    momento para entidades cargadas por id, sin ningún tipo de relación cargada, sólo la foreign key.
    :return: None
    """
    # Recorrer las columnas de la clase, ignorando por el momento las relaciones
    entity_type: type(BaseEntity) = type(entity)
    columns = entity_type.__table__.columns

    for column in columns:
        if column.name in model_dict:
            setattr(entity, column.name, model_dict[column.name])

    # Obtener relaciones del modelo
    relationships = entity_type.__mapper__.relationships

    if relationships:
        ins = inspect(entity_type)

        related_key: Union[str, None]
        nested_entity: BaseEntity
        nested_entity_id_field: str
        nested_entity_id: any

        for rel in relationships:
            related_key = None
            nested_entity_id_field = find_entity_id_field_name(rel.entity.class_)

            if rel.key in model_dict:
                for lcl in rel.local_columns:
                    # Buscar el nombre de la foreign_key para completar el dato
                    related_key = ins.mapper.get_property_by_column(lcl).key
                    break

                if related_key:
                    # Si es para un update de una entidad existente, sólo me centro en las foreign keys sin ignorando
                    # las relaciones para evitar problemas de integridad.
                    if is_an_update:
                        # Busco en el diccionario la clave perteneciente al id de la entidad anidada.
                        # Si no lo encuentra lanzará un KeyError.
                        if model_dict[rel.key] is not None:
                            nested_entity_id = model_dict[rel.key][nested_entity_id_field]
                            # Establezco el valor únicamente de la foreign_key asociada a la relación
                            setattr(entity, related_key, nested_entity_id)
                        else:
                            # Si llega como null es que quieren eliminar la relación
                            setattr(entity, related_key, None)
                    else:
                        # Llamo recursivamente a esta función para crear la entidad anidada
                        if model_dict[rel.key] is not None:
                            nested_entity = deserialize_model(model_dict[rel.key], rel.entity.class_)
                            setattr(entity, rel.key, nested_entity)
                            # Completo la columna de la foreign key: el valor es el que corresponde al id de la clase
                            # anidada
                            setattr(entity, related_key, getattr(nested_entity, nested_entity_id_field))
                        else:
                            # Si ha llegado como None significa que quieren eliminar la relación.
                            setattr(entity, rel.key, None)
                            setattr(entity, related_key, None)


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
