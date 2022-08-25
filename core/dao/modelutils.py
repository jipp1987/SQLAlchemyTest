from typing import Union, List

from sqlalchemy import inspect
from sqlalchemy.orm import declarative_base
from sqlalchemy.util import symbol

BaseEntity = declarative_base()
"""Declaración de clase para mapeo de todas la entidades de la base de datos."""


def find_entity_id_field_name(entity_type: type(BaseEntity)) -> Union[str, List[str]]:
    """
    Devuelve el nombre del campo de la clave primaria de la entidad. Puede devolver un listado de strings si
    tuviese más de una, como por ejemplo el caso de los modelos de relaciones n a m.
    :param entity_type: Tipo de la entidad, siempre y cuando herede de BaseEntity.
    :return: Nombre del campo id de la entidad, o un listado de strings para el caso de entidades con más de una pk.
    """
    primary_keys = [key.name for key in inspect(entity_type).primary_key]

    if primary_keys is None:
        raise RuntimeError(f"Entity {entity_type.__name__} does not have a primary key defined.")

    return primary_keys[0] if len(primary_keys) == 1 else primary_keys


def deserialize_model(model_dict: dict, entity_type: type(BaseEntity), only_set_foreign_key: bool = False) \
        -> BaseEntity:
    """
    Convierte de diccionario a modelo de SQLAlchemy.
    :param model_dict: Diccionario json con los valores del objeto.
    :param entity_type: Tipo de entidad, heredando de BaseEntity.
    :param only_set_foreign_key: Si True, respecto a las entidades anidadas sólo se fijará el valor de la
    foreign key, no de la relación completa. OJO!!! En caso de True para entidades existentes (con id), sólo
    está probado de momento para entidades cargadas por id, sin ningún tipo de relación cargada, sólo la foreign key.
    :return: Nueva entidad del tipo pasado como parámetro.
    """
    # Instancio una nueva entidad del tipo pasado como parámetro
    new_entity = entity_type()
    set_model_properties_by_dict(model_dict=model_dict, entity=new_entity, only_set_foreign_key=only_set_foreign_key)

    return new_entity


def set_model_properties_by_dict(entity: BaseEntity, model_dict: dict, only_set_foreign_key: bool = False) -> None:
    """
    Establece las propiedades de una entidad a partir de un diccionario; las propiedades que no formen parte del
    diccionario se dejarán tal cual estén en la entidad.
    :param entity: Entidad a modificar.
    :param model_dict: Diccinoario de valores.
    :param only_set_foreign_key: Si True, respecto a las entidades anidadas sólo se fijará el valor de la
    foreign key, no de la relación completa. OJO!!! En caso de True para entidades existentes (con id), sólo
    está probado de momento para entidades cargadas por id, sin ningún tipo de relación cargada, sólo la foreign key.
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
        nested_entity_id_field: Union[str, List[str]]
        nested_entity_id: Union[int, dict]
        is_many_to_many: bool
        is_one_to_many: bool
        entity_list: list

        for rel in relationships:
            # Comprobar si es una relación many-many o one-to-many
            is_many_to_many = rel.direction is not None and rel.direction == symbol("MANYTOMANY")
            is_one_to_many = rel.direction is not None and rel.direction == symbol("ONETOMANY")
            if is_many_to_many or is_one_to_many:
                continue

            related_key = None
            # Esto puede devolver una lista si es una entidad compuesta, pero no llegará a pasar porque ya estoy
            # comprobando el tipo de relación antes
            nested_entity_id_field = find_entity_id_field_name(rel.entity.class_)

            if rel.key in model_dict:
                for lcl in rel.local_columns:
                    # Buscar el nombre de la foreign_key para completar el dato
                    related_key = ins.mapper.get_property_by_column(lcl).key
                    break

                if related_key:
                    # Si es para un update de una entidad existente, sólo me centro en las foreign keys sin
                    # ignorando las relaciones para evitar problemas de integridad.
                    if only_set_foreign_key:
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
                            # Completo la columna de la foreign key: el valor es el que corresponde al id de la
                            # clase anidada
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
        is_many_to_many: bool
        is_one_to_many: bool

        for rel in relationships:
            # Comprobar si es una relación many-many o one-to-many
            is_many_to_many = rel.direction is not None and rel.direction == symbol("MANYTOMANY")
            is_one_to_many = rel.direction is not None and rel.direction == symbol("ONETOMANY")

            # Si es una relación one_to_many o mm, es un listado de objetos. Añadimos un diccionario por cada
            # elemento contenido.
            if is_many_to_many or is_one_to_many:
                attr = getattr(model, rel.key)

                if attr:
                    json_dict[rel.key] = []
                    for i in attr:
                        json_dict[rel.key].append(serialize_model(i))

                continue

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
                        # OJO!!! NO debería llegar nada aquí con más de una clave primaria, find_entity_id_field_name
                        # puede devolver una lista de strings pero no debería llegar hasta aquí ese caso porque ya estoy
                        # controlando las entidades many to many.
                        lazyload_dict = {find_entity_id_field_name(rel.mapper.class_): attr}
                        json_dict[rel.key] = lazyload_dict if attr is not None else None
                        break

    return json_dict
