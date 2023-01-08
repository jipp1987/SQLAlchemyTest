from typing import List, Union

from flask import Blueprint
from flask_jwt_extended import get_jwt_identity

from core.dao.daotools import JsonQuery
from core.dao.modelutils import serialize_model, deserialize_model, find_entity_id_field_name
from core.rest.apitools import RequestResponse, EnumHttpResponseStatusCodes, DBRequestBody, \
    convert_request_response_to_json_response
from core.service.service import BaseService
from impl.rest.restutils import db_rest_fn

db_service_blueprint = Blueprint("DBService", __name__, url_prefix='/api/DBService')
"""Blueprint para módulo de api."""


@db_service_blueprint.route('/create', methods=['POST'])
def create():
    """
    Servicio Rest para crear entidades en la base de datos.
    """

    @db_rest_fn
    def __create(*args, **kwargs): # noqa
        """
        Función interna create.
        :param request_body:
        :param service:
        :return: Response.
        """
        request_body: DBRequestBody = kwargs["request_body"]
        service: BaseService = kwargs["service"]

        # Objeto query_object creado a partir del request_object
        entity_to_be_created = deserialize_model(request_body.request_object, service.get_entity_type(), True)
        service.create(entity_to_be_created)

        json_result = f"'{entity_to_be_created}' has been created."
        response_body = RequestResponse(response_object=json_result, success=True,
                                        status_code=EnumHttpResponseStatusCodes.OK.value)

        return convert_request_response_to_json_response(response_body)

    return __create()


@db_service_blueprint.route('/load', methods=['POST'])
def load():
    """
    Servicio Rest para carga completa de entidades.
    """

    @db_rest_fn
    def __load(*args, **kwargs):  # noqa
        request_body: DBRequestBody = kwargs["request_body"]
        service: BaseService = kwargs["service"]

        # Objeto query_object creado a partir del request_object
        entity_id = request_body.request_object["entity_id"]
        entity = service.load(entity_id)
        json_result = serialize_model(entity)

        response_body = RequestResponse(response_object=json_result, success=True,
                                        status_code=EnumHttpResponseStatusCodes.OK.value)

        return convert_request_response_to_json_response(response_body)

    return __load()


@db_service_blueprint.route('/update', methods=['POST'])
def update():
    """
    Servicio Rest para actualizar entidades en la base de datos.
    """

    @db_rest_fn
    def __update(*args, **kwargs): # noqa
        """
        Función interna update.
        :param request_body:
        :param service:
        :return: Response.
        """
        request_body: DBRequestBody = kwargs["request_body"]
        service: BaseService = kwargs["service"]

        # Recupero el id de la entidad del diccionario de valores.
        id_field_name: Union[str, list] = find_entity_id_field_name(service.get_entity_type())
        entity_id: any

        # Si el id field es un listado significa que es una tabla con más de una clave primaria y no debería pasar
        # utilizar un rest service para modificar sus datos, deberían venir siempre como dato adicional de una
        # tabla principal
        if isinstance(id_field_name, list):
            raise ValueError(f"Entity type {service.get_entity_type().__name__} not allowed for direct update.")

        if id_field_name in request_body.request_object:
            entity_id = request_body.request_object[id_field_name]
        else:
            raise KeyError("You have to specify the id of the entity on the request.")

        # Actualizo los campos pasados como parámetro.
        entity_to_be_updated = service.update_fields(registry_id=entity_id, values_dict=request_body.request_object)

        json_result = f"'{entity_to_be_updated}' has been updated."
        response_body = RequestResponse(response_object=json_result, success=True,
                                        status_code=EnumHttpResponseStatusCodes.OK.value)

        return convert_request_response_to_json_response(response_body)

    return __update()


@db_service_blueprint.route('/delete', methods=['POST'])
def delete():
    """
    Servicio Rest para eliminar entidades en la base de datos.
    """

    @db_rest_fn
    def __delete(*args, **kwargs): # noqa
        """
        Función interna delete.
        :param request_body:
        :param service:
        :return: Response.
        """
        request_body: DBRequestBody = kwargs["request_body"]
        service: BaseService = kwargs["service"]

        # Objeto query_object creado a partir del request_object
        entity_to_be_deleted = deserialize_model(request_body.request_object, service.get_entity_type())
        service.delete(entity_to_be_deleted)

        json_result = f"'{entity_to_be_deleted}' has been deleted."
        response_body = RequestResponse(response_object=json_result, success=True,
                                        status_code=EnumHttpResponseStatusCodes.OK.value)

        return convert_request_response_to_json_response(response_body)

    return __delete()


@db_service_blueprint.route('/select', methods=['POST'])
def select():
    """
    Servicio Rest para seleccionar entidades de la base de datos.
    """

    @db_rest_fn
    def __select(*args, **kwargs): # noqa
        """
        Función interior select.
        :param request_body:
        :param service:
        :return: Response.
        """
        # En realidad estas dos variables podrían ser los parámetros de la función... pero si no utilizo
        # "*args, **kwargs" el debugger no sabe llegar hasta aquí sin hacer step into desde el decorador.
        request_body: DBRequestBody = kwargs["request_body"]
        service: BaseService = kwargs["service"]

        # Obtener identidad del usuario
        current_user_id = get_jwt_identity()

        result: list

        # Consulta
        json_result: List[dict] = []

        # Objeto query_object creado a partir del request_object
        query_object = JsonQuery(request_body.request_object)

        # Si la consulta ha llegado con field_clauses, es una selección de campos individuales. Si no ha llegado con
        # field_clauses, es una selección de entidades.
        if query_object.fields:
            result = service.select_fields(filter_clauses=query_object.filters, order_by_clauses=query_object.order,
                                           join_clauses=query_object.joins,
                                           field_clauses=query_object.fields, group_by_clauses=query_object.group_by,
                                           limit=query_object.limit, offset=query_object.offset)
            if result:
                for r in result:
                    # Creo instancias de la entidad y la serializo
                    json_result.append(serialize_model(r))
        else:
            result = service.select(filter_clauses=query_object.filters, order_by_clauses=query_object.order,
                                    join_clauses=query_object.joins, limit=query_object.limit,
                                    offset=query_object.offset)
            # en este caso, el resultado hay que serializarlo al ser modelos de la base de datos
            if result:
                for r in result:
                    json_result.append(serialize_model(r))

        response_body: RequestResponse = RequestResponse(response_object=json_result, success=True,
                                                         status_code=EnumHttpResponseStatusCodes.OK.value)

        return convert_request_response_to_json_response(response_body)

    return __select()


@db_service_blueprint.route('/count', methods=['POST'])
def count():
    """
    Servicio Rest para seleccionar entidades de la base de datos.
    """

    @db_rest_fn
    def __count(*args, **kwargs): # noqa
        """
        Función interior select.
        :param request_body:
        :param service:
        :return: Response.
        """
        # En realidad estas dos variables podrían ser los parámetros de la función... pero si no utilizo
        # "*args, **kwargs" el debugger no sabe llegar hasta aquí sin hacer step into desde el decorador.
        request_body: DBRequestBody = kwargs["request_body"]
        service: BaseService = kwargs["service"]

        result: int

        # Objeto query_object creado a partir del request_object
        query_object = JsonQuery(request_body.request_object)

        # Utilizo count_by_filtered_query del servicio asociado
        result = service.count_by_filtered_query(filter_clauses=query_object.filters, join_clauses=query_object.joins)

        response_body: RequestResponse = RequestResponse(response_object=result, success=True,
                                                         status_code=EnumHttpResponseStatusCodes.OK.value)

        return convert_request_response_to_json_response(response_body)

    return __count()
