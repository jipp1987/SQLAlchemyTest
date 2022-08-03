import types
from typing import Callable, Dict, Type

from core.dao.basedao import BaseDao, BaseEntity
from core.exception.errorhandler import ErrorHandler


def service_method(function):
    """Utilizo este decorador para establecer un método de BaseService como método de transacción
    en la base de datos."""

    def wrapped(*args, **kwargs):
        # Esta parte sería para añadir alguna lógica en la función, pero no es el caso. Realmente la quiero devolver
        # como tal pero con un atributo a mayores para saber que le he añadido un decorador "service_function_dec". Lo
        # utilizo para saber qué funciones deben iniciar transacciones de base de datos y hacer rollback en caso de
        # error.
        pass

    # Le puedo añadir a cualquier función un atributo al vuelo, así que le añado uno para saber que tiene etiqueta
    function.is_service_method = True
    return function


class BaseService(object, metaclass=ErrorHandler):
    """
    Clase abstracta de la que han de heredar el resto de servicios del programa.
    """

    def __init__(self, dao: BaseDao = None):
        """
        Constructor.
        :param dao: DAO.
        """
        self._dao = dao
        """Acceso a datos asociado al servicio."""

    # Sobrescribo __getattribute__ para interceptar las llamadas a las funciones con el objetivo de envolverlas
    # automáticamente en transacciones, a modo de interceptor de llamadas a funciones.
    def __getattribute__(self, name):
        # Obtengo los atributos de la clase usando la función de la superclase object
        attr = object.__getattribute__(self, name)

        # Compruebo si el atributo es un método (una función de la clase, con parámetro "self"). También compruebo
        # si tiene un atributo llamado "is_service_method", lo cual significa que le he aplicado el decorador
        # "service_function_dec". Además, también compruebo que no haya sesión en el hilo actual: la idea es que sólo la
        # primera función de un service (sea cual sea) que inicie un proceso en la base de datos inicie la transacción;
        # cualquier otra función que de servicio (sea del mismo u otros) que tenga dentro de la función principal no
        # intente iniciar transacción, hacer commit o rollback si falla algo, que sólo lo haga la función principal.
        if isinstance(attr, types.MethodType) and getattr(attr, 'is_service_method', False) \
                and not self._dao.is_there_any_session_in_current_thread():
            # Creo una nueva función, que en realidad es la misma pero "envuelta" en la función de iniciar transacción
            def transaction_func(*args, **kwargs):
                result = self.__start_transaction(attr, *args, **kwargs)
                return result

            return transaction_func
        else:
            return attr

    def get_entity_type(self) -> type(BaseEntity):
        """
        Devuelve el tipo de entidad usando el dao asociado.
        :return: Tipo de entidad.
        """
        return self._dao.entity_type

    def __start_transaction(self, function: Callable, *args, **kwargs):
        """
        Envuelve la función dentro de un contexto transaccional: se hace commit al final si no hay problemas,
        y si sucede algo se hace rollback.
        :param function: Función miembro a ejecutar. Se accede a ella poniendo primero su nombre prececido de punto
        y el nombre del objeto al que pertenece
        :param args: Argumentos de la función que no se han identificado por clave.
        :param kwargs: Argumentos de la función que se han identificado por clave.
        :return: Resultado de la función
        """
        try:
            # Crear sesión
            self._dao.create_session()

            # Realizar función
            result = function(*args, **kwargs)

            # Hacer commit al final
            self._dao.commit()

            # Devolver resultado
            return result
        except Exception as e:
            # Si hay algún error, hacer rollback y devolver error hacia arriba.
            self._dao.rollback()
            raise e
        finally:
            # Desconectar siempre al final (sólo desconecta la función original, la que inició la transacción y solicitó
            # la conexión del hilo)
            self._dao.close_session()

    # FUNCIONES DE ACCESO A DATOS
    @service_method
    def create(self, registry: BaseEntity) -> None:
        """
        Crea una entidad en la base de datos y sincroniza su id.
        :param registry: Registro a crear.
        :return: None
        """
        self._dao.create(registry)

    @service_method
    def delete_by_id(self, registry_id: any) -> None:
        """
        Elimina un registro por id.
        :param registry_id: Id del registro a eliminar.
        :return: None
        """
        self._dao.delete_by_id(registry_id)

    @service_method
    def find_by_id(self, registry_id: any):
        """
        Devuelve un registro a partir de un id.
        :param registry_id: Id del registro en la base de datos.
        :return: Una instancia de la clase principal del dao si el registro exite; None si no existe.
        """
        return self._dao.find_by_id(registry_id)

    @service_method
    def find_last_entity(self) -> BaseEntity:
        return self._dao.find_last_entity()

    @service_method
    def select(self, filter_clause=None):
        return self._dao.select(filter_clause)


class ServiceFactory(object):
    """
    Factoría de servicios, de tal modo que sólo se tendrá en el contexto de la aplicación una instancia de un servicio.
    """

    __services: Dict[str, BaseService] = {}
    """Diccionario de servicios ya instanciados. La clave es el nombre del servicio y el valor la instancia única del 
    mismo."""

    @classmethod
    def get_service(cls, class_to_instanciate: Type[BaseService]):
        """
        Devuelve una instancia de un servicio. Si no existe, la instancia y la guarda en el diccionario; si existe,
        devuelve la que tenga ya almacenada.
        :param class_to_instanciate: Clase del servicio a instanciar. Es el tipo, directamente, no el nombre.
        :return: Servicio.
        """
        # Obtengo el nombre de la clase
        class_name = class_to_instanciate.__name__

        # Si no existe, la creo y la almaceno en el diccionario
        if class_name not in cls.__services:
            cls.__services[class_name] = class_to_instanciate()

        # devuelvo la instancia del diccionario
        return cls.__services[class_name]