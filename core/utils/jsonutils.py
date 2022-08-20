import inspect
import json
from json import JSONEncoder


class CustomJsonEncoder(JSONEncoder):
    """Codificador JSON de entidades."""

    def default(self, obj):
        # Si tiene una función to_json, se usa dicha función para la codificación.
        if hasattr(obj, "to_json"):
            return self.default(obj.to_json())
        elif hasattr(obj, "__dict__"):
            # En cualquier otro caso se usa el atributo builtin __dict__ que tienen todas las clases.
            d = dict(
                (key, value)
                # Aquí se descartan ciertos atributos, como los privados, abstractos, builtin...
                for key, value in inspect.getmembers(obj)
                if not key.startswith("__")
                and not inspect.isabstract(value)
                and not inspect.isbuiltin(value)
                and not inspect.isfunction(value)
                and not inspect.isgenerator(value)
                and not inspect.isgeneratorfunction(value)
                and not inspect.ismethod(value)
                and not inspect.ismethoddescriptor(value)
                and not inspect.isroutine(value)
            )
            return self.default(d)

        return obj


def encode_object_to_json(object_to_encode: any) -> str:
    """
    Codifica un objeto a json.
    :param object_to_encode:
    :return: str
    """
    return json.dumps(object_to_encode, cls=CustomJsonEncoder, indent=2, sort_keys=True,
                      ensure_ascii=False)


def decode_object_from_json(json_format: str, t: type) -> any:
    """
    Transforma un string formato json a una instancia de un objeto.
    :param json_format: String formato json.
    :param t: Tipo del objeto a instanciar.
    :return: Instancia de t con los atributos especificados en json_format.
    """
    # lo convierto a diccionario
    json_dict = json.loads(json_format)
    # Instanciar un nuevo tipo t.
    # El operador ** en este caso va a coger el diccionario y va a descomponerlo en grupos de clave valor, de tal
    # manera que la clave será el identificador del parámetro de __init__ y el valor su valor. Es decir, esto llama
    # al constructor de la clase; no importa el orden de los valores en json,
    # pero el nombre debe coincidir con el nombre de los parámetros del constructor.
    return t(**json_dict)
