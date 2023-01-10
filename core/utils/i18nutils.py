import gettext
import locale
import os

from core.utils.fileutils import get_project_root_dir

__languages = {
    # base es el nombre del fichero mo, locales es el directorio en donde se encuentra
    "es_ES": gettext.translation('base', os.path.abspath(os.path.dirname(__file__))
                                 + f'{os.path.join(get_project_root_dir(), "/resources")}/locales',
                                 languages=['es_ES'], fallback=True),
    "en_GB": gettext.translation('base', os.path.abspath(os.path.dirname(__file__))
                                 + f'{os.path.join(get_project_root_dir(), "/resources")}/locales',
                                 languages=['en_GB'], fallback=True)
}
"""Diccionario con traductores de gettext identificados por el código iso del locale. OJO!!! La ruta del directorio de 
locales se espera que esté en la raíz del proyecto/resources"""


def change_locale(locale_iso: str):
    """Cambia el locale del sistema.
    :param locale_iso: Código Iso del locale al que se quiere cambiar
    """
    locale.setlocale(locale.LC_ALL, locale_iso)


def translate(key: str, locale_iso: str = None, args: list = None):
    """Traduce una clave i18n Parameters:
    :param: str:Clave i18n a traducir
    :param: str:Iso del locale al que se quiere traducir
    :param: args Posibles argumentos para sustituir valores que espera el valor de la clave en el fichero .po.
    :return: str:Clave traducida en función del locale actual
    """
    # Primero obtengo el valor de la clave en los ficheros po/mo
    # Locale es una tupla, el primer valor es el código del idioma que es lo que uso en como clave del diccionario
    result = __languages[locale_iso if locale_iso is not None else locale.getlocale()[0]].gettext(key)

    # Ahora, si han llegado parámetros para sustituir los placeholders, los sustituyo en el valor obtenido
    if args:
        result = result % (*args,)

    return result
