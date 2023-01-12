from datetime import timedelta

from flask_cors import CORS
from flask_jwt_extended import JWTManager

from core.dao.basedao import BaseDao
from core.utils.fileutils import read_section_in_ini_file

from flask import Flask

from impl.rest.dbrestcontroller import db_service_blueprint
from impl.rest.userrestcontroller import user_service_blueprint


def create_app():
    """
    Crea la app de flask.
    :return: app
    """
    app_ = Flask(__name__)

    # Configuración Json Web Token
    app_.config["JWT_SECRET_KEY"] = read_section_in_ini_file(file_name="config", section="JWT")["jwt_secret_key"]
    app_.config["JWT_TOKEN_LOCATION"] = ["headers"]
    app_.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=1)

    # @app_.before_request
    # def foo(response):
    #     pass

    # @app_.after_request
    # def foo(response):
    #     pass

    return app_


if __name__ == '__main__':
    # Configurar Dao desde fichero ini
    db_config = read_section_in_ini_file(file_name="config", section="DB")
    BaseDao.set_db_config_values(**db_config)

    # Configurar app
    app_config = read_section_in_ini_file(file_name="config", section="REST")

    # Aplicación flask
    app = create_app()

    jwt = JWTManager(app)

    # Registro de blueprints
    app.register_blueprint(db_service_blueprint)
    app.register_blueprint(user_service_blueprint)

    # CORS para habilitar llamadas cross-origin a la api
    CORS(app)
    # CORS(app, support_credentials=True)

    # Ejecutar app
    app.run(**app_config)
