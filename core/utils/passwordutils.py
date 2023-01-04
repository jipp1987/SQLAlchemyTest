import bcrypt


def hash_password_using_bcrypt(passwd: str) -> str:
    """
    Crea un bcrypt hash para un password.
    :param passwd:
    :return: bytes
    """
    # gensalt genera un salt, que es la primera parte del hash.
    bvalue = bytes(passwd, 'utf-8')
    temp_hash = bcrypt.hashpw(bvalue, bcrypt.gensalt())
    return temp_hash.decode('utf-8')


def check_password_using_bcrypt(passwd: str, hashed: str) -> bool:
    """
    Comprueba si un password equivale a un hash usando bcrypt.
    :param passwd:
    :param hashed:
    :return: bool
    """
    return bcrypt.checkpw(passwd.encode('utf-8'), hashed.encode('utf-8'))
