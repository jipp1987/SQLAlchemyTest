metadic = {}


def _generatemetaclass(bases, metas, priority):
    trivial = lambda m: sum([issubclass(M, m) for M in metas], m is type)
    # m es trivial si es 'type' o, en el caso explícito
    # de que se hayan dado metaclases, si es una superclase de al menos uno de ellos.
    metabs = tuple([mb for mb in map(type, bases) if not trivial(mb)])
    metabases = (metabs + metas, metas + metabs)[priority]
    if metabases in metadic:  # metaclase ya generada
        return metadic[metabases]
    elif not metabases:  # metabase trivial
        meta = type
    elif len(metabases) == 1:  # single metabase
        meta = metabases[0]
    else:  # multiple metabases
        metaname = "_" + ''.join([m.__name__ for m in metabases])
        meta = makecls()(metaname, metabases, {})
    return metadic.setdefault(metabases, meta)


def makecls(*metas, **options):
    """Factoría de clases evitanto conflictos de metatipos. La sintaxis de invocación es
    makecls(M1,M2,..,priority=1)(name,bases,dic). Si las clases base tienen
    conflictos de metaclases entre ellas o con las metaclases dadas,
    automáticamente genera una metaclase compatible y la instancia.
    Si prioridad es True, las metaclases dadas tiene prioridad sobre las metaclases base."""

    priority = options.get('priority', False)  # default, no priority
    return lambda n, b, d: _generatemetaclass(b, metas, priority)(n, b, d)