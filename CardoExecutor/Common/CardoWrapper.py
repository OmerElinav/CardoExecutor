_INNER = "_inner"
PRIVATE_PREFIX = "_"


def get_wrapped_attribute(cardo_object, item: str, inner_object_name: str = _INNER):
    df = object.__getattribute__(cardo_object, inner_object_name)
    return _get_attribute(cardo_object, df, type(df), item)


def _get_attribute(cardo_object, inner, inner_type, item: str):
    if hasattr(inner_type, item):
        value = object.__getattribute__(inner, item)
        return _wrap(value, cardo_object, inner_type) if callable(value) else value
    return object.__getattribute__(cardo_object, item)


def _wrap(func, outer, inner):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, inner):
            return type(outer)(result, name=outer.name)
        return result

    return wrapper
