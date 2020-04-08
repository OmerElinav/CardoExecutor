class CardoWrapper(type):
    def __init__(cls, name, bases, namespace):
        for base in bases:
            _wrap_base(base, cls)
        super(CardoWrapper, cls).__init__(name, bases, namespace)


def _wrap(func, wrapping_class):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, wrapping_class):
            return wrapping_class(result)
        return result

    return wrapper


def _wrap_base(base, wrapping_class):
    for key, value in vars(base).items():
        if callable(value) and key not in vars(wrapping_class).keys():
            wrap = _wrap(value, wrapping_class)
            setattr(wrapping_class, key, wrap)


def get_wrapped_attribute(cardo_object, item: str, inner_object_name: str):
    df = object.__getattribute__(cardo_object, inner_object_name)
    df_type = type(df)
    if hasattr(df_type, item):
        return object.__getattribute__(df, item)
    return object.__getattribute__(cardo_object, item)
