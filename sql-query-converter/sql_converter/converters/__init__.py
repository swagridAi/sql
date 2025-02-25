# sql_converter/converters/__init__.py
from .base import BaseConverter
# Register default converters
from .cte import CTEConverter
__all__ = ['CTEConverter']
_converters = {}

def register_converter(name: str, converter_class: type):
    if not issubclass(converter_class, BaseConverter):
        raise TypeError("Converters must inherit from BaseConverter")
    _converters[name] = converter_class

def get_converter(name: str, config: dict = None) -> BaseConverter:
    if name not in _converters:
        raise ValueError(f"Unknown converter: {name}")
    return _converters[name](config=config)


register_converter('cte', CTEConverter)
# Future converters would be registered here
# from .pivot import PivotConverter
# register_converter('pivot', PivotConverter)