from dataclasses import dataclass, field

@dataclass
class new_bar:
    symbol: str
    datetime: str
    close: int

@dataclass
class position_info:
    direction: int = 0
    position: int = 0