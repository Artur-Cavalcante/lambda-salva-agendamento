from enum import Enum

class AgendamentoStatus(Enum):
    EmAnalise = 1
    Confirmado = 2
    Rejeitado = 3
    Error = 4