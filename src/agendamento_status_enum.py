from enum import Enum

class AgendamentoStatus(Enum):
    EmAnalise = "Em análise"
    Confirmado = "Confirmado"
    Rejeitado = "Rejeitado"
    Error = "Error"