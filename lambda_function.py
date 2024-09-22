import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.data_classes import event_source, SQSEvent

from src.salva_agendamento_service import SalvaAgendamentoService

logger = Logger(service="salva-agendamento") 
salva_agendamento_service = SalvaAgendamentoService(logger)

@event_source(data_class=SQSEvent)
def lambda_handler(event: SQSEvent, context) -> dict:
    try:
        for record in event.records:
            logger.info(f"Event: {record.body}")
            salva_agendamento_service.handle_agendamento(json.loads(record.body))
        return {
            "status_code": 200,
            "body": "Sucesso"
        }
    except Exception as ex:
        logger.error(f"Erro ao processar agendamento: {ex}")
        return {
            "status_code": 500,
            "body": "Erro ao processar evento"
        }