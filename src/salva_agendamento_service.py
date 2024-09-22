import os
import json
import boto3
import pickle
from aws_lambda_powertools import Logger

from src.agendamento_status_enum import AgendamentoStatus


class SalvaAgendamentoService():
    def __init__(self, logger: Logger) -> None:
        self.logger = logger
        self.s3_client = boto3.client('s3')
        self.sqs_client = boto3.client("sqs")
        self.bucket_name = "bucket-agendamentos-fiap"
        self.url_fila_notificacao = os.environ["url_fila_notificacao"]

    def handle_agendamento(self, agendamento: dict) -> bool:
        crm_medico: str = agendamento["crm_medico"]
        horario: str = agendamento["horario"]
        path_arquivo_trava_horario: str = f'{crm_medico}-{horario}'

        self.logger.info(f'Path arquivo trava {path_arquivo_trava_horario}')
        arquivo = self.__ler_arquivo_s3(self.bucket_name, path_arquivo_trava_horario)
        self.logger.info(f'ARQUIVO {arquivo}')

        if(True): #TODO trocar aqui para verificar se arquivo existe
            self.__alterar_status_agendamento(agendamento['id'], AgendamentoStatus.Rejeitado)
            agendamento["status_agendamento"] = AgendamentoStatus.Rejeitado
            self.__envio_notificacao_email(agendamento, "email_paciente")
        else:
            self.__alterar_status_agendamento(agendamento['id'], AgendamentoStatus.Confirmado)
            agendamento["status_agendamento"] = AgendamentoStatus.Confirmado
            __envio_notificacao_email(agendamento, "email_paciente")
            __envio_notificacao_email(agendamento, "email_medico")
        
        #TODO dps ajustar a rota de buscar status do agendamentom, mas o que importa é o e-mail

    def __envio_notificacao_email(self, agendamento, email_para_envio):
        msg_email = agendamento
        msg_email["email_para_envio"] = email_para_envio
        self.sqs_client.send_message(QueueUrl=self.url_fila_notificacao, MessageBody=json.dumps(msg_email))

    def __ler_arquivo_s3(self, bucket_name, arquivo_s3):
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=arquivo_s3)
            conteudo = response['Body'].read().decode('utf-8')

            return json.loads(conteudo)

        except Exception as e:
            print(f'Erro ao ler o arquivo {arquivo_s3} do S3: {str(e)}')
            return None
    
    def __alterar_status_agendamento(self, id: str, novoStatus: AgendamentoStatus):
        try:
            response = self.s3_client.getObject(
                Bucket=self.bucket_name,
                Key=f"{id}.pkl"
            )

            self.logger.info(f'Response get {response}')

            conteudo = response['Body'].read().decode('utf-8')
            result = json.loads(conteudo)
            self.logger.info(f'Response result {result}')

            result["status_agendamento"] = novoStatus
            self.logger.info(f'Response result agendamento {result}')

            pickled_obj = pickle.dumps(result)
            self.logger.info(f'Response pickled {pickled_obj}')

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"{id}.pkl",
                Body=pickled_obj
            )

            self.logger.info(f'Status atualizado')
        except Exception as e:
            print(f'Erro ao atualizar status {id} do S3: {str(e)}')
            return None