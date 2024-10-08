import os
import json
import boto3
import pickle
from datetime import datetime
from aws_lambda_powertools import Logger

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
        path_arquivo_trava_horario: str = f'{crm_medico}-{self.__format_time_trava(horario)}'

        self.logger.info(f'Path arquivo trava {path_arquivo_trava_horario}')
        existeTravaMedicoEHorario = self.__existe_trava_medico_e_horario(self.bucket_name, path_arquivo_trava_horario)
        self.logger.info(f'ExisteTravaMedicoEHorario {path_arquivo_trava_horario} {existeTravaMedicoEHorario}')

        if(existeTravaMedicoEHorario):
            self.__alterar_status_agendamento(agendamento['id'], "Rejeitado")
            agendamento["status_agendamento"] = "Rejeitado"
            self.__envio_notificacao_email(agendamento, email_para_envio = agendamento["email_paciente"], para_email_medico = False)
        else:
            self.__inserir_trava_horario(agendamento, path_arquivo_trava_horario)
            self.__alterar_status_agendamento(agendamento['id'], "Confirmado")
            agendamento["status_agendamento"] = "Confirmado"
            self.__envio_notificacao_email(agendamento, email_para_envio = agendamento["email_paciente"], para_email_medico = False)
            self.__envio_notificacao_email(agendamento, email_para_envio = agendamento["email_medico"], para_email_medico = True)

    def __envio_notificacao_email(self, agendamento, email_para_envio, para_email_medico):
        msg_email = agendamento
        msg_email["email_para_envio"] = email_para_envio
        msg_email["para_email_medico"] = para_email_medico
        self.logger.info(f"Iniciado envio para fila notificação {msg_email}")
        self.sqs_client.send_message(QueueUrl=self.url_fila_notificacao, MessageBody=json.dumps(msg_email))
        self.logger.info(f"Finalizando envio para fila notificação {msg_email}")

    def __existe_trava_medico_e_horario(self, bucket_name, arquivo_s3):
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=f'{arquivo_s3}.pkl')
            conteudo = response['Body'].read()
            self.logger.info(f"Foi encontrado trava para o medico e horario {arquivo_s3}")

            return True
        except Exception as e:
            print(f'Arquivo {arquivo_s3} do S3: {e}')
            if(e.response["ResponseMetadata"]["HTTPStatusCode"] == 404):
                self.logger.info(f"Não foi encontrado trava para o medico e horario {arquivo_s3}")
                return False
            
            raise
    
    def __alterar_status_agendamento(self, id: str, novoStatus: str):
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"{id}.pkl"
            )

            self.logger.info(f'Response get {response}')

            conteudo = response['Body'].read()
            self.logger.info(f'Response result {conteudo}')

            conteudoJson = pickle.loads(conteudo)

            conteudoJson["status_agendamento"] = novoStatus
            self.logger.info(f'Response result agendamento {conteudoJson}')

            pickled_obj = pickle.dumps(conteudoJson)
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
        
    def __inserir_trava_horario(self, agendamento, path_trava):
        json_trava = {
            'id': id,
            'horario': agendamento["horario"],
            'crm_medico': agendamento["crm_medico"],
            'cpf_paciente': agendamento["cpf_paciente"]
        }

        self.logger.info(f'JSON trava agendamento {json_trava}')
        pickled_obj = pickle.dumps(json_trava)

        self.logger.info(f'Iniciando put object s3')
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=f"{path_trava}.pkl",
            Body=pickled_obj
        )

    def __format_time_trava(self, time_string):
        dt = datetime.strptime(time_string, "%Y-%m-%dT%H:%M")
        formatted_time = dt.strftime("%d-%m-%Y-%H-%M")
        
        return formatted_time