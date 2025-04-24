import socket
import os
import logging
from datetime import datetime, timedelta, timezone
import re

# Configuração do servidor
HOST = '10.233.30.109'
PORT = 9000

# Caminhos dos arquivos
save_dir = r"C:\mac\3CX"
#cdr_file = os.path.join(save_dir, "cdr_logs.txt")
data_atual = datetime.now().strftime("%Y-%m-%d")
cdr_file = os.path.join(save_dir, f"cdr_logs_{data_atual}.txt")
log_file = os.path.join(save_dir, "event_logs.txt")

# Garante que a pasta existe
os.makedirs(save_dir, exist_ok=True)

# Fuso horário do Brasil (UTC-3)
tz_brasil = timezone(timedelta(hours=-3))

# Configuração do logger com dois handlers: arquivo + console
logger = logging.getLogger("CDRLogger")
logger.setLevel(logging.INFO)

log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')

file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

callid_regex = "^(Call\s)"

logger.info(f"Servidor iniciado. Arquivo CDR será salvo em: {cdr_file}")

# Função para converter timestamps no formato "YYYY/MM/DD HH:MM:SS" para UTC-3
def converter_timestamps_para_utc3(texto):
    padrao = r"\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}"
    matches = re.findall(padrao, texto)
    for original in matches:
        try:
            dt_utc = datetime.strptime(original, "%Y/%m/%d %H:%M:%S")
            dt_brasil = dt_utc.replace(tzinfo=timezone.utc).astimezone(tz_brasil)
            convertido = dt_brasil.strftime("%Y/%m/%d %H:%M:%S")
            texto = texto.replace(original, convertido)
        except Exception as e:
            logger.warning(f"Erro ao converter timestamp '{original}': {e}")
    return texto

# Criação e configuração do socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
    server_socket.bind((HOST, PORT))
    server_socket.listen()
    logger.info(f"Servidor CDR escutando em {HOST}:{PORT}")

    try:
        while True:
            conn, addr = server_socket.accept()
            with conn:
                logger.info(f"Conexão recebida de {addr}")
                while True:
                    data = conn.recv(1024)
                    if not data:
                        logger.info(f"Conexão encerrada com {addr}")
                        break

                    decoded_data = data.decode('utf-8').strip()

                    if decoded_data:
                        # Converte timestamps dentro do conteúdo do CDR para UTC-3
                        decoded_data = converter_timestamps_para_utc3(decoded_data)

                        # Timestamp do evento (chegada da mensagem)
                        timestamp = datetime.now(tz_brasil).strftime("%Y-%m-%d %H:%M:%S")

                        decoded_data = re.sub(callid_regex, '', decoded_data)

                        log_entry = "callid " + re.findall("^([^,]+)", decoded_data)[0]

                        logger.info(f"CDR recebido. ({log_entry})")

                        try:
                            with open(cdr_file, "a", encoding="utf-8") as file:
                                file.write(decoded_data + "\n")
                            logger.info(f"CDR salvo com sucesso. ({log_entry})")
                        except Exception as e:
                            logger.error(f"Erro ao salvar CDR ({log_entry}): {e}")

    except KeyboardInterrupt:
        logger.info("Servidor encerrado manualmente.")
