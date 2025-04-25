import asyncio
import os
import logging
from datetime import datetime, timedelta, timezone
import re

HOST = '10.233.30.109'
PORT = 9000

save_dir = r"C:\mac\3CX"
os.makedirs(save_dir, exist_ok=True)

tz_brasil = timezone(timedelta(hours=-3))
data_atual = datetime.now().strftime("%Y-%m-%d")
cdr_file = os.path.join(save_dir, f"cdr_logs_{data_atual}.txt")
log_file = os.path.join(save_dir, f"event_logs_{data_atual}.txt")

# Logger setup
logger = logging.getLogger("CDRLogger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')

file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

callid_regex = r"^(Call\s)"

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

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    logger.info(f"Conexão recebida de {addr}")
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                logger.info(f"Conexão encerrada com {addr}")
                break

            decoded_data = data.decode('utf-8').strip()
            if decoded_data:
                decoded_data = converter_timestamps_para_utc3(decoded_data)
                decoded_data = re.sub(callid_regex, "", decoded_data)
                log_entry = "callid" + re.findall("^([^,]+)", decoded_data)[0]
                timestamp = datetime.now(tz_brasil).strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"CDR recebido. ({log_entry})")

                try:
                    with open(cdr_file, "a", encoding="utf-8") as file:
                        file.write(decoded_data + "\n")
                    logger.info(f"CDR salvo com sucesso. ({log_entry})")
                except Exception as e:
                    logger.error(f"Erro ao salvar CDR ({log_entry}): {e}")
    except Exception as e:
        logger.error(f"Erro na conexão com {addr}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    logger.info(f"Servidor CDR escutando em {HOST}:{PORT}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Servidor encerrado manualmente.")
