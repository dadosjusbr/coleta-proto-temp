import sys
import os
import crawler
import parser
import json
import coleta_pb2 as Coleta
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToJson

if('COURT' in os.environ):
    court = os.environ['COURT']
else:
    sys.stderr.write("Invalid arguments, missing parameter: 'COURT'.\n")
    os._exit(1)
if('YEAR' in os.environ):
    year = os.environ['YEAR']
else:
    sys.stderr.write("Invalid arguments, missing parameter: 'YEAR'.\n")
    os._exit(1)
if('MONTH' in os.environ):
    month = os.environ['MONTH']
    month = month.zfill(2)
else:
    sys.stderr.write("Invalid arguments, missing parameter: 'MONTH'.\n")
    os._exit(1)
if('DRIVER_PATH' in os.environ):
    driver_path = os.environ['DRIVER_PATH']
else:
    sys.stderr.write("Invalid arguments, missing parameter: 'DRIVER_PATH'.\n")
    os._exit(1)
if('OUTPUT_FOLDER' in os.environ):
    output_path = os.environ['OUTPUT_FOLDER']
else:
    output_path = "./output"
if('GIT_COMMIT' in os.environ):
    crawler_version = os.environ['GIT_COMMIT']
else:
    sys.stderr.write("crawler_version cannot be empty")
    os._exit(1)

repColetor = "https://github.com/dadosjusbr/coletores"

# Main execution
def main():
    #file_names = crawler.crawl(court, year, month, driver_path, output_path)
    file_names = ['/home/joh/dadosjusbr/coleta-proto-temp/cnj/src/output/TJRJ-contracheque.xlsx','/home/joh/dadosjusbr/coleta-proto-temp/cnj/src/output/TJRJ-direitos-eventuais.xlsx',
    '/home/joh/dadosjusbr/coleta-proto-temp/cnj/src/output/TJRJ-direitos-pessoais.xlsx', '/home/joh/dadosjusbr/coleta-proto-temp/cnj/src/output/TJRJ-indenizações.xlsx']
    coleta = Coleta.Coleta()
    coleta.chave_coleta = court.lower() + '/' + month + '/' + year
    folha = Coleta.FolhaDePagamento()
    folha = parser.parse(file_names, coleta.chave_coleta)
    
    coleta = Coleta.Coleta()
    coleta.chave_coleta = court.lower() + '/' + month + '/' + year
    coleta.orgao = court.lower()
    coleta.mes = int(month)
    coleta.ano = int(year)
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    coleta.timestamp_coleta.CopyFrom(timestamp)
    coleta.repositorio_coletor = repColetor
    coleta.versao_coletor = crawler_version
    coleta.dir_coletor = 'cnj'
    coleta.arquivos.extend(file_names)
    rc = {
        'coleta': coleta,
        'folha': folha,
    }
    rc = Coleta.ResultadoColeta()
    rc.folha.CopyFrom(folha)
    rc.coleta.CopyFrom(coleta)
    print(MessageToJson(rc))
    #print(json.dumps({'rc': rc}, ensure_ascii=False))


if __name__ == '__main__':
    main()
