package main

import (
	"fmt"
	"strings"

	"github.com/dadosjusbr/proto"
	"github.com/knieriem/odf/ods"
)

const (
	INDENIZACOES                                   = 1
	REMUNERACOES                                   = 2
	INDENIZACOES_VERBAS_INDENIZATORIAS_1           = 0
	INDENIZACOES_OUTRAS_REMUNERACOES_TEMPORARIAS_2 = 1
	REMUNERACAO_BASICA                             = 2
	REMUNERACAO_EVENTUAL_TEMPORARIA                = 3
	OBRIGATORIOS_LEGAIS                            = 4
	INDENIZACOES_MATRICULA                         = 0
	REMUNERACOES_MATRICULA                         = 0
	REMUNERACOES_NOME                              = 1
	REMUNERACOES_CARGO                             = 2
	REMUNERACOES_LOTACAO                           = 3
)

// Mapeia as categorias das planilhas.
var headersMap = []map[string]int{
	INDENIZACOES_VERBAS_INDENIZATORIAS_1: {
		"ALIMENTAÇÃO":           4,
		"SAÚDE":                 5,
		"PECÚNIA":               6,
		"MORADIA":               7,
		"LICENÇA COMPENSATÓRIA": 8,
		"NATALIDADE":            9,
	},
	INDENIZACOES_OUTRAS_REMUNERACOES_TEMPORARIAS_2: {
		"AJUDA DE CUSTO":                              10,
		"ADICIONAL DE INSALUBRIDADE":                  11,
		"SUBSTITUIÇÃO CUMULATIVA":                     12,
		"GRATIFICAÇÃO POR ATUAÇÃO EM COMARCA DIVERSA": 13,
		"SUBSTITUIÇÃO DE CARGO":                       14,
		"SUBSTITUIÇÃO DE PROCURADOR DE JUSTIÇA":       15,
		"DIFERENÇA DE ENTRÂNCIA":                      16,
		"DIFERENÇA DE 1/3 DE FÉRIAS":                  17,
		"DIFERENÇA DE PENSÃO":                         18,
		"DIFERENÇA ANTERIOR DENTRO DO EXERCÍCIO":      19,
		"PARCELA DE GRATIFICAÇÃO ISONÔMICA":           20,
		"SERVIÇO EXTRAORDINÁRIO":                      21,
		"DESPESA DE EXERCÍCIOS ANTERIORES":            22,
	},
	REMUNERACAO_BASICA: {
		"CARGO EFETIVO": 4,
		"OUTRAS VERBAS": 5,
	},
	REMUNERACAO_EVENTUAL_TEMPORARIA: {
		"CARGO EM COMISSÃO":     6,
		"GRATIFICAÇÃO NATALINA": 7,
		"FÉRIAS":                8,
		"PERMANÊNCIA":           9,
	},
	OBRIGATORIOS_LEGAIS: {
		"PREVIDENCIÁRIA": 13,
		"IMPOSTO":        14,
		"RETENÇÃO":       15,
	},
}

// Parse parses the ods tables.
func Parse(arquivos []string, chave_coleta string) (*proto.FolhaDePagamento, error) {
	var folha []*proto.ContraCheque
	var parseErr bool
	indenizacoes, err := getDadosIndenizacoes(arquivos)
	if err != nil {
		return nil, fmt.Errorf("erro tentando recuperar os dados de indenizações: %q", err)
	}
	mapIndenizacoes := map[string][]string{}
	for _, f := range indenizacoes {
		mapIndenizacoes[f[INDENIZACOES_MATRICULA]] = f
	}

	for _, f := range arquivos {
		if tipoCSV(f) == INDENIZACOES {
			continue
		}

		dados, err := dadosParaMatriz(f)
		if err != nil {
			return nil, fmt.Errorf("erro na tentativa de transformar os dados em matriz (%s): %q", f, err)
		}
		if len(dados) == 0 {
			return nil, fmt.Errorf("Não há dados para serem parseados. (%s)", f)
		}
		contra_cheque, ok := getMembros(dados, mapIndenizacoes, chave_coleta, f)
		if !ok {
			parseErr = true
		}
		folha = append(folha, contra_cheque...)
	}
	if parseErr {
		return &proto.FolhaDePagamento{ContraCheque: folha}, fmt.Errorf("parse error")
	}
	return &proto.FolhaDePagamento{ContraCheque: folha}, nil
}

// getDadosIndenizacoes retorna a planilha de indenizações em forma de matriz
func getDadosIndenizacoes(files []string) ([][]string, error) {
	for _, f := range files {
		if tipoCSV(f) == INDENIZACOES {
			return dadosParaMatriz(f)
		}
	}
	return nil, nil
}

// getMembros retorna o array com a folha de pagamento da coleta.
func getMembros(membros [][]string, mapIndenizacoes map[string][]string, chaveColeta string, fileName string) ([]*proto.ContraCheque, bool) {
	ok := true
	var contraCheque []*proto.ContraCheque
	counter := 1
	for _, membro := range membros {
		var err error
		var novoMembro *proto.ContraCheque
		indenizacoesMembro := getIndenizacaoMembro(membro[0], mapIndenizacoes)
		if novoMembro, err = criaMembro(membro, indenizacoesMembro, chaveColeta, counter, fileName); err != nil {
			ok = false
			logError("error na criação de um novo membro %s: %q", fileName, err)
			continue
		}
		counter++
		contraCheque = append(contraCheque, novoMembro)
	}
	return contraCheque, ok
}

// getIndenizacaoMembro busca as indenizacoes de um membro baseado na matrícula.
func getIndenizacaoMembro(regNum string, mapIndenizacoes map[string][]string) []string {
	if val, ok := mapIndenizacoes[regNum]; ok {
		return val
	}
	return nil
}

// criaMembro monta um contracheque de um único membro.
func criaMembro(membro []string, indenizacoes []string, chaveColeta string, counter int, fileName string) (*proto.ContraCheque, error) {
	var novoMembro proto.ContraCheque
	novoMembro.IdContraCheque = fmt.Sprintf("%v/%v", chaveColeta, counter)
	novoMembro.ChaveColeta = chaveColeta
	novoMembro.Matricula = membro[REMUNERACOES_MATRICULA]
	novoMembro.Nome = membro[REMUNERACOES_NOME]
	novoMembro.Funcao = membro[REMUNERACOES_CARGO]
	novoMembro.LocalTrabalho = membro[REMUNERACOES_LOTACAO]
	novoMembro.Tipo = proto.ContraCheque_MEMBRO
	novoMembro.Ativo = true
	remuneracoes, err := processaRemuneracao(membro, indenizacoes)
	if err != nil {
		return nil, fmt.Errorf("error na transformação das remunerações: %q", err)
	}
	novoMembro.Remuneracoes = &proto.Remuneracoes{Remuneracao: remuneracoes}
	return &novoMembro, nil
}

// processaRemuneracao processa todas as remunerações de um único membro.
func processaRemuneracao(membro []string, indenizacoes []string) ([]*proto.Remuneracao, error) {
	var remuneracoes []*proto.Remuneracao
	for i := range headersMap {
		switch i {
		case INDENIZACOES_VERBAS_INDENIZATORIAS_1:
			temp, err := criaRemuneracao(indenizacoes, proto.Remuneracao_R, "VERBAS INDENIZATÓRIAS 1", i)
			if err != nil {
				return nil, fmt.Errorf("erro processando verbas indenizatorias 1: %q", err)
			}
			remuneracoes = append(remuneracoes, temp...)
		case INDENIZACOES_OUTRAS_REMUNERACOES_TEMPORARIAS_2:
			temp, err := criaRemuneracao(indenizacoes, proto.Remuneracao_R, "OUTRAS REMUNERAÇÕES TEMPORÁRIAS 2", i)
			if err != nil {
				return nil, fmt.Errorf("erro processando outras remuneracoes temporarias 2: %q", err)
			}
			remuneracoes = append(remuneracoes, temp...)
		case REMUNERACAO_BASICA:
			temp, err := criaRemuneracao(membro, proto.Remuneracao_R, "REMUNERAÇÃO BÁSICA", i)
			if err != nil {
				return nil, fmt.Errorf("erro processando remuneracao básica: %q", err)
			}
			remuneracoes = append(remuneracoes, temp...)
		case REMUNERACAO_EVENTUAL_TEMPORARIA:
			temp, err := criaRemuneracao(membro, proto.Remuneracao_R, "REMUNERAÇÃO EVENTUAL OU TEMPORÁRIA", i)
			if err != nil {
				return nil, fmt.Errorf("erro processando remuneracao eventual temporaria: %q", err)
			}
			remuneracoes = append(remuneracoes, temp...)
		default:
			temp, err := criaRemuneracao(membro, proto.Remuneracao_D, "OBRIGATÓRIOS/LEGAIS", i)
			if err != nil {
				return nil, fmt.Errorf("erro processando erro processando obrigatório/legais: %q", err)
			}
			remuneracoes = append(remuneracoes, temp...)
		}
	}
	return remuneracoes, nil
}

// criaRemuneracao monta as remuneracoes de um membro, a partir de cada categoria.
func criaRemuneracao(planilha []string, natureza proto.Remuneracao_Natureza, categoria string, indice int) ([]*proto.Remuneracao, error) {
	var remuneracoes []*proto.Remuneracao
	for key := range headersMap[indice] {
		var remuneracao proto.Remuneracao
		remuneracao.Natureza = natureza
		remuneracao.Categoria = categoria
		remuneracao.Item = key
		if err := retrieveFloat64(&remuneracao.Valor, planilha, key, indice); err != nil {
			return nil, fmt.Errorf("error buscando o valor na planilha: %q", err)
		}
		if natureza == proto.Remuneracao_D {
			remuneracao.Valor = remuneracao.Valor * (-1)
		}
		remuneracoes = append(remuneracoes, &remuneracao)
	}
	return remuneracoes, nil
}

// dadosParaMatriz transforma os dados de determinado arquivo, em uma matriz
func dadosParaMatriz(file string) ([][]string, error) {
	var result [][]string
	var doc ods.Doc
	f, err := ods.Open(file)
	if err != nil {
		return nil, fmt.Errorf("ods.Open error(%s): %q", file, err)
	}
	f.ParseContent(&doc)
	fileType := tipoCSV(file)
	if err := assertHeaders(doc, fileType); err != nil {
		return nil, fmt.Errorf("assertHeaders() for %s error: %q", file, err)
	}
	result = append(result, getEmployees(doc)...)
	f.Close()

	return result, nil
}

// tipoCSV checa se o arquivo é de indenizações ou membros.
func tipoCSV(nomeArquivo string) int {
	if strings.Contains(nomeArquivo, "indenizacoes") {
		return INDENIZACOES
	} else if strings.Contains(nomeArquivo, "membros") {
		return REMUNERACOES
	}
	return -1
}

// getEmployees varre a lista de membros e seleciona apenas as linhas que correspondem aos dados.
func getEmployees(doc ods.Doc) [][]string {
	var lastLine int
	for i, values := range doc.Table[0].Strings() {
		if len(values) < 1 {
			continue
		}
		if values[0] == "TOTAL GERAL" {
			lastLine = i - 1
			break
		}
	}

	if lastLine == 0 {
		return [][]string{}
	}
	return cleanStrings(doc.Table[0].Strings()[10:lastLine])
}

// getHeaders varre o documento e retorna o cabeçalho de cada arquivo.
func getHeaders(doc ods.Doc, fileType int) []string {
	var headers []string
	raw := cleanStrings(doc.Table[0].Strings()[5:8])
	switch fileType {
	case INDENIZACOES:
		headers = append(headers, raw[0][:4]...)
		headers = append(headers, raw[2][4:]...)
		break
	case REMUNERACOES:
		headers = append(headers, raw[0][:4]...)
		headers = append(headers, raw[2][4:10]...)
		headers = append(headers, raw[1][10:13]...)
		headers = append(headers, raw[2][13:]...)
		break
	}
	return headers
}

// assertHeaders verifica se o cabeçalho existe.
func assertHeaders(doc ods.Doc, fileType int) error {
	headers := getHeaders(doc, fileType)
	for key, value := range headersMap[fileType] {
		if err := containsHeader(headers, key, value); err != nil {
			return err
		}
	}
	return nil
}

// containsHeader verifica se é possível encontrar a chave buscada em alguma posição da planilha.
func containsHeader(headers []string, key string, value int) error {
	if strings.Contains(headers[value], key) {
		return nil
	}
	return fmt.Errorf("couldn't find %s at position %d", key, value)
}
