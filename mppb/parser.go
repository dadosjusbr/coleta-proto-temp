package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/dadosjusbr/proto"
	"github.com/knieriem/odf/ods"
)

const (
	INDENIZACOES                                   = "indenizacoes"
	REMUNERACOES                                   = "membros"
	INDENIZACOES_VERBAS_INDENIZATORIAS_1           = "VERBAS INDENIZATÓRIAS 1"
	INDENIZACOES_OUTRAS_REMUNERACOES_TEMPORARIAS_2 = "OUTRAS REMUNERAÇÕES TEMPORÁRIAS 2"
	REMUNERACAO_BASICA                             = "REMUNERAÇÃO BÁSICA"
	REMUNERACAO_EVENTUAL_TEMPORARIA                = "REMUNERAÇÃO EVENTUAL OU TEMPORÁRIA"
	OBRIGATORIOS_LEGAIS                            = "OBRIGATÓRIOS/LEGAIS"
)

// Mapeia as categorias das planilhas.
var headersMap = map[string]map[string]int{
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
	const INDENIZACOES_MATRICULA = 0
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
		indenizacoesMembro := mapIndenizacoes[membro[0]]
		if novoMembro, err = criaMembro(membro, indenizacoesMembro, chaveColeta, counter, fileName); err != nil {
			ok = false
			log.Fatalf("error na criação de um novo membro %s: %q", fileName, err)
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
	const REMUNERACOES_MATRICULA = 0
	const REMUNERACOES_NOME = 1
	const REMUNERACOES_CARGO = 2
	const REMUNERACOES_LOTACAO = 3
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
	temp, err := criaRemuneracao(indenizacoes, proto.Remuneracao_R, INDENIZACOES_VERBAS_INDENIZATORIAS_1)
	if err != nil {
		return nil, fmt.Errorf("erro processando verbas indenizatorias 1: %q", err)
	}
	remuneracoes = append(remuneracoes, temp...)

	temp, err = criaRemuneracao(indenizacoes, proto.Remuneracao_R, INDENIZACOES_OUTRAS_REMUNERACOES_TEMPORARIAS_2)
	if err != nil {
		return nil, fmt.Errorf("erro processando outras remuneracoes temporarias 2: %q", err)
	}
	remuneracoes = append(remuneracoes, temp...)

	temp, err = criaRemuneracao(membro, proto.Remuneracao_R, REMUNERACAO_BASICA)
	if err != nil {
		return nil, fmt.Errorf("erro processando remuneracao básica: %q", err)
	}
	remuneracoes = append(remuneracoes, temp...)

	temp, err = criaRemuneracao(membro, proto.Remuneracao_R, REMUNERACAO_EVENTUAL_TEMPORARIA)
	if err != nil {
		return nil, fmt.Errorf("erro processando remuneracao eventual temporaria: %q", err)
	}
	remuneracoes = append(remuneracoes, temp...)

	temp, err = criaRemuneracao(membro, proto.Remuneracao_D, OBRIGATORIOS_LEGAIS)
	if err != nil {
		return nil, fmt.Errorf("erro processando erro processando obrigatório/legais: %q", err)
	}
	remuneracoes = append(remuneracoes, temp...)

	return remuneracoes, nil
}

// criaRemuneracao monta as remuneracoes de um membro, a partir de cada categoria.
func criaRemuneracao(planilha []string, natureza proto.Remuneracao_Natureza, categoria string) ([]*proto.Remuneracao, error) {
	var remuneracoes []*proto.Remuneracao
	var err error
	for key := range headersMap[categoria] {
		var remuneracao proto.Remuneracao
		remuneracao.Natureza = natureza
		remuneracao.Categoria = categoria
		remuneracao.Item = key
		remuneracao.Valor, err = parseFloat(planilha, key, categoria)
		if err != nil {
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
	defer f.Close()
	if err != nil {
		return nil, fmt.Errorf("ods.Open error(%s): %q", file, err)
	}
	f.ParseContent(&doc)
	fileType := tipoCSV(file)
	if err := assertHeaders(doc, fileType); err != nil {
		return nil, fmt.Errorf("assertHeaders() for %s error: %q", file, err)
	}
	result = append(result, getEmployees(doc)...)
	return result, nil
}

// tipoCSV checa se o arquivo é de indenizações ou membros.
func tipoCSV(nomeArquivo string) string {
	if strings.Contains(nomeArquivo, INDENIZACOES) {
		return INDENIZACOES
	} else if strings.Contains(nomeArquivo, REMUNERACOES) {
		return REMUNERACOES
	}
	return ""
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
func getHeaders(doc ods.Doc, fileType string) []string {
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
func assertHeaders(doc ods.Doc, fileType string) error {
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

// parseFloat makes the string with format "xx.xx,xx" able to be parsed by the strconv.ParseFloat and return it parsed.
func parseFloat(emp []string, key, fileType string) (float64, error) {
	valueStr := emp[headersMap[fileType][key]]
	if valueStr == "" {
		return 0.0, nil
	} else {
		valueStr = strings.Trim(valueStr, " ")
		valueStr = strings.Replace(valueStr, ",", ".", 1)
		if n := strings.Count(valueStr, "."); n > 1 {
			valueStr = strings.Replace(valueStr, ".", "", n-1)
		}
	}
	return strconv.ParseFloat(valueStr, 64)
}

// cleanStrings makes all strings to uppercase and removes N/D fields
func cleanStrings(raw [][]string) [][]string {
	for row := range raw {
		for col := range raw[row] {
			raw[row][col] = strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(raw[row][col], "N/D", ""), "\n", " "))
			raw[row][col] = strings.ReplaceAll(raw[row][col], "  ", " ")
		}
	}
	return raw
}
