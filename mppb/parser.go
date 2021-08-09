package main

import (
	"fmt"
	"math"
	"strings"

	"github.com/dadosjusbr/coletores"
	"github.com/knieriem/odf/ods"
)

// Data type
const (
	ESTAGIARIOS  = 0
	INDENIZACOES = 1
	REMUNERACOES = 2
)

var headersInd = []string{ 
	"INSALUBRIDADE","SUBSTITUICAO CUMULATIVA","COMARCA DIVERSA","SUBSTITUICAO DE CARGO","SUBSTITUICAO DE PROCURADOR",
"DIFERENCA DE ENTRANCIA", "DIFERENCA DE 1/3 DE FERIAS","DIFERENCA DE PENSAO","DIFERENCA DE ANTERIOR DENTRO DO EXERCICIO","GRATIFICACAO ISONOMICA",
"SERVICO EXTRAORDINARIO",
}

// Mapping of headers to indexes
var headersMap = []map[string]int{
	ESTAGIARIOS: {
		"NOME":                0,
		"CARGO":               1,
		"LOTAÇÃO":             2,
		"REMUNERAÇÃO":         3,
		"OUTRAS VERBAS":       4,
		"FUNÇÃO DE CONFIANÇA": 5,
		"13º VENCIMENTO":      6,
		"FÉRIAS":              7,
		"PERMANÊNCIA":         8,
		"PREVIDENCIÁRIA":      10,
		"IMPOSTO":             11,
		"RETENÇÃO":            12,
		"TEMPORÁRIAS":         16,
		"INDENIZAÇÕES":        15,
	},

	INDENIZACOES: {
		"MATRÍCULA":             0,
		"ALIMENTAÇÃO":           4,
		"SAÚDE":                 5,
		"PECÚNIA":               6,
		"MORADIA":               7,
		"LICENÇA COMPENSATÓRIA": 8,
		"NATALIDADE":            9,
		"AJUDA DE CUSTO":        10, 
		"INSALUBRIDADE":        11,
		"SUBSTITUICAO CUMULATIVA":        12,
		"COMARCA DIVERSA":        13,
		"SUBSTITUICAO DE CARGO":        14,
		"SUBSTITUICAO DE PROCURADOR":        15,
		"DIFERENCA DE ENTRANCIA":        16,
		"DIFERENCA DE 1/3 DE FERIAS":        17,
		"DIFERENCA DE PENSAO":        18,
		"DIFERENCA DE ANTERIOR DENTRO DO EXERCICIO":        19,
		"GRATIFICACAO ISONOMICA":        20,
		"SERVICO EXTRAORDINARIO":        21,
		"DESPESA":               22,
	},

	REMUNERACOES: {
		"MATRÍCULA":             0,
		"NOME":                  1,
		"CARGO":                 2,
		"LOTAÇÃO":               3,
		"CARGO EFETIVO":         4,
		"OUTRAS VERBAS":         5,
		"CARGO EM COMISSÃO":     6,
		"GRATIFICAÇÃO NATALINA": 7,
		"FÉRIAS":                8,
		"PERMANÊNCIA":           9,
		"TEMPORÁRIAS":           10,
		"INDENIZATÓRIAS":        11,
		"PREVIDENCIÁRIA":        13,
		"IMPOSTO":               14,
		"RETENÇÃO":              15,
	},
}

// Parse parses the ods tables.
func Parse(files []string, chave_coleta string) (FolhaDePagamento, Remuneracoes, error) {
	var folha []*ContraCheque
	var remuneracoes []*Remuneracao
	var parseErr bool
	counter := 1
	perks, err := retrievePerksData(files)
	if err != nil {
		return nil, nil, fmt.Errorf("error trying to retrieve perks data: %q", err)
	}
	for _, f := range files {
		if dataType(f) == INDENIZACOES {
			continue
		}

		data, err := dataAsSlices(f)
		if err != nil {
			return nil, fmt.Errorf("error trying to parse data as slices(%s): %q", f, err)
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("No data to be parsed. (%s)", f)
		}

		remu, contra_cheque, ok := retrieveEmployees(data, perks, chave_coleta, counter, f)
		if !ok {
			parseErr = true
		}
		folha = append(folha, contra_cheque...)
		remuneracoes = append(remuneracoes, remu...)
		counter = counter + 1
	}
	if parseErr {
		return FolhaDePagamento{ContraCheque: &folha}, Remuneracoes{Remuneracao: &remuneracoes}, fmt.Errorf("parse error")
	}
	return FolhaDePagamento{ContraCheque: &folha}, Remuneracoes{Remuneracao: &remuneracoes}, nil
}

func retrievePerksData(files []string) ([][]string, error) {
	for _, f := range files {
		if dataType(f) == INDENIZACOES {
			return dataAsSlices(f)
		}
	}
	return nil, nil
}

func retrieveEmployees(emps [][]string, perks [][]string, chave_coleta string, fileName string) ([]*Remuneracao, []*ContraCheque, bool) {
	ok := true
	var remuneracoes []*Remuneracao
	var contra_cheque []ContraCheque
	fileType := dataType(fileName)
	for _, emp := range emps {
		var err error
		var newEmp *ContraCheque
		var newRem []*Remuneracao

		if fileType == REMUNERACOES {
			empPerks := retrievePerksLine(emp[0], perks)
			if newEmp, newRem, err = newEmployee(emp, empPerks, fileName); err != nil {
				ok = false
				logError("error retrieving employee from %s: %q", fileName, err)
				continue
			}
		} else if fileType == ESTAGIARIOS {
			if newEmp, newRem, err = newIntern(emp, fileName); err != nil {
				ok = false
				logError("error retrieving employee from %s: %q", fileName, err)
				continue
			}
		}
		remuneracoes = append(remuneracoes, newRem...)
		contra_cheque = append(contra_cheque, &newEmp)
	}
	return remuneracoes, contra_cheque, ok
}

func retrievePerksLine(regNum string, perks [][]string) []string {
	if perks == nil || len(perks) == 0 {
		return nil
	}
	for _, p := range perks {
		if p[headersMap[INDENIZACOES]["MATRÍCULA"]] == regNum {
			return p
		}
	}
	return nil
}

func newIntern(emp []string, fileName string) (*ContraCheque, []*Remuneracao, error) {
	fileType := dataType(fileName)
	var newEmp *ContraCheque
	var remuneracoes []*Remuneracao
	var descontos []*Remuneracao
	var err error
	newEmp.Nome = retrieveString(emp, "NOME", fileType)
	newEmp.Funcao = retrieveString(emp, "CARGO", fileType)
	newEmp.LocalTrabalho = retrieveString(emp, "LOTAÇÃO", fileType)
	newEmp.Tipo = employeeType(fileName)
	newEmp.Ativo = employeeActive(fileName)

	if remuneracoes, err = internIncomeInfo(emp, fileType); err != nil {
		return nil, fmt.Errorf("error parsing new employee: %q", err)
	}
	if descontos, err = employeeDiscountInfo(emp, fileType); err != nil {
		return nil, fmt.Errorf("error parsing new employee: %q", err)
	}

	remuneracoes = append(remuneracoes, descontos...)

	return &newEmp, remuneracoes,  nil
}

func newEmployee(emp []string, perks []string, chave_coleta string, counter int, fileName string) ( *ContraCheque, []*Remuneracao error) {
	fileType := dataType(fileName)
	var newEmp *ContraCheque
	var remuneracoes []*Remuneracao
	var descontos []*Remuneracao
	var err error
	newEmp.IdContraCheque = fmt.Sprintf("%v/%v", chave_coleta, counter)
	newEmp.ChaveColeta = chave_coleta
	newEmp.Matricula = retrieveString(emp, "MATRÍCULA", fileType)
	newEmp.Nome = retrieveString(emp, "NOME", fileType)
	newEmp.Funcao = retrieveString(emp, "CARGO", fileType)
	newEmp.LocalTrabalho = retrieveString(emp, "LOTAÇÃO", fileType)
	newEmp.Tipo = employeeType(fileName)
	newEmp.Ativo = employeeActive(fileName)

	if remuneracoes, err = employeeIncomeInfo(emp, perks, fileType, newEmp.IdContraCheque, chave_coleta); err != nil {
		return nil, nil,  fmt.Errorf("error parsing new employee: %q", err)
	}
	if descontos, err = employeeDiscountInfo(emp, fileType); err != nil {
		return nil,nil, fmt.Errorf("error parsing new employee: %q", err)
	}
	
	remuneracoes = append(remuneracoes, descontos...)

	return &newEmp, remuneracoes,  nil
}

func employeeActive(fileName string) bool {
	return (strings.Contains(fileName, "Inativos") || strings.Contains(fileName, "aposentados")) == false
}

func employeeType(fileName string) string {
	if strings.Contains(fileName, "servidor") {
		return "servidor"
	} else if strings.Contains(fileName, "membro") {
		return "membro"
	} else if strings.Contains(fileName, "aposentados") {
		return "pensionista"
	} else if strings.Contains(fileName, "estagiario") {
		return "estagiario"
	}
	return ""
}

func internIncomeInfo(emp []string, fileType int, idContraCheque string, chave_coleta string) ([]*Remuneracao, error) {
	var err error
	var remuneracoes  []*Remuneracao
	internSlice := ["REMUNERAÇÃO", "INDENIZAÇÕES", "OUTRAS VERBAS", "PERMANÊNCIA", "FÉRIAS", "13º VENCIMENTO", "TEMPORÁRIAS", "FUNÇÃO DE CONFIANÇA"]
	for key, value := range internSlice {
		if value >= 11 && value <=21 {
			if remu, err = remuneracaoBuild("R", value, "REMUNERAÇÃO BÁSICA"  idContraCheque, chave_coleta, fileType, emp); err != nil {
				return nil, fmt.Errorf("error retrieving employee income info: %q", err)
			} 
			remuneracoes = append(remuneracoes, remu)
		}
	}
	return remuneracoes, nil
}

func employeeIncomeInfo(emp []string, perks []string, fileType int, idContraCheque string, chave_coleta string) ([]*Remuneracao, error) {
	var err error
	var remuneracoes  []*Remuneracao

	if remu, err := remuneracaoBuild("R", "CARGO EFETIVO", "REMUNERAÇÃO BÁSICA", idContraCheque, chave_coleta, fileType, emp); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)
	if remu, err = remuneracaoBuild("R", "ALIMENTAÇÃO", "VERBAS INDENIZATÓRIAS", idContraCheque, chave_coleta, fileType, perks); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)
	if remu, err = remuneracaoBuild("R", "SAÚDE", "VERBAS INDENIZATÓRIAS", idContraCheque, chave_coleta, fileType, perks); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)
	if remu, err = remuneracaoBuild("R", "MORADIA", "VERBAS INDENIZATÓRIAS", idContraCheque, chave_coleta, fileType, perks); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)
	if remu, err = remuneracaoBuild("R", "NATALIDADE", "VERBAS INDENIZATÓRIAS", idContraCheque, chave_coleta, fileType, perks); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)
	if remu, err = remuneracaoBuild("R", "AJUDA DE CUSTO","VERBAS INDENIZATÓRIAS", idContraCheque, chave_coleta, fileType, perks); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)
	if remu, err = remuneracaoBuild("R", "PECÚNIA", "VERBAS INDENIZATÓRIAS", idContraCheque, chave_coleta, fileType, perks); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)

	if remu, err = remuneracaoBuild("R", "LICENÇA COMPENSATÓRIA", "VERBAS INDENIZATÓRIAS", idContraCheque, chave_coleta, fileType, perks); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)

	if remu, err = remuneracaoBuild("R", "OUTRAS VERBAS", "REMUNERAÇÃO BÁSICA", idContraCheque, chave_coleta, fileType, emp); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)

	if remu, err = remuneracaoBuild("R", "PERMANÊNCIA", "REMUNERAÇÃO EVENTUAL OU TEMPORÁRIA",  idContraCheque, chave_coleta, fileType, emp); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)

	if remu, err = remuneracaoBuild("R", "FÉRIAS", "REMUNERAÇÃO EVENTUAL OU TEMPORÁRIA", idContraCheque, chave_coleta, fileType, emp); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)

	if remu, err = remuneracaoBuild("R", "GRATIFICAÇÃO NATALINA", "REMUNERAÇÃO EVENTUAL OU TEMPORÁRIA", idContraCheque, chave_coleta, fileType, emp); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	remuneracoes = append(remuneracoes, remu)

	if remu, err = remuneracaoBuild("R", "CARGO EM COMISSÃO", "REMUNERAÇÃO EVENTUAL OU TEMPORÁRIA",  idContraCheque, chave_coleta, fileType, emp); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	} 
	
	for key, value := range headersMap[fileType] {
		if value >= 11 && value <=21 {
			if remu, err = remuneracaoBuild("R", key, "OUTRAS REMUNERAÇÕES TEMPORÁRIAS 2" , idContraCheque, chave_coleta, fileType, perks); err != nil {
				return nil, fmt.Errorf("error retrieving employee income info: %q", err)
			} 
			remuneracoes = append(remuneracoes, remu)
		}
	}

	return &remuneracoes, nil
}

func remuneracaoBuild(natureza, nome_auxilio, categoria, idContraCheque, chave_coleta string, fileType int, data []string) (*Remuneracao, error) {
	var remuneracao Remuneracao
	remuneracao.IdContraCheque = idContraCheque
	remuneracao.ChaveColeta = chave_coleta
	remuneracao.Natureza = natureza
	remuneracao.Categoria = categoria
	remuneracao.Item = nome_auxilio
	if err = retrieveFloat64(&remuneracao.Valor, data, nome_auxilio, fileType); err != nil {
		return nil, fmt.Errorf("error retrieving employee income info: %q", err)
	}
	if natureza == "D" {
		remuneracao.Valor = remuneracao.Valor * (-1)
	}
	return &remuneracao, nil
}

func employeeDiscountInfo(emp []string, fileType int) ([]*Remuneracao, error) {

	var descontos []*Remuneracao
	for k, i := range ["PREVIDENCIÁRIA", "RETENÇÃO", "IMPOSTO"] {
		if desc, err = remuneracaoBuild("D", k, "Obrigatórios / Legais"  idContraCheque, chave_coleta, fileType, emp); err != nil {
			return nil, fmt.Errorf("error retrieving employee income info: %q", err)
		} 
		descontos = append(descontos, desc)
	}
	return descontos, nil
}

func dataAsSlices(file string) ([][]string, error) {
	var result [][]string
	var doc ods.Doc
	f, err := ods.Open(file)
	if err != nil {
		return nil, fmt.Errorf("ods.Open error(%s): %q", file, err)
	}
	f.ParseContent(&doc)
	fileType := dataType(file)
	if err := assertHeaders(doc, fileType); err != nil {
		return nil, fmt.Errorf("assertHeaders() for %s error: %q", file, err)
	}
	result = append(result, getEmployees(doc)...)
	f.Close()
	return result, nil
}

func dataType(fileName string) int {
	if strings.Contains(fileName, "indenizacoes") {
		return INDENIZACOES
	} else if strings.Contains(fileName, "estagiarios") {
		return ESTAGIARIOS
	} else if strings.Contains(fileName, "membros") || strings.Contains(fileName, "servidores") || strings.Contains(fileName, "aposentados") {
		return REMUNERACOES
	}
	return -1
}

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

func getHeaders(doc ods.Doc, fileType int) []string {
	var headers []string
	raw := cleanStrings(doc.Table[0].Strings()[5:8])
	switch fileType {
	case INDENIZACOES:
		headers = append(headers, raw[0][:4]...)
		headers = append(headers, raw[2][4:]...)
		break
	case ESTAGIARIOS:
		headers = append(headers, raw[0][:3]...)
		headers = append(headers, raw[2][3:9]...)
		headers = append(headers, raw[1][9])
		headers = append(headers, raw[2][10:]...)
		headers = append(headers, raw[1][13])
		headers = append(headers, raw[0][14:]...)
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

func assertHeaders(doc ods.Doc, fileType int) error {
	headers := getHeaders(doc, fileType)
	for key, value := range headersMap[fileType] {
		if err := containsHeader(headers, key, value); err != nil {
			return err
		}
	}
	return nil
}

func containsHeader(headers []string, key string, value int) error {
	if strings.Contains(headers[value], key) {
		return nil
	}
	return fmt.Errorf("couldn't find %s at position %d", key, value)
}
