package main

import (
	"encoding/json"
	"fmt"
	"os"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var gitCommit string

func main() {
	outputFolder := "./output"
	month := 02
	year := 2020
	if outputFolder == "" {
		outputFolder = "./output"
	}

	if err := os.Mkdir(outputFolder, os.ModePerm); err != nil && !os.IsExist(err) {
		logError("Error creating output folder(%s): %q", outputFolder, err)
		os.Exit(1)
	}

	files, err := Crawl(outputFolder, month, year)
	if err != nil {
		logError("Crawler error: %q", err)
		os.Exit(1)
	}
	fmt.Println(files)

	chave_coleta := fmt.Sprintf("mppb/%v/%v", year, month)
	folha, remuneracoes, parseErr := Parse(files, chave_coleta)
	if parseErr != nil {
		logError("Parsing error: %q", parseErr)
		os.Exit(1)
	}

	er := newCrawlingResult(*folha, *remuneracoes, files, month, year)
	b, err := json.MarshalIndent(er, "", "  ")
	if err != nil {
		logError("JSON marshaling error: %q", err)
		os.Exit(1)
	}
	fmt.Printf("%s", string(b))
}

func newCrawlingResult(folha FolhaDePagamento, remuneracoes Remuneracoes, files []string, month, year int) ResultadoColeta {
	coleta := Coleta{
		ChaveColeta:        fmt.Sprintf("mppb/%v/%v", year, month),
		Orgao:              "mppb",
		Mes:                int32(month),
		Ano:                int32(year),
		TimestampColeta:    timestamppb.Now(),
		RepositorioColetor: "mppb",
		VersaoColetor:      gitCommit,
		DirColetor:         "https://github.com/dadosjusbr/coletores/tree/master/mppb",
	}
	rc := ResultadoColeta{
		Coleta:       &coleta,
		Folha:        &folha,
		Remuneracoes: &remuneracoes,
	}
	return rc
}
