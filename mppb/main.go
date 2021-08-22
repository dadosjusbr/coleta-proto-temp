package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/dadosjusbr/proto"
	"github.com/joho/godotenv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var gitCommit string

func main() {
	err := godotenv.Load(".env")
	outputFolder := os.Getenv("OUTPUT_FOLDER")
	month, err := strconv.Atoi(os.Getenv("MONTH"))
	if err != nil {
		logError("Invalid month (\"%s\"): %q", os.Getenv("MONTH"), err)
		os.Exit(1)
	}
	year, err := strconv.Atoi(os.Getenv("YEAR"))
	if err != nil {
		logError("Invalid year (\"%s\"): %q", os.Getenv("YEAR"), err)
		os.Exit(1)
	}
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

	chaveColeta := proto.IDColeta("mppb", month, year)

	folha, parseErr := Parse(files, chaveColeta)
	if parseErr != nil {
		logError("Parsing error: %q", parseErr)
		os.Exit(1)
	}

	er := newCrawlingResult(*folha, chaveColeta, files, month, year)
	b, err := json.MarshalIndent(er, "", "  ")
	if err != nil {
		logError("JSON marshaling error: %q", err)
		os.Exit(1)
	}
	fmt.Printf("%s", string(b))
}

func newCrawlingResult(folha proto.FolhaDePagamento, chaveColeta string, files []string, month, year int) proto.ResultadoColeta {
	coleta := proto.Coleta{
		ChaveColeta:        chaveColeta,
		Orgao:              "mppb",
		Mes:                int32(month),
		Ano:                int32(year),
		TimestampColeta:    timestamppb.Now(),
		RepositorioColetor: "mppb",
		VersaoColetor:      gitCommit,
		DirColetor:         "https://github.com/dadosjusbr/coletores/tree/master/mppb",
	}
	rc := proto.ResultadoColeta{
		Coleta: &coleta,
		Folha:  &folha,
	}
	return rc
}
