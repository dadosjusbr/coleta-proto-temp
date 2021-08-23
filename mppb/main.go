package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/dadosjusbr/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var gitCommit string

const (
	agenciaID  = "mppb"
	repColetor = "https://github.com/dadosjusbr/coletores"
)

func main() {
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
	outputFolder := os.Getenv("OUTPUT_FOLDER")
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

	chaveColeta := proto.IDColeta(agenciaID, month, year)

	folha, parseErr := Parse(files, chaveColeta)
	if parseErr != nil {
		logError("Parsing error: %q", parseErr)
		os.Exit(1)
	}

	coleta := proto.Coleta{
		ChaveColeta:        chaveColeta,
		Orgao:              agenciaID,
		Mes:                int32(month),
		Ano:                int32(year),
		TimestampColeta:    timestamppb.Now(),
		RepositorioColetor: repColetor,
		VersaoColetor:      gitCommit,
		DirColetor:         agenciaID,
	}
	rc := proto.ResultadoColeta{
		Coleta: &coleta,
		Folha:  folha,
	}

	b, err := json.MarshalIndent(rc, "", "  ")
	if err != nil {
		logError("JSON marshaling error: %q", err)
		os.Exit(1)
	}
	fmt.Printf("%s", string(b))
}
